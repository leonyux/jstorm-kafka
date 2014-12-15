package storm.kafka;

import backtype.storm.Config;
import backtype.storm.metric.api.IMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import kafka.message.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.KafkaSpout.EmitState;
import storm.kafka.PartitionManager.KafkaMessageId;

import java.util.*;

// TODO: need to add blacklisting
// TODO: need to make a best effort to not re-emit messages if don't have to
public class KafkaSpoutLocal extends BaseRichSpout {
    public static class MessageAndRealOffset {
        public Message msg;
        public long offset;

        public MessageAndRealOffset(Message msg, long offset) {
            this.msg = msg;
            this.offset = offset;
        }
    }

    public static final Logger LOG = LoggerFactory
            .getLogger(KafkaSpoutLocal.class);

    String _uuid = UUID.randomUUID().toString();
    SpoutConfig _spoutConfig;// spout需要用的kafka配置信息，主要是zk和topic相关
    SpoutOutputCollector _collector;
    PartitionCoordinator _coordinator;// Partition Coordinator协调不
                                      // 同的spout向不同的kafka partion取数据
    DynamicPartitionConnections _connections;// Dynamic Partition
                                             // Connection是做什么的
    ZkState _state;// 封装的zk信息和接口

    long _lastUpdateMs = 0;

    int _currPartitionIndex = 0;

    public KafkaSpoutLocal(SpoutConfig spoutConf) {// 构建topology构造kafkaspout时需要提供配置信息
        _spoutConfig = spoutConf;
    }

    @Override
    public void open(Map conf, final TopologyContext context,
            final SpoutOutputCollector collector) {
        _collector = collector;

        Map stateConf = new HashMap(conf);
        List<String> zkServers = _spoutConfig.zkServers;
        if (zkServers == null) {// 如果没有额外配置，认为kafka和storm公用一个zookeeper集群
            zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        }
        Integer zkPort = _spoutConfig.zkPort;
        if (zkPort == null) {// 如果没有显示的指定端口，使用storm配置中的端口或者默认端口
            zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT))
                    .intValue();
        }

        // 将获取的zk端口等放置在TRANSACTIONAL变量中，不与原先的storm配置冲突，但会不会影响其他使用用途
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, _spoutConfig.zkRoot);

        // 根据上面的配置创建ZkState对象，实际上是CuratorFramework类型的zk客户端
        _state = new ZkState(stateConf);

        // DynamicPartitionConnections主要用来保存SimpleConsumer对象用以向kafka获取数据
        _connections = new DynamicPartitionConnections(_spoutConfig,
                KafkaUtils.makeBrokerReader(conf, _spoutConfig));

        // 获取topology中有多少个KafkaSpout
        // using TransactionalState like this is a hack
        int totalTasks = context
                .getComponentTasks(context.getThisComponentId()).size();

        // 根据传入的主机类型，获取Coordinator，Coordinator主要做什么工作？负责维护本spout需要获取的partition及其对应主机，连接，还有partition
        // manager用来
        // 管理offset
        if (_spoutConfig.hosts instanceof StaticHosts) {
            _coordinator = new StaticCoordinator(_connections, conf,
                    _spoutConfig, _state, context.getThisTaskIndex(),
                    totalTasks, _uuid);
        } else {
            _coordinator = new ZkCoordinatorLocal(_connections, conf,
                    _spoutConfig, _state, context, _uuid);
        }

        // 注册metric
        context.registerMetric("kafkaOffset", new IMetric() {
            KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(
                    _spoutConfig.topic, _connections);

            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = _coordinator
                        .getMyManagedPartitions();
                Set<Partition> latestPartitions = new HashSet();
                for (PartitionManager pm : pms) {
                    latestPartitions.add(pm.getPartition());
                }
                _kafkaOffsetMetric.refreshPartitions(latestPartitions);
                for (PartitionManager pm : pms) {
                    _kafkaOffsetMetric.setLatestEmittedOffset(
                            pm.getPartition(), pm.lastCompletedOffset());
                }
                return _kafkaOffsetMetric.getValueAndReset();
            }
        }, _spoutConfig.metricsTimeBucketSizeInSecs);

        context.registerMetric("kafkaPartition", new IMetric() {
            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = _coordinator
                        .getMyManagedPartitions();
                Map concatMetricsDataMaps = new HashMap();
                for (PartitionManager pm : pms) {
                    concatMetricsDataMaps.putAll(pm.getMetricsDataMap());
                }
                return concatMetricsDataMaps;
            }
        }, _spoutConfig.metricsTimeBucketSizeInSecs);
    }

    @Override
    public void close() {
        _state.close();
    }

    @Override
    public void nextTuple() {
        // 获取本spout负责拉取信息的kafka partition
        List<PartitionManager> managers = _coordinator.getMyManagedPartitions();
        for (int i = 0; i < managers.size(); i++) {

            // in case the number of managers decreased
            // zkcoordinator刷新的时候会导致partitionmanger数量变化
            _currPartitionIndex = _currPartitionIndex % managers.size();
            EmitState state = managers.get(_currPartitionIndex)
                    .next(_collector);
            // 如果本分区没有更多需要emit，则选择下一分区
            if (state != EmitState.EMITTED_MORE_LEFT) {
                _currPartitionIndex = (_currPartitionIndex + 1)
                        % managers.size();
            }
            // 如果状态不为NO_EMITTED则终止循环，一次只从一个分区读数据
            if (state != EmitState.NO_EMITTED) {
                break;
            }
        }

        long now = System.currentTimeMillis();
        // 如果超时，刷新zk信息
        if ((now - _lastUpdateMs) > _spoutConfig.stateUpdateIntervalMs) {
            commit();
        }
    }

    @Override
    public void ack(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        PartitionManager m = _coordinator.getManager(id.partition);
        if (m != null) {
            m.ack(id.offset);
        }
    }

    @Override
    public void fail(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        PartitionManager m = _coordinator.getManager(id.partition);
        if (m != null) {
            m.fail(id.offset);
        }
    }

    @Override
    public void deactivate() {
        commit();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_spoutConfig.scheme.getOutputFields());
    }

    private void commit() {
        _lastUpdateMs = System.currentTimeMillis();
        for (PartitionManager manager : _coordinator.getMyManagedPartitions()) {
            manager.commit();
        }
    }

}
