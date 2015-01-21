package storm.kafka;

import backtype.storm.task.IMetricsContext;
import backtype.storm.task.TopologyContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.Assignment;

import storm.kafka.trident.GlobalPartitionInformation;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class ZkCoordinatorLocal implements PartitionCoordinator {
    public static final Logger LOG = LoggerFactory
            .getLogger(ZkCoordinatorLocal.class);

    SpoutConfig _spoutConfig;
    int _taskIndex;
    int _totalTasks;
    String _topologyInstanceId;
    Map<Partition, PartitionManager> _managers = new HashMap();
    List<PartitionManager> _cachedList;
    Long _lastRefreshTime = null;
    int _refreshFreqMs;
    DynamicPartitionConnections _connections;
    DynamicBrokersReader _reader;
    DynamicSupervisorReader _sreader;
    ZkState _state;
    Map _stormConf;
    IMetricsContext _metricsContext;
    String _hostname;
    String _hostIp;
    String _topic;
    Boolean _localMode;
    String _topologyId;
    List<Integer> _tasks;
    int _taskId;

    public ZkCoordinatorLocal(DynamicPartitionConnections connections,
            Map stormConf, SpoutConfig spoutConfig, ZkState state,
            final TopologyContext context, String topologyInstanceId) {
        _spoutConfig = spoutConfig;
        _connections = connections;// 管理与需要通信的各kafka主机的连接客户端
        _topologyInstanceId = topologyInstanceId;// kafkaspout的uuid
        _stormConf = stormConf;
        _state = state;// ZkStat，用来返回zk客户端
        _taskIndex = context.getThisTaskIndex();// 本kafkaspout task id
        _topologyId = context.getTopologyId();
        _tasks = context.getComponentTasks(context.getThisComponentId());
        _totalTasks = _tasks.size();// kafkaspout task数量
        _taskId = context.getThisTaskId();

        ZkHosts brokerConf = (ZkHosts) spoutConfig.hosts;
        _refreshFreqMs = brokerConf.refreshFreqSecs * 1000;
        // 同样需要获取一个DynamicBrokerReader
        _reader = new DynamicBrokersReader(stormConf, brokerConf.brokerZkStr,
                brokerConf.brokerZkPath, spoutConfig.topic);
        _topic = spoutConfig.topic;
        _sreader = new DynamicSupervisorReader(stormConf,
                brokerConf.brokerZkStr, _topologyId);
        try {
            InetAddress addr = InetAddress.getLocalHost();
            _hostname = addr.getCanonicalHostName();
            _hostIp = addr.getHostAddress();
            _localMode = true;
            LOG.info("in localmode");
        } catch (UnknownHostException e) {
            LOG.error("local_hostname", e);
            e.printStackTrace();
            _localMode = false;
            LOG.warn("not in localmode");
        }

    }

    // 返回定时刷新的partitionmanager
    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        if (_lastRefreshTime == null
                || (System.currentTimeMillis() - _lastRefreshTime) > _refreshFreqMs) {
            refresh();
            _lastRefreshTime = System.currentTimeMillis();
        }
        return _cachedList;
    }

    void refresh() {
        try {
            LOG.info("Refreshing partition manager connections");
            GlobalPartitionInformation brokerInfo = _reader.getBrokerInfo();
            Assignment assignment = _sreader.getAssignmentInfo();
            LOG.info("Assignement: " + assignment);
            Set<Partition> mine;
            if (_localMode) {
                mine = myOwnershipLocal(brokerInfo, assignment);
            } else {
                mine = new HashSet<Partition>();
                for (Partition partitionId : brokerInfo) {
                    // 判断本任务是否负责这个partition，一个partition仅能被一个kafkaspout处理
                    if (myOwnership(partitionId)) {
                        mine.add(partitionId);
                    }
                }
            }
            LOG.info("My topic: " + _topic);
            LOG.info("My partition managers: " + mine.toString());

            synchronized (_managers) {// 锁定_manager结构，在jstorm下支持nextTuple和ack、fail的并发
                Set<Partition> curr = _managers.keySet();
                Set<Partition> newPartitions = new HashSet<Partition>(mine);
                // 为什么要从新获取的partition中删除当前的partition？
                newPartitions.removeAll(curr);

                // 获取被删除的分区
                Set<Partition> deletedPartitions = new HashSet<Partition>(curr);
                deletedPartitions.removeAll(mine);

                LOG.info("Deleted partition managers: "
                        + deletedPartitions.toString());

                // 关闭移除的partition的manager
                for (Partition id : deletedPartitions) {
                    PartitionManager man = _managers.remove(id);
                    man.close();
                }
                LOG.info("New partition managers: " + newPartitions.toString());

                // 为新增的partition构建partition manager，partition
                // manger主要管理向broker获取数据的一些状态，最主要的是offset
                for (Partition id : newPartitions) {
                    PartitionManager man = null;
                    if (_spoutConfig.batchMode) {
                        man = new PartitionManagerBatch(_connections,
                                _topologyInstanceId, _state, _stormConf,
                                _spoutConfig, id);
                    } else {
                        man = new PartitionManagerSingle(_connections,
                                _topologyInstanceId, _state, _stormConf,
                                _spoutConfig, id);
                    }
                    _managers.put(id, man);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        _cachedList = new ArrayList<PartitionManager>(_managers.values());
        LOG.info("Finished refreshing");
    }

    @Override
    public PartitionManager getManager(Partition partition) {
        synchronized (_managers) {// 锁定_manager结构，在jstorm下支持nextTuple和ack、fail的并发
            return _managers.get(partition);
        }
    }

    // 根据这个算法映射partitions对spout的分配
    /*
     * private boolean myOwnership(Partition id) { int val =
     * Math.abs(id.host.hashCode() + 23 * id.partition); return val %
     * _totalTasks == _taskIndex; }
     */
    // 原有算法有时会导致不均匀
    // Local版本试图将partition分配到本地spout
    private boolean myOwnership(Partition id) {
        return id.partition % _totalTasks == _taskIndex;
    }

    // 希望获得一个只消费本机partition的分布方式
    private Set<Partition> myOwnershipLocal(
            GlobalPartitionInformation brokerInfo, Assignment assignment) {
        Set<Partition> mine = new HashSet<Partition>();
        Set<ResourceWorkerSlot> workers = assignment.getWorkers();
        List<Integer> localtasks = new ArrayList<Integer>();
        for (ResourceWorkerSlot worker : workers) {
            if (worker.getHostname().equals(_hostname)
                    || worker.getHostname().equals(_hostIp))
                localtasks.addAll(worker.getTasks());
        }
        List<Integer> localComponentTasks = new ArrayList<Integer>(_tasks);
        localComponentTasks.retainAll(localtasks);
        Collections.sort(localComponentTasks);
        Map<Integer, Partition> localPartitions = new HashMap<Integer, Partition>();
        for (Partition partitionId : brokerInfo) {
            if (partitionId.host.host.equals(_hostname)
                    || partitionId.host.host.equals(_hostIp)) {
                localPartitions.put(partitionId.partition, partitionId);
            }
        }
        List<Integer> partitions = new ArrayList<Integer>(
                localPartitions.keySet());
        Collections.sort(partitions);
        int numTasks = localComponentTasks.size();
        int numPartitions = partitions.size();
        int thisTaskIndex = localComponentTasks.indexOf(_taskId);
        for (int index = 0; index < numPartitions; index++) {
            if (index % numTasks == thisTaskIndex) {
                mine.add(localPartitions.get(partitions.get(index)));
            }
        }
        return mine;
    }
}
