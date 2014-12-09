package storm.kafka;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.GlobalPartitionInformation;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

public class DynamicSupervisorReader {

    public static final Logger LOG = LoggerFactory
            .getLogger(DynamicSupervisorReader.class);

    private CuratorFramework _curator;
    private String _zkPath;

    public DynamicSupervisorReader(Map conf, String zkStr, String zkPath) {
        _zkPath = zkPath;
        // try {
        // 获取zk curator客户端
        _curator = CuratorFrameworkFactory.newClient(
                zkStr,
                Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
                15000,
                new RetryNTimes(Utils.getInt(conf
                        .get(Config.STORM_ZOOKEEPER_RETRY_TIMES)),
                        Utils.getInt(conf
                                .get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
        _curator.start();
        // } catch (IOException ex) {
        // LOG.error("can't connect to zookeeper");
        // }
    }

    // 获取topic各partition的leader的主机端口信息
    /**
     * Get all partitions with their current leaders
     */
    public GlobalPartitionInformation getBrokerInfo() {
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
        try {
            int numPartitionsForTopic = getNumPartitions();
            String brokerInfoPath = brokerPath();
            // 为每个partition获取Leader主机和端口信息
            for (int partition = 0; partition < numPartitionsForTopic; partition++) {
                int leader = getLeaderFor(partition);
                String path = brokerInfoPath + "/" + leader;
                try {
                    byte[] brokerData = _curator.getData().forPath(path);
                    Broker hp = getBrokerHost(brokerData);
                    globalPartitionInformation.addPartition(partition, hp);
                } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
                    LOG.error("Node {} does not exist ", path);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.info("Read partition info from zookeeper: "
                + globalPartitionInformation);
        return globalPartitionInformation;
    }

    // 获取本topic的partition数
    private int getNumPartitions() {
        try {
            String topicBrokersPath = partitionPath();
            List<String> children = _curator.getChildren().forPath(
                    topicBrokersPath);
            return children.size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 获取本topic的分区信息路径
    public String partitionPath() {
        return _zkPath + "/topics/" + "/partitions";
    }

    // 获取kafka broker节点信息
    public String brokerPath() {
        return _zkPath + "/ids";
    }

    // 获取zk中的分区状态信息得到leader id
    /**
     * get /brokers/topics/distributedTopic/partitions/1/state {
     * "controller_epoch":4, "isr":[ 1, 0 ], "leader":1, "leader_epoch":1,
     * "version":1 }
     * 
     * @param partition
     * @return
     */
    private int getLeaderFor(long partition) {
        try {
            String topicBrokersPath = partitionPath();
            byte[] hostPortData = _curator.getData().forPath(
                    topicBrokersPath + "/" + partition + "/state");
            Map<Object, Object> value = (Map<Object, Object>) JSONValue
                    .parse(new String(hostPortData, "UTF-8"));
            Integer leader = ((Number) value.get("leader")).intValue();
            return leader;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        _curator.close();
    }

    // 解析zk返回的json信息得到broker的主机和端口
    /**
     * [zk: localhost:2181(CONNECTED) 56] get /brokers/ids/0 {
     * "host":"localhost", "jmx_port":9999, "port":9092, "version":1 }
     * 
     * @param contents
     * @return
     */
    private Broker getBrokerHost(byte[] contents) {
        try {
            Map<Object, Object> value = (Map<Object, Object>) JSONValue
                    .parse(new String(contents, "UTF-8"));
            String host = (String) value.get("host");
            Integer port = ((Long) value.get("port")).intValue();
            return new Broker(host, port);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

}
