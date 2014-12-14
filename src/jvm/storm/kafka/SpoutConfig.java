package storm.kafka;

import java.io.Serializable;
import java.util.List;

public class SpoutConfig extends KafkaConfig implements Serializable {
    public List<String> zkServers = null;
    public Integer zkPort = null;
    public String zkRoot = null;
    public String id = null;
    public long stateUpdateIntervalMs = 2000;
    public Boolean batchMode = false;
    public int batchSize = 0;

    public SpoutConfig(BrokerHosts hosts, String topic, String zkRoot, String id) {
        // 实际传入的是zkHosts 主题 使用的zk根路径 spoutid
        super(hosts, topic);
        this.zkRoot = zkRoot;
        this.id = id;
    }
    
    public SpoutConfig(BrokerHosts hosts, String topic, String zkRoot, String id, int batchSize) {
        // 实际传入的是zkHosts 主题 使用的zk根路径 spoutid
        this(hosts, topic, zkRoot, id);
        this.batchMode = true;
        this.batchSize = batchSize;
    }
}
