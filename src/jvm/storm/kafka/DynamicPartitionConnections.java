package storm.kafka;

import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.IBrokerReader;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

//用来管理与kafka各主机的连接客户端
public class DynamicPartitionConnections {

    public static final Logger LOG = LoggerFactory
            .getLogger(DynamicPartitionConnections.class);

    static class ConnectionInfo {
        SimpleConsumer consumer;
        Set<Integer> partitions = new HashSet();

        public ConnectionInfo(SimpleConsumer consumer) {
            this.consumer = consumer;
        }
    }

    Map<Broker, ConnectionInfo> _connections = new HashMap();
    KafkaConfig _config;
    IBrokerReader _reader;

    public DynamicPartitionConnections(KafkaConfig config,
            IBrokerReader brokerReader) {
        _config = config;
        _reader = brokerReader;
    }

    public SimpleConsumer register(Partition partition) {
        // 获取partition所在的broker主机和端口
        Broker broker = _reader.getCurrentBrokers().getBrokerFor(
                partition.partition);
        return register(broker, partition.partition);
    }

    // 为某partition构造存储ConnectionInfo
    public SimpleConsumer register(Broker host, int partition) {
        if (!_connections.containsKey(host)) {
            _connections.put(host, new ConnectionInfo(new SimpleConsumer(
                    host.host, host.port, _config.socketTimeoutMs,
                    _config.bufferSizeBytes, _config.clientId)));
        }
        // 为对应host的ConncetionInfo增加分区信息，分区信息中包含的就是主机涉及的所有分区信息以及对于该主机的kafka客户端
        ConnectionInfo info = _connections.get(host);
        info.partitions.add(partition);
        return info.consumer;
    }

    public SimpleConsumer getConnection(Partition partition) {
        ConnectionInfo info = _connections.get(partition.host);
        if (info != null) {
            return info.consumer;
        }
        return null;
    }

    public void unregister(Broker port, int partition) {
        ConnectionInfo info = _connections.get(port);
        info.partitions.remove(partition);
        if (info.partitions.isEmpty()) {
            info.consumer.close();
            _connections.remove(port);
        }
    }

    public void unregister(Partition partition) {
        unregister(partition.host, partition.partition);
    }

    public void clear() {
        for (ConnectionInfo info : _connections.values()) {
            info.consumer.close();
        }
        _connections.clear();
    }
}
