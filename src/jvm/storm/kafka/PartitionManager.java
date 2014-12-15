package storm.kafka;

import java.util.Map;

import storm.kafka.KafkaSpout.EmitState;
import backtype.storm.spout.SpoutOutputCollector;

public interface PartitionManager {
    public Map getMetricsDataMap();

    public EmitState next(SpoutOutputCollector collector);

    public void ack(Long offset);

    public void fail(Long offset);

    public void commit();

    public long queryPartitionOffsetLatestTime();

    public long lastCommittedOffset();

    public long lastCompletedOffset();

    public Partition getPartition();

    public void close();

    static class KafkaMessageId {
        public Partition partition;
        public long offset;

        public KafkaMessageId(Partition partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }
}
