package storm.kafka;

import java.util.List;

public interface PartitionCoordinatorBatch {
    List<PartitionManagerBatch> getMyManagedPartitions();

    PartitionManagerBatch getManager(Partition partition);
}
