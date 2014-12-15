package storm.kafka;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.task.Assignment;
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
    private String _zkStr;
    private String _zkPath;
    private String _topologyId;

    public DynamicSupervisorReader(Map conf, String zkStr, String topologyId) {
        _zkStr = zkStr;
        _topologyId = topologyId;
        _zkPath = (String) conf.get(Config.STORM_ZOOKEEPER_ROOT);
        // try {
        // 获取zk curator客户端
        _curator = CuratorFrameworkFactory.newClient(
                _zkStr,
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

    public void close() {
        _curator.close();
    }

    // 获取本jstorm assignment
    public String assignmentPath() {
        return _zkPath + Cluster.assignment_path(_topologyId);
    }

    public Assignment getAssignmentInfo() {
        byte[] assignmentData = null;
        try {
            assignmentData = _curator.getData().forPath(assignmentPath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Object data = Cluster.maybe_deserialize(assignmentData);
        if (data == null) {
            return null;
        }
        return (Assignment) data;
    }

}
