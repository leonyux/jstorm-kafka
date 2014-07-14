package storm.kafka.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.DynamicBrokersReader;
import storm.kafka.ZkHosts;

import java.util.Map;

public class ZkBrokerReader implements IBrokerReader {

	public static final Logger LOG = LoggerFactory
			.getLogger(ZkBrokerReader.class);

	GlobalPartitionInformation cachedBrokers;
	DynamicBrokersReader reader;
	long lastRefreshTimeMs;

	long refreshMillis;

	public ZkBrokerReader(Map conf, String topic, ZkHosts hosts) {
		// DynamicBrokerReader是真正的BrokerReader，用来从zk中获取topic各partition的broker主机和端口
		reader = new DynamicBrokersReader(conf, hosts.brokerZkStr,
				hosts.brokerZkPath, topic);
		// 获取各partition与broker的映射
		cachedBrokers = reader.getBrokerInfo();
		lastRefreshTimeMs = System.currentTimeMillis();
		// ZkHost中写死的固定60秒
		refreshMillis = hosts.refreshFreqSecs * 1000L;

	}

	// 返回partition到broker的映射，只不过有超时从zk刷新，如果没超时但是zk的信息已经和缓存的不同了？
	@Override
	public GlobalPartitionInformation getCurrentBrokers() {
		long currTime = System.currentTimeMillis();
		if (currTime > lastRefreshTimeMs + refreshMillis) {
			LOG.info("brokers need refreshing because " + refreshMillis
					+ "ms have expired");
			cachedBrokers = reader.getBrokerInfo();
			lastRefreshTimeMs = currTime;
		}
		return cachedBrokers;
	}

	@Override
	public void close() {
		reader.close();
	}
}
