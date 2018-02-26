package com.daixinlian.storm.topology;

import com.daixinlian.storm.bolt.MyBolt1;
import com.daixinlian.storm.bolt.MyBolt2;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;

/**
 * Created by Duo Nuo on 2018/2/22 0022.
 */
public class MyTopology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {

		Config config = new Config();


		TopologyBuilder builder = new TopologyBuilder();

		String zks = "192.168.10.22:2181,192.168.10.23:2181,192.168.10.24:2181";
		String topic = "test";
		String zkRoot = "/storm"; // default zookeeper root configuration for storm
		String id = "word";

		BrokerHosts brokerHosts = new ZkHosts(zks);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
//		spoutConf.forceFromStart = true;
		spoutConf.zkServers = Arrays.asList(new String[] {"192.168.10.22","192.168.10.23","192.168.10.24"});
		spoutConf.zkPort = 2181;

		KafkaSpout kafkaSpout = new KafkaSpout(spoutConf);

		builder.setSpout("myspout",kafkaSpout,3);
		builder.setBolt("mybolt1",new MyBolt1(),2).shuffleGrouping("myspout");

		// 输出字段分隔符
		RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

		// 每1000个tuple同步到HDFS一次
		SyncPolicy syncPolicy = new CountSyncPolicy(1000);

		// 每个写出文件的大小为100MB
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(100.0f, FileSizeRotationPolicy.Units.MB);

		// 设置输出目录
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(args[0]);

		// 执行HDFS地址
		HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://node1:8020").withFileNameFormat(fileNameFormat)
				.withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);


		builder.setBolt("mybolt2",new MyBolt2(),4).fieldsGrouping("mybolt1",new Fields("word"));

		config.setNumWorkers(2);

		if(args != null && args.length > 0){
			StormSubmitter.submitTopology("wordcount",config,builder.createTopology());
		}else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("wordcount",config,builder.createTopology());
			System.out.println("本地调试已经启动");
		}
	}
}
