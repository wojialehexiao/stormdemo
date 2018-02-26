package com.daixinlian.storm.topology;

import com.daixinlian.storm.bolt.MyBolt1;
import com.daixinlian.storm.bolt.MyBolt2;
import com.daixinlian.storm.spout.MySpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
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
