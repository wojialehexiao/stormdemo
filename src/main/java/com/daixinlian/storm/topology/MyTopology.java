package com.daixinlian.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.daixinlian.storm.bolt.MyBolt1;
import com.daixinlian.storm.bolt.MyBolt2;
import com.daixinlian.storm.spout.MySpout;

/**
 * Created by Duo Nuo on 2018/2/22 0022.
 */
public class MyTopology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

		Config config = new Config();


		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("myspout",new MySpout(),2);
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
