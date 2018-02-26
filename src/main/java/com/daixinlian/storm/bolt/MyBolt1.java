package com.daixinlian.storm.bolt;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Duo Nuo on 2018/2/22 0022.
 */
public class MyBolt1 extends BaseRichBolt {

	OutputCollector collector;

	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		collector = outputCollector;
	}

	public void execute(Tuple tuple) {
		String line = tuple.getString(0);
		String[] split = line.split(" ");
		for(String s : split){
			collector.emit(new Values(s));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("word"));
	}
}
