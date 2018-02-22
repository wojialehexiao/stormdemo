package com.daixinlian.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

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
