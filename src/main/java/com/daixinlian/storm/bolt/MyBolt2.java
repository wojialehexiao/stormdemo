package com.daixinlian.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Duo Nuo on 2018/2/22 0022.
 */
public class MyBolt2 extends BaseRichBolt {

	OutputCollector collector;

	Map<String,Integer> map = new HashMap<String, Integer>();

	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		collector = outputCollector;
	}

	public void execute(Tuple tuple) {
		String word = tuple.getString(0);
		Integer count = map.get(word);
		if(count == null){
			map.put(word,1);
		}else {
			map.put(word, count + 1);
		}

		System.out.println("----------------------------------");
		System.out.println(map);
		System.out.println("----------------------------------");
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}
}
