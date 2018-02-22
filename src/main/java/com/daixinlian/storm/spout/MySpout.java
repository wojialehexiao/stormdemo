package com.daixinlian.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Duo Nuo on 2018/2/22 0022.
 */
public class MySpout extends BaseRichSpout {

	SpoutOutputCollector collector;
	Properties props = new Properties();
	KafkaConsumer<String, String> consumer;

	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		collector = spoutOutputCollector;

		props.put("bootstrap.servers", "192.168.10.249:9092");
		props.put("metadata.broker.list", "192.168.10.249:9092");
		props.put("zookeeper.connect", "192.168.10.188:2181");

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		/**
		 * cousumer的分组id
		 */
		props.put("group.id", "test");

		/**
		 * 自动提交offsets
		 */
		props.put("enable.auto.commit", "true");

		/**
		 * 每隔1s，自动提交offsets
		 */
		props.put("auto.commit.interval.ms", "1000");

		/**
		 * Consumer向集群发送自己的心跳，超时则认为Consumer已经死了，kafka会把它的分区分配给其他进程
		 */
		props.put("session.timeout.ms", "30000");

		 /* 消费者订阅的topic, 可同时订阅多个 */
		consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("test"));
//		consumer.seek(new TopicPartition("test",0),0);
	}

	public void nextTuple() {
        /* 读取数据，读取超时时间为100ms */
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
				collector.emit(new Values(record.value()));
			}
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("word"));
	}
}
