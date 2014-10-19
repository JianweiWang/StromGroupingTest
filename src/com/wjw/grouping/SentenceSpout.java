package com.wjw.grouping;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout{
	public SpoutOutputCollector _collector = null;
	Long msgId = new Long(0);
	long startTime = new Date().getTime();
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}
	String[] sentences = {
			"the cow jumped over the moon", 
			"an apple a day keeps the doctor away",
	       "four score and seven years ago", 
	       "snow white and the seven dwarfs",
	       "i am at two with nature"
	};
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
//		if(new Date().getTime() - startTime < 1800000) {
//			Utils.sleep(10L);
//		} else {
//			Utils.sleep(1L);
//		}
		Utils.sleep(10L);
			Random rand = new Random(System.currentTimeMillis());
			String sentence = sentences[rand.nextInt(sentences.length)];
			//_collector.emit(new Values(sentence));
			_collector.emit(new Values(sentence),msgId);
			msgId++;
			//_collector.emitDirect(34, "sentence", new Values(sentence));
			//count++;
//		if(msgId == 100)
//			return;
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		System.out.println("messageId: " + msgId);
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("sentence"));
		//declarer.declareStream("sentence", true, new Fields("sentence"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
