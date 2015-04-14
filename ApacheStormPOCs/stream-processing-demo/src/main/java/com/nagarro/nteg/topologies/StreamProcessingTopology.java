/**
 * 
 */
package com.nagarro.nteg.topologies;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Tuple;

/**
 * @author amitkumar02
 *
 */
public class StreamProcessingTopology {
	public static void main(String[] args) {
		final TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		topologyBuilder.setSpout("words_stream", new BaseRichSpout() {
			
			public void declareOutputFields(OutputFieldsDeclarer arg0) {
				// TODO Auto-generated method stub
				
			}
			
			public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
				// TODO Auto-generated method stub
				
			}
			
			public void nextTuple() {
				// TODO Auto-generated method stub
				
			}
		});
		
		topologyBuilder.setBolt("log", new BaseBasicBolt() {
			
			private static final long serialVersionUID = 1L;

			public void declareOutputFields(OutputFieldsDeclarer arg0) {
				// TODO Auto-generated method stub
				
			}
			
			public void execute(final Tuple tuple, final BasicOutputCollector basicOutputCollector) {
				System.out.println(tuple.getValue(0));
				
			}
		}).shuffleGrouping("words_stream");
	}
}
