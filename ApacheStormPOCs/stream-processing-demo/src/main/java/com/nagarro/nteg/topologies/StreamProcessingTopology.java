/**
 * 
 */
package com.nagarro.nteg.topologies;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologySummary;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.nagarro.nteg.spouts.DirectoryFilesDataStreamSpout;

/**
 * @author amitkumar02
 *
 */
public class StreamProcessingTopology {
	
	public static final String DIRECTORY_PATH = "dirPath";
	
	public static void main(String[] args) {
		final TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		topologyBuilder.setSpout("file_lines_stream", new DirectoryFilesDataStreamSpout());
		
		topologyBuilder.setBolt("log", new BaseBasicBolt() {
			
			private static final long serialVersionUID = 1L;

			public void declareOutputFields(OutputFieldsDeclarer declarer) {
			}
			
			public void execute(Tuple input, BasicOutputCollector collector) {
				System.out.println(input.getString(0));
			}
		}).shuffleGrouping("file_lines_stream");
		
		final Map<String, Object> stormConf = new HashMap<String, Object>();
		stormConf.put(DIRECTORY_PATH, "/opt/app/data/storm/testdata/files");
		stormConf.put(TopologySummary._Fields.NUM_WORKERS.name(), 5);
		
		try {
			StormSubmitter.submitTopology("file_data_stream_processing", stormConf, topologyBuilder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
	}
}
