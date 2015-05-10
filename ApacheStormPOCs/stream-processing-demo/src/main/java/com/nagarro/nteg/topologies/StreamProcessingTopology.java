/**
 * Copyright 2015 Nagarro Softwares Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nagarro.nteg.topologies;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

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
 * @author Nagarro Softwares Pvt. Ltd.
 *
 */
public class StreamProcessingTopology {
	
	private static final Logger LOG = Logger.getLogger(StreamProcessingTopology.class);
	
	private static final int NUM_WORKERS = 2;
	private static final String TOPOLOGY_NAME = "file_data_stream_processing";
	
	public static final String DIRECTORY_PATH = "dirPath";
	
	
	public static void main(String[] args) {
		
		if(args.length == 0) {
			throw new IllegalArgumentException("Please specify the directory path containing the files to process");
		} 
		
		final String dirPath = args[0];
		
		int numOfWorkers = NUM_WORKERS;
		if(args.length > 1) {
			try {
				numOfWorkers = Integer.parseInt(args[1]);
			} catch (NumberFormatException numberFormatException) {
				LOG.warn("Invalid value for number of workers: "  + args[1] + ", going with default: " + NUM_WORKERS);
			}
		} 
		
		
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
		stormConf.put(DIRECTORY_PATH, dirPath);
		stormConf.put(TopologySummary._Fields.NUM_WORKERS.name(), numOfWorkers);
		
		try {
			StormSubmitter.submitTopology(TOPOLOGY_NAME, stormConf, topologyBuilder.createTopology());
		} catch (AlreadyAliveException e) {
			LOG.fatal(e.getMessage(), e);
		} catch (InvalidTopologyException e) {
			LOG.fatal(e.getMessage(), e);
		}
	}
}
