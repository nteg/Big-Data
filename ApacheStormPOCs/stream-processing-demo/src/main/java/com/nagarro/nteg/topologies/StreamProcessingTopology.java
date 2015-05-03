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
