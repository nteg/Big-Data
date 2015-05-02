/**
 * 
 */
package com.nagarro.nteg.spouts;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import com.nagarro.nteg.topologies.StreamProcessingTopology;
import com.nagarro.nteg.utils.DirectoryFilesDataReader;
import com.nagarro.nteg.utils.DirectoryFilesDataReaderFactory;

/**
 * @author amitkumar02
 *
 */
public class DirectoryFilesDataStreamSpout extends BaseRichSpout{
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(DirectoryFilesDataStreamSpout.class);
	
	private SpoutOutputCollector collector;
	
	private DirectoryFilesDataReader directoryFilesDataReader = null;
	
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		final String directoryPath = (String) conf.get(StreamProcessingTopology.DIRECTORY_PATH);
		
		try {
			directoryFilesDataReader = DirectoryFilesDataReaderFactory.getDirectoryFilesDataReader(directoryPath, 10);
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read files from directory: " + directoryPath);
		}
	}

	public void nextTuple() {
		String line = directoryFilesDataReader.nextLine();
		
		System.out.println("Emitting tuple: " + line);
		LOG.info("Emitting tuple[LOGGER]: " + line);
		collector.emit(Collections.singletonList((Object)line), UUID.randomUUID());
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("file_line"));
	}

	public void setDirectoryFilesDataReader(DirectoryFilesDataReader directoryFilesDataReader) {
		this.directoryFilesDataReader = directoryFilesDataReader;
	}


}
