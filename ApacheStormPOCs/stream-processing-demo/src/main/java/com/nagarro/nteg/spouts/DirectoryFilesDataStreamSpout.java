/**
 * 
 */
package com.nagarro.nteg.spouts;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import com.nagarro.nteg.topologies.StreamProcessingTopology;
import com.nagarro.nteg.utils.FixedLinesBatchFileReader;

/**
 * @author amitkumar02
 *
 */
public class DirectoryFilesDataStreamSpout extends BaseRichSpout{
	
	private static final long serialVersionUID = 1L;
	
	private String directoryPath;
	private SpoutOutputCollector collector;
	
	private FixedLinesBatchFileReader fiLinesBatchFileReader = null;
	
	private final void findAnotherDataStream() {
		
		finalizeDataStream();
	
		
		final File dir = new File(directoryPath);
		final File[] filePaths =  dir.listFiles(new FilenameFilter() {
			
			public boolean accept(File dir, String name) {
				
				if(name.endsWith(".in_progress")) {
					return false;
				}

				if(name.endsWith(".processed")) {
					return false;
				}
				
				return true;
			}
		});
		
		if(filePaths.length == 0 ) {
			try {
				Thread.sleep(100000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return;
		}
		
		for(File filePath : filePaths) {
			try {
				final Path targetPath = Paths.get(filePath + ".in_progress");
				Files.move(filePath.toPath(), targetPath);
				
				fiLinesBatchFileReader = new FixedLinesBatchFileReader(targetPath, 10);
				return;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private final void finalizeDataStream() {
		
		if(fiLinesBatchFileReader == null) {
			return;
		}
		
		//close current file reader
		fiLinesBatchFileReader.close();
		
		final Path sourcePath = Paths.get(fiLinesBatchFileReader.getFileName());
		
		final String targetPathFileName = fiLinesBatchFileReader.getFileName().substring(0, fiLinesBatchFileReader.getFileName().lastIndexOf(".in_progress"));
		final Path targetPath = Paths.get(targetPathFileName);
		
		try {
			Files.move(sourcePath, targetPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		directoryPath = (String) conf.get(StreamProcessingTopology.DIRECTORY_PATH);
		
		final Path path = Paths.get(directoryPath);
		
		if(!Files.isDirectory(path)) {
			throw new IllegalArgumentException(directoryPath + " is not a directory or program doesn't have sufficient permissions to access it");
		}
		
		findAnotherDataStream();
	}

	public void nextTuple() {
		String line = fiLinesBatchFileReader.nextLine();
		
		do {			
			if(line == null) {
				findAnotherDataStream();
				
				line = fiLinesBatchFileReader.nextLine();
			} 
			
		} while(line == null);
		
		collector.emit(Collections.singletonList((Object)line), UUID.randomUUID());
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("file_line"));
	}

}
