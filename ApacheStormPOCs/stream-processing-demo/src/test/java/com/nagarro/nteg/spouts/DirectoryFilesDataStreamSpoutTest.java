package com.nagarro.nteg.spouts;

import java.io.IOException;

import org.junit.Test;

import com.nagarro.nteg.utils.DirectoryFilesDataReaderFactory;

public class DirectoryFilesDataStreamSpoutTest {

	@Test
	public void testNextTuple() throws IOException {
		DirectoryFilesDataStreamSpout dataStreamSpout = new DirectoryFilesDataStreamSpout();
		dataStreamSpout.setDirectoryFilesDataReader(DirectoryFilesDataReaderFactory.getDirectoryFilesDataReader("E:\\sumit\\data files", 10));
		
		for(int i=0; i < 10000; i++) {
			dataStreamSpout.nextTuple();
		}
		
	}

}
