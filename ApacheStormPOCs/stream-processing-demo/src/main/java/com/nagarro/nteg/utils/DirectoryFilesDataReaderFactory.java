package com.nagarro.nteg.utils;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.log4j.Logger;

public class DirectoryFilesDataReaderFactory {
	
	private static final Logger LOG = Logger.getLogger(LocalDirectoryFilesDataReader.class);
	
	public static DirectoryFilesDataReader getDirectoryFilesDataReader(final String dirPath, final int batchSize) throws IOException {
		DirectoryFilesDataReader directoryFilesDataReader = null;
		
		final String scheme = Paths.get(dirPath).toUri().getScheme();
		if("file".equalsIgnoreCase(scheme)) {
			directoryFilesDataReader = new LocalDirectoryFilesDataReader(dirPath, batchSize);
		} else {
			throw new UnsupportedOperationException("URI scheme: " + scheme + " not supported.");
		}
		
		return directoryFilesDataReader; 
	}

}
