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
package com.nagarro.nteg.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;

/**
 * @author Nagarro Softwares Pvt. Ltd.
 *
 */
public class HDFSDirectoryFilesDataReader extends AbstractDirectoryFilesDataReader {
	
	private static final Logger LOG = Logger.getLogger(HDFSDirectoryFilesDataReader.class);
	
	private final FileSystem hdfs;
	
	public HDFSDirectoryFilesDataReader(final String dirPathName, final int batchSize) throws IOException {
		super(dirPathName, batchSize);
		
		final Path hdfsPath = new Path(dirPathName);
		hdfs = FileSystem.get(hdfsPath.toUri(), new Configuration());
		
		if(!hdfs.getFileStatus(hdfsPath).isDirectory()) {
			hdfs.close();
			throw new IllegalArgumentException(dirPathName + " is not a directory or program doesn't have sufficient permissions to access it");
		}
	}
	
	/* (non-Javadoc)
	 * @see com.nagarro.nteg.utils.AbstractDirectoryFilesDataReader#getFileDataBufferedReaderForNewFile()
	 */
	@Override
	protected FileDataBufferedReader getFileDataBufferedReaderForNewFile()
			throws IOException {
		
		final Path hdfsPath = new Path(dirPathName);
		
		Path locatedFilePath = null;
		final RemoteIterator<LocatedFileStatus> locatedFileIterator = hdfs.listFiles(hdfsPath, true);
		while (locatedFileIterator != null && locatedFileIterator.hasNext()) {
			final LocatedFileStatus locatedFileStatus = locatedFileIterator.next();

			final Path tmpPath = locatedFileStatus.getPath();

			final String pathName = tmpPath.getName();

			if (LOG.isInfoEnabled()) {
				LOG.info("Checking file with name[Log]: " + pathName);
			}

			 if(!(pathName.endsWith(FileDataBufferedReader.IN_PROGRESS_FILE_SUFFIX) 
					 					|| pathName.endsWith(FileDataBufferedReader.PROCESSED_FILE_SUFFIX))) {
				 locatedFilePath = tmpPath;
				 break;
			 }
		}
		
		FileDataBufferedReader fileDataBufferedReader = null;
		if(locatedFilePath != null) {
			fileDataBufferedReader =  new HDFSFileDataBufferedReader(locatedFilePath, batchSize);
		}
		
		return fileDataBufferedReader;
	}
}
