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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.log4j.Logger;

/**
 * @author Nagarro Softwares Pvt. Ltd.
 *
 */
public class LocalDirectoryFilesDataReader extends AbstractDirectoryFilesDataReader{
	
	private static final Logger LOG = Logger.getLogger(LocalDirectoryFilesDataReader.class);
	
	public LocalDirectoryFilesDataReader(final File dirPath, final int batchSize) throws IOException {
		super(dirPath, batchSize);
	}
	
	public LocalDirectoryFilesDataReader(final String dirPathName, final int batchSize) throws IOException {
		super(dirPathName, batchSize);
		
		if(!Files.isDirectory(Paths.get(dirPathName))) {
			throw new IllegalArgumentException(dirPathName + " is not a directory or program doesn't have sufficient permissions to access it");
		}
	}
	
	/* (non-Javadoc)
	 * @see com.nagarro.nteg.utils.AbstractDirectoryFilesDataReader#getFileDataBufferedReaderForNewFile()
	 */
	@Override
	protected FileDataBufferedReader getFileDataBufferedReaderForNewFile() throws IOException {
		
		FileDataBufferedReader fileDataBufferedReader = null;
		
		final File dir = new File(dirPathName);
		final File[] filePaths =  dir.listFiles(new FilenameFilter() {
			
			public boolean accept(File dir, String name) {
				
				if(LOG.isInfoEnabled()) {
					LOG.info("Checking file with name[Log]: " + name);
				}
				
				final File fileToTest = dir.toPath().resolve(name).toFile();
				
				if(!fileToTest.isFile()) {
					return false;
				}
				
				return !(name.endsWith(FileDataBufferedReader.IN_PROGRESS_FILE_SUFFIX) || name.endsWith(FileDataBufferedReader.PROCESSED_FILE_SUFFIX));
			}
		});
		
		if(filePaths != null && filePaths.length > 0) {
			fileDataBufferedReader =  new LocalFileDataBufferedReader(filePaths[0].toPath(), batchSize);
		}
		
		return fileDataBufferedReader;
	}

}
