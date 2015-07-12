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
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * @author Nagarro Softwares Pvt. Ltd.
 *
 */
public abstract class AbstractDirectoryFilesDataReader implements DirectoryFilesDataReader {

	private static final Logger LOG = Logger.getLogger(AbstractDirectoryFilesDataReader.class);
	
	protected final String dirPathName;
	protected final int batchSize;
	protected FileDataBufferedReader fileDataBufferedReader;
	
	protected AbstractDirectoryFilesDataReader(final File dirPath, final int batchSize) throws IOException {
		this(dirPath.getAbsolutePath(), batchSize);
	}
	
	protected AbstractDirectoryFilesDataReader(final String dirPathName, final int batchSize) throws IOException {
		this.batchSize = batchSize;
		this.dirPathName = dirPathName;
	}
	
	private boolean findFileToProcess() throws IOException {
		
		if(fileDataBufferedReader != null) {
			fileDataBufferedReader.close();
		}
		
		fileDataBufferedReader = getFileDataBufferedReaderForNewFile();
		
		return (fileDataBufferedReader != null);
	}
	
	protected abstract FileDataBufferedReader getFileDataBufferedReaderForNewFile() throws IOException;
	
	@Override
	public String nextLine() {
		
		String nextLine = null;
		boolean fileFound = true;
		
		if(fileDataBufferedReader == null || fileDataBufferedReader.isEndOfFile()) {
			try {
				fileFound = findFileToProcess();
			} catch (IOException e) {
				fileFound = false;
				LOG.error(e.getMessage(), e);
			}
		}
		
		if(fileFound) {
			nextLine = fileDataBufferedReader.nextLine();
		}
		
		return nextLine;
	}

	@Override
	public String getCurrentDataFileName() {
		return fileDataBufferedReader.getFileName();
	}
}
