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
import java.nio.file.Paths;

import org.apache.log4j.Logger;

/**
 * @author Nagarro Softwares Pvt. Ltd.
 *
 */
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
