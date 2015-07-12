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
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Nagarro Softwares Pvt. Ltd.
 *
 */
public class DirectoryFilesDataReaderFactory {
	
	public static DirectoryFilesDataReader getDirectoryFilesDataReader(final String dirPath, final int batchSize) throws IOException, URISyntaxException {
		DirectoryFilesDataReader directoryFilesDataReader = null;
		
		final URI uri = new URI(dirPath);
		final String scheme = uri.getScheme();
		
		if("hdfs".equalsIgnoreCase(scheme)){
			directoryFilesDataReader = new HDFSDirectoryFilesDataReader(dirPath, batchSize);
		} else {
			directoryFilesDataReader = new LocalDirectoryFilesDataReader(dirPath, batchSize);
		}
		
		return directoryFilesDataReader; 
	}

}
