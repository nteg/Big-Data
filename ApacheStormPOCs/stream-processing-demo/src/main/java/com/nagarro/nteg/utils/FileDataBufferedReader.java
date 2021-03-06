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

/**
 * @author Nagarro Softwares Pvt. Ltd.
 *
 */
public interface FileDataBufferedReader {
	String IN_PROGRESS_FILE_SUFFIX = ".in-progress";
	String PROCESSED_FILE_SUFFIX = ".processed";
	
	boolean isEndOfFile();
	
	String nextLine();
	
	void close() throws IOException;
	
	String getFileName();
}
