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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Nagarro Softwares Pvt. Ltd.
 * 
 */
public class HDFSFileDataBufferedReader implements FileDataBufferedReader {

	private final int batchSize;
	private final Path currentFile;
	private final Path internalRenamedFile;
	private final Scanner scanner;
	private final FileSystem fileSystem;

	protected final Deque<String> buffer;

	/**
	 * @param path
	 * @param batchSize
	 * @throws IOException
	 */
	public HDFSFileDataBufferedReader(final Path path, final int batchSize) throws IOException {
		this.batchSize = batchSize;
		this.currentFile = path;
		this.buffer = new ArrayDeque<String>(batchSize);
		internalRenamedFile = path.suffix(IN_PROGRESS_FILE_SUFFIX);
		
		fileSystem = path.getFileSystem(new Configuration());

		fileSystem.rename(path, internalRenamedFile);
		
		scanner = new Scanner(fileSystem.open(internalRenamedFile));
	}

	protected void fillNextBatchIfBufferEmpty() {

		if (buffer.isEmpty()) {
			
			for (int i = 0; i < batchSize; i++) {
				
				if (scanner.hasNextLine()) {
					buffer.addFirst(scanner.nextLine());
				}
			}
		}
	}

	@Override
	public boolean isEndOfFile() {
		
		fillNextBatchIfBufferEmpty();
		return (buffer.peekLast() == null);
	}

	@Override
	public String nextLine() {
		
		fillNextBatchIfBufferEmpty();
		return buffer.pollLast();
	}

	@Override
	public String getFileName() {
		return currentFile.getName();
	}

	@Override
	public void close() throws IOException {
		if(scanner != null) {
			scanner.close();
		}
		
		fileSystem.rename(internalRenamedFile, currentFile.suffix(PROCESSED_FILE_SUFFIX));
	}

}
