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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Scanner;

/**
 * @author Nagarro Softwares Pvt. Ltd.
 *
 */
public class LocalFileDataBufferedReader implements FileDataBufferedReader{
	
	private final int batchSize;
	private final Scanner scanner;
	private final Path currentFile;
	private final Path internalRenamedFile;
	protected final Deque<String> buffer;
	
	public LocalFileDataBufferedReader(final Path file, final int batchSize) throws IOException {
		
		this.currentFile = file;
		this.batchSize = batchSize;
		this.buffer = new ArrayDeque<String>(batchSize);
		
		this.internalRenamedFile = Paths.get(file.toString() + IN_PROGRESS_FILE_SUFFIX);
		Files.move(file, internalRenamedFile);
		
		this.scanner = new Scanner(internalRenamedFile);
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
	public void close() throws IOException {
		if(scanner != null) {
			scanner.close();
		}
		
		Files.move(internalRenamedFile, Paths.get(currentFile.toString() + PROCESSED_FILE_SUFFIX));
	}

	/* (non-Javadoc)
	 * @see com.nagarro.nteg.utils.FileDataBufferedReader#getFileName()
	 */
	@Override
	public String getFileName() {
		return currentFile.toAbsolutePath().toString();
	}
}
