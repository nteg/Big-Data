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
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Scanner;

import org.apache.log4j.Logger;

/**
 * @author Nagarro Softwares Pvt. Ltd.
 *
 */
public class LocalDirectoryFilesDataReader implements DirectoryFilesDataReader{
	
	private static final Logger LOG = Logger.getLogger(LocalDirectoryFilesDataReader.class);
	
	private final int batchSize;
	private final String dirPathName;
	private final Deque<String> buffer;
	
	private BatchDataReader batchDataReader;
	
	public LocalDirectoryFilesDataReader(final File dirPath, final int batchSize) throws IOException {
		this(dirPath.toPath(), batchSize);
	}
	
	public LocalDirectoryFilesDataReader(final String dirPathName, final int batchSize) throws IOException {
		this(Paths.get(dirPathName), batchSize);
	}
	
	public LocalDirectoryFilesDataReader(final Path dirPath, final int batchSize) throws IOException {
		this.batchSize = batchSize;
		buffer = new ArrayDeque<String>(batchSize);
		this.dirPathName = dirPath.toString();
		
		if(!Files.isDirectory(dirPath)) {
			throw new IllegalArgumentException(dirPath + " is not a directory or program doesn't have sufficient permissions to access it");
		}
		
		findFileToProcess();
	}
	
	private boolean findFileToProcess() throws IOException {
		
		if(batchDataReader != null) {
			batchDataReader.finalize();
		}
		
		final File dir = new File(dirPathName);
		final File[] filePaths =  dir.listFiles(new FilenameFilter() {
			
			public boolean accept(File dir, String name) {
				
				LOG.debug("Checking file with name[Log]: " + name);
				System.out.println("Checking file with name: " + name);
				
				return !(name.endsWith(BatchDataReader.IN_PROGRESS_FILE_SUFFIX) || name.endsWith(BatchDataReader.PROCESSED_FILE_SUFFIX));
			}
		});
		
		if(filePaths.length <= 0) {
			batchDataReader = null;
			return false;
		}
		
		batchDataReader = new BatchDataReader(filePaths[0].toPath());
		return true;
	}

	public String nextLine() {
		
		while(batchDataReader == null || batchDataReader.isEndOfFile()) {
			
			try {
				boolean fileFound = findFileToProcess();
				
				if(!fileFound) {
					Thread.sleep(10000);
				}
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return batchDataReader.nextLine();
	}
	
	public String getCurrentDataFileName() {
		return batchDataReader.currentFile.toAbsolutePath().toString();
	}
	
	private class BatchDataReader {
		
		private static final String IN_PROGRESS_FILE_SUFFIX = ".in-progress";
		private static final String PROCESSED_FILE_SUFFIX = ".processed";
		
		private final Scanner scanner;
		private final Path currentFile;
		private final Path internalRenamedFile;
		
		private BatchDataReader(final Path file) throws IOException {
			
			this.currentFile = file;
			
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
		
		public boolean isEndOfFile() {
			
			fillNextBatchIfBufferEmpty();
			return (buffer.peekLast() == null);
		}
		
		public String nextLine() {

			fillNextBatchIfBufferEmpty();
			return buffer.pollLast();
		}
		
		public void finalize() throws IOException {
			if(scanner != null) {
				scanner.close();
			}
			
			Files.move(internalRenamedFile, Paths.get(currentFile.toString() + PROCESSED_FILE_SUFFIX));
		}
	}
}
