/**
 * 
 */
package com.nagarro.nteg.utils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Scanner;

/**
 * @author amitkumar02
 *
 */
public class FixedLinesBatchFileReader {
	
	private final int batchSize;
	private final String fileName;
	private final Scanner scanner;
	private final Deque<String> buffer;
	
	public FixedLinesBatchFileReader(final String filePath, final int batchSize) throws IOException {
		this(Paths.get(filePath), batchSize);
	}
	
	public FixedLinesBatchFileReader(final Path filePath, final int batchSize) throws IOException {
		this.batchSize = batchSize;
		buffer = new ArrayDeque<String>(batchSize);
		scanner = new Scanner(filePath);
		this.fileName = filePath.toString();
	}
	
	protected void fillNextBatch() {
		for(int i= 0; i < batchSize; i++) {
			if(scanner.hasNextLine()) {
				buffer.addFirst(scanner.nextLine());
			}
		}
	}

	public void open() throws IOException {
		
		fillNextBatch();
	}
	
	public String nextLine() {
		
		if(buffer.isEmpty()) {
			fillNextBatch();
		}
		
		return buffer.pollLast();
	}
	
	public void close() {
		if(scanner != null) {
			scanner.close();
		}
	}

	public String getFileName() {
		return fileName;
	}
	
	
}
