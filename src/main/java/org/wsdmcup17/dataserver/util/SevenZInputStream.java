package org.wsdmcup17.dataserver.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.compress.archivers.sevenz.SevenZFile;

/**
 * A <code>SevenZInputStream</code> obtains input bytes from a compressed 7z
 * file in a file system.
 */
public class SevenZInputStream extends InputStream{
	
	private static final String
		ERROR_MSG_MULTIPLE_7Z_STREAMS = "Multiple 7z streams.";
	
	private SevenZFile sevenZFile;
	
	public SevenZInputStream(File file) throws IOException {
		sevenZFile = new SevenZFile(file);
		sevenZFile.getNextEntry();
	}

	@Override
	public int read() throws IOException {
		int result = sevenZFile.read();
		return result;		
	}
	
	@Override
	public int read(byte[] b) throws IOException {
		int numberOfBytesRead = sevenZFile.read(b);
		return numberOfBytesRead;
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int numberOfBytesRead = sevenZFile.read(b, off, len);
		return numberOfBytesRead;
	}	

	@Override
	public void close() throws IOException {
		if (sevenZFile.getNextEntry() != null) {
			sevenZFile.close();
			throw new IOException(ERROR_MSG_MULTIPLE_7Z_STREAMS);
		}
		sevenZFile.close();
	}
}
