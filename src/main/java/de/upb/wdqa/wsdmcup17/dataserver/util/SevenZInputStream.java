package de.upb.wdqa.wsdmcup17.dataserver.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.compress.archivers.sevenz.SevenZFile;

/**
 * A <code>SevenZInputStream</code> obtains input bytes from a compressed 7z file in a file system. 
 */
public class SevenZInputStream extends InputStream{
	
	SevenZFile sevenZFile;
	
	public SevenZInputStream(File file) throws IOException{
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
		if (sevenZFile.getNextEntry() != null){
			sevenZFile.close();
			throw new IOException("7z file contains more than one stream!");
		}
		
		sevenZFile.close();
	}
}
