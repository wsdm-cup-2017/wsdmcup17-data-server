package org.wsdmcup17.dataserver.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * InputStream capable of reading lines without reading ahead, thus avoiding
 * blocking.
 */
public class NonBlockingLineBufferedInputStream extends InputStream {
	
	private static final String
		ERROR_MSG_LINE_TOO_LONG_FOR_BUFFER = "Line too long for buffer",
		ERROR_MSG_INVALID_LINE_ENDING = "Invalid Line Ending ";

	private InputStream inputStream;
	
	byte[] lineBuffer;
	
	public NonBlockingLineBufferedInputStream(
		InputStream inputStream, int bufferSize
	) {
		this.inputStream = inputStream;
		this.lineBuffer = new byte[bufferSize];
	}	
	
	@Override
	public int read() throws IOException {
		return inputStream.read();
	}
	
	@Override
	public int read(byte[] b) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long skip(long n) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int available() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		inputStream.close();
	}

	@Override
	public synchronized void mark(int readlimit) {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized void reset() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean markSupported() {
		return false;
	}
	
	public String readLine() throws IOException {
		int curPos = 0;
		
		while(true) {
			int b = this.read();
			if (b == (byte)'\r') {
				b = this.read();
				if (b != (byte)'\n') {
					throw new IOException(ERROR_MSG_INVALID_LINE_ENDING + b);
				}
				break;
			}
			else if (b == -1) {				
				return null;
			}
			lineBuffer[curPos] = (byte)b;
			curPos++;
			if (curPos >= lineBuffer.length) {
				throw new IOException(ERROR_MSG_LINE_TOO_LONG_FOR_BUFFER);
			}
		}
		
		return new String(lineBuffer, 0, curPos, StandardCharsets.UTF_8);		
	}
}
