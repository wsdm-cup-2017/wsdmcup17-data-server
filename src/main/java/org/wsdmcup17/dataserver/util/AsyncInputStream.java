package org.wsdmcup17.dataserver.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An input stream that splits the processing of the stream into two threads.
 */
public class AsyncInputStream extends PipedInputStream {
	
	private static final Logger
		LOG = LoggerFactory.getLogger(AsyncInputStream.class);
	
	private Thread thread;
	
	public AsyncInputStream(
			final ThreadGroup threadGroup, String threadName,
			InputStream inputStream, int bufferSize
	) throws IOException {		
		super(bufferSize);
		final PipedOutputStream pipedOutputStream = new PipedOutputStream();
		this.connect(pipedOutputStream);
		thread = new Thread(threadGroup, threadName) {
			@Override
			public void run() {
				try {					
					IOUtils.copy(inputStream, pipedOutputStream);
					inputStream.close();
					pipedOutputStream.close();					
				}
				catch (Throwable e) {
					LOG.error("", e);
					throw new RuntimeException(e);
				}
			}
		};
		thread.start();
	}
	
	public AsyncInputStream(
		final String threadName, InputStream inputStream, int bufferSize
	) throws IOException {		
		this(null, threadName, inputStream, bufferSize);
	}

	@Override
	public void close() throws IOException {
		super.close();
		try {
			thread.join();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOG.error("", e);
			throw new RuntimeException(e);
		}
	}
}
