package de.upb.wdqa.wsdmcup17.dataserver.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An input stream that splits the processing of the stream into two threads.
 *
 */
public class AsyncInputStream extends PipedInputStream {
	
	static final Logger logger = LoggerFactory.getLogger(AsyncInputStream.class);
	
	Thread thread;
	
	public AsyncInputStream(final InputStream inputStream, String threadName, int bufferSize) throws IOException{		
		super(bufferSize);
		final PipedOutputStream pipedOutputStream = new PipedOutputStream();
		this.connect(pipedOutputStream);
		
		thread = new Thread(threadName){
			@Override
			public void run(){
				try {					
					IOUtils.copy(inputStream, pipedOutputStream);
					
					inputStream.close();
					pipedOutputStream.close();					
				} catch (Throwable e) {
					logger.error("", e);
				}
			}
		};
		thread.start();
	}
	
	@Override
	public void close() throws IOException {
		super.close();
		
		try {
			thread.join();
		} catch (InterruptedException e) {
			logger.error("", e);
		}
	}
}
