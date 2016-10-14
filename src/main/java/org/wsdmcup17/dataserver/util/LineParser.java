package org.wsdmcup17.dataserver.util;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wsdmcup17.dataserver.util.BinaryItem;
import org.wsdmcup17.dataserver.util.ItemProcessor;

/**
 * Abstract class for parsing lines of a text file one by one.
 *
 * A concrete subclass has to implement the {@link #consumeLine(String)} method
 * and call the the {@link #processCurItem()} method whenever the processing of
 * an item has finished (possibly after having consumed several lines}.
 */
public abstract class LineParser {

	private static final Logger LOG = LoggerFactory.getLogger(LineParser.class);

	private static final String
		LOG_MSG_END_OF_FILE = "end of file reached",
		CRLF = "\r\n", // RFC4180 demands the line ending \r\n.
		UTF_8 = "UTF-8";

	private StringBuilder curString = new StringBuilder();
	private ItemProcessor processor;
	private InputStream inputStream;
	
	private BinaryItem lastItem = null;

	private volatile boolean isStopping;

	protected long curRevisionId; // Must be set by subclass.

	protected LineParser(ItemProcessor processor, InputStream inputStream) {
		this.processor = processor;
		this.inputStream = inputStream;
	}

	public void consumeFile() {
		try (
			Reader reader = new InputStreamReader(inputStream, UTF_8);
			BufferedReader bufferedReader = new BufferedReader(reader);
		) {
			String line;
			while ((line = bufferedReader.readLine()) != null &&
					!isStopping && !Thread.currentThread().isInterrupted()) {
				appendLineToCurItem(line);
				consumeLine(line);
			}
			if (line == null) { // end of file reached
				consumeLine(line);
				
				LOG.debug(LOG_MSG_END_OF_FILE);
				
				// send sentinel to indicate end of item stream
				curRevisionId = Long.MAX_VALUE;
				curString = new StringBuilder();
				endItem();
				processLastItem();
			}
		}
		catch (Throwable e) {
			LOG.error("", e);
		}
	}
	
	protected abstract void consumeLine(String line);

	/**
	 * Must be called by subclass at the end of an item.
	 */
	protected void processLastItem() {
		if (lastItem != null){
			processor.processItem(lastItem);
			lastItem = null;
		}
	}
	
	protected void endItem() {
		byte[] bytes = stringToBytes(curString.toString());
		lastItem = new BinaryItem(curRevisionId, bytes);
		resetBuffers();
	}
	
	protected void appendToItem() {
		try {
			byte[] bytes = stringToBytes(curString.toString());
		
			// append bytes
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
			outputStream.write(lastItem.getBytes());
			outputStream.write(bytes);			
			bytes = outputStream.toByteArray();
			
			lastItem = new BinaryItem(lastItem.getRevisionId(), bytes);
			resetBuffers();
		} catch (IOException e) {
			LOG.error("", e);
		}
	}

	private void appendLineToCurItem(String line) {
		curString.append(line);
		curString.append(CRLF);
	}

	private void resetBuffers() {
		curRevisionId = -1;
		curString = new StringBuilder();
	}

	private byte[] stringToBytes(String str) {
		return curString.toString().getBytes(StandardCharsets.UTF_8);
	}

	public void stop() {
		isStopping = true;
	}
}
