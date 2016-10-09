package org.wsdmcup17.dataserver.util;

import java.io.BufferedReader;
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
		LOG_MSG_PROCESSING_TAIL_OF_FILE = "processing tail of file...",
		LINE_BREAK = "\r\n", // RFC4180 demands the line ending \r\n.
		UTF_8 = "UTF-8";

	private StringBuilder curItem = new StringBuilder();
	private ItemProcessor processor;
	private InputStream inputStream;

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
			if (!isStopping && !Thread.currentThread().isInterrupted()) {
				LOG.debug(LOG_MSG_PROCESSING_TAIL_OF_FILE);
				byte[] bytes = stringToBytes(curItem.toString());
				BinaryItem item = new BinaryItem(Long.MAX_VALUE, bytes);
				processor.processItem(item);
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
	protected void processCurItem() {
		byte[] bytes = stringToBytes(curItem.toString());
		BinaryItem item = new BinaryItem(curRevisionId, bytes);
		processor.processItem(item);
		resetBuffers();
	}

	private void appendLineToCurItem(String line) {
		curItem.append(line);
		curItem.append(LINE_BREAK);
	}

	private void resetBuffers() {
		curRevisionId = -1;
		curItem = new StringBuilder();
	}

	private byte[] stringToBytes(String str) {
		return curItem.toString().getBytes(StandardCharsets.UTF_8);
	}

	public void stop() {
		isStopping = true;
	}
}
