package de.upb.wdqa.wsdmcup17.dataserver.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.upb.wdqa.wsdmcup17.dataserver.util.BinaryItem;
import de.upb.wdqa.wsdmcup17.dataserver.util.ItemProcessor;

/**
 * Abstract class for parsing lines of a text file one by one.
 * 
 * A concrete subclass has to implement the {@link #consumeLine(String)} method
 * and call the the {@link #processCurItem()} method whenever the processing of
 * an item has finished (possibly after having consumed several lines}.
 */
public abstract class LineParser {
	final static Logger logger = LoggerFactory.getLogger(LineParser.class);

	// used for writing (e.g., RFC4180 demands the line ending \r\n)
	final static String LINE_BREAK = "\r\n";

	private StringBuilder curItem = new StringBuilder();
	private ItemProcessor processor;
	private InputStream inputStream;

	private volatile boolean isStopping;

	protected long curRevisionId; // must be set by subclass

	protected LineParser(ItemProcessor processor, InputStream inputStream) {
		this.processor = processor;
		this.inputStream = inputStream;
	}

	protected abstract void consumeLine(String line);

	public void consumeFile() {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));) {
			String line;

			while ((line = reader.readLine()) != null && !isStopping) {
				appendLineToCurItem(line);
				consumeLine(line);
			}

			if (!isStopping) {
				logger.debug("processing tail of file...");
				BinaryItem item = new BinaryItem(Long.MAX_VALUE, stringToBytes(curItem.toString()));
				processor.processItem(item);
			}

		} catch (Throwable e) {
			logger.error("", e);
		}
	}

	/**
	 * Must be called by subclass a the end of an item.
	 */
	protected void processCurItem() {
		BinaryItem item = new BinaryItem(curRevisionId, stringToBytes(curItem.toString()));

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
