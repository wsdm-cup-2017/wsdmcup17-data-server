package org.wsdmcup17.dataserver.revision;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.wsdmcup17.dataserver.util.AsyncInputStream;
import org.wsdmcup17.dataserver.util.BinaryItem;
import org.wsdmcup17.dataserver.util.QueueProcessor;
import org.wsdmcup17.dataserver.util.SevenZInputStream;

/**
 * Thread reading revisions from a file, parsing them and putting them into a
 * queue.
 */
public class RevisionProvider implements Runnable {

	private static final String REVISION_DECOMPRESSOR =
			"%s: Revision Decompressor";
	private static final int BUFFER_SIZE = 512 * 1024 * 1024;

	private static final Logger LOG = Logger.getLogger(RevisionProvider.class);
	
	private ThreadGroup threadGroup;
	private BlockingQueue<BinaryItem> queue;
	private File file;
	private RevisionParser parser;
	
	public RevisionProvider(
		ThreadGroup threadGroup, BlockingQueue<BinaryItem> queue, File file) {
		
		this.threadGroup = threadGroup;
		this.queue = queue;
		this.file = file;
	}

	public void run() {
		try (
			InputStream	sevenZInput = new SevenZInputStream(file);
			InputStream asyncInput = new AsyncInputStream(
				threadGroup,
				String.format(REVISION_DECOMPRESSOR, threadGroup.getName()),
				sevenZInput,
				BUFFER_SIZE);
		){
			parser = new RevisionParser(new QueueProcessor(queue), asyncInput);
			parser.consumeFile();
		} catch (IOException e) {
			LOG.error("", e);
		}
	}
}