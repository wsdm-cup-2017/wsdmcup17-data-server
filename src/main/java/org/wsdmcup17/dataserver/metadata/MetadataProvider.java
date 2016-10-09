package org.wsdmcup17.dataserver.metadata;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.wsdmcup17.dataserver.util.BinaryItem;
import org.wsdmcup17.dataserver.util.FilterProcessor;
import org.wsdmcup17.dataserver.util.ItemProcessor;
import org.wsdmcup17.dataserver.util.QueueProcessor;
import org.wsdmcup17.dataserver.util.SevenZInputStream;

/**
 * Thread reading meta data from a file, parsing the data and putting it into a
 * queue.
 */
public class MetadataProvider implements Runnable {
	
	private static final Logger
		LOG = Logger.getLogger(MetadataProvider.class);

	private BlockingQueue<BinaryItem> queue;
	private File file;
	private long firstRevision;
	private MetadataParser parser;
	
	public MetadataProvider(BlockingQueue<BinaryItem> queue, File file,
			long firstRevision) {
		this.queue = queue;
		this.file = file;
		this.firstRevision = firstRevision;
	}	

	@Override
	public void run() {
		ItemProcessor nextProcessor = new QueueProcessor(queue);
		nextProcessor = new FilterProcessor(nextProcessor, firstRevision);
		try (InputStream inputStream = new SevenZInputStream(file)) {
			parser = new MetadataParser(nextProcessor, inputStream);
			parser.consumeFile();
		}
		catch (IOException e) {
			LOG.error("", e);
		}
	}
	
	public void stop(){
		parser.stop();
	}
}
