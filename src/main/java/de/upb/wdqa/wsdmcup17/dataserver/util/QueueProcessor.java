package de.upb.wdqa.wsdmcup17.dataserver.util;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

/**
 * Puts binary items in a queue.
 */
public class QueueProcessor implements ItemProcessor {
	
	Logger logger = Logger.getLogger(QueueProcessor.class);
	
	BlockingQueue<BinaryItem> queue;

	public QueueProcessor(BlockingQueue<BinaryItem> queue) {
		this.queue = queue;
	}

	@Override
	public void processItem(BinaryItem item) {
		try{
			queue.put(item);
		}
		catch (InterruptedException e){
			logger.debug("Thread was interrupted");
		}
		catch(Throwable e){
			logger.error("", e);
		}
	}
}
