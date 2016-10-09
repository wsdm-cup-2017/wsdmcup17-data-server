package org.wsdmcup17.dataserver.util;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

/**
 * Puts binary items in a queue.
 */
public class QueueProcessor implements ItemProcessor {
	
	private static final Logger LOG = Logger.getLogger(QueueProcessor.class);
	
	private static final String
		LOG_MSG_THREAD_INTERRUPTED = "Thread interrupted";
	
	private BlockingQueue<BinaryItem> queue;

	public QueueProcessor(BlockingQueue<BinaryItem> queue) {
		this.queue = queue;
	}

	@Override
	public void processItem(BinaryItem item) {
		try{
			queue.put(item);
		}
		catch (InterruptedException e){
			// Reset the interrupt flag.
			Thread.currentThread().interrupt();
			LOG.debug(LOG_MSG_THREAD_INTERRUPTED);
		}
		catch(Throwable e){
			LOG.error("", e);
		}
	}
}
