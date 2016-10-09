package org.wsdmcup17.dataserver.util;

/**
 * Processes a binary item and forwards the event to the next processor if the
 * filtering criteria are met (the given <code>firstRevisionId</code> has been
 * read).
 */
public class FilterProcessor implements ItemProcessor {

	private ItemProcessor processor;
	private long firstRevisionId;
	private boolean firstRevisionIdRead = false;

	public FilterProcessor(ItemProcessor processor, long firstRevisionId) {
		this.processor = processor;
		this.firstRevisionId = firstRevisionId;
	}

	@Override
	public void processItem(BinaryItem item) {
		if (item.getRevisionId() == firstRevisionId) {
			firstRevisionIdRead = true;
		}
		if (firstRevisionIdRead) {
			processor.processItem(item);
		}
	}
}
