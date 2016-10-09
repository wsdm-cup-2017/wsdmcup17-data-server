package org.wsdmcup17.dataserver.result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wsdmcup17.dataserver.util.SynchronizedBoundedBlockingMapQueue;

/**
 * Thread parsing and validating scoring results.
 */
public class ResultRecorder implements Runnable {	
	
	private static final String
		ERROR_MSG_MISSING_REVISION_SCORES = "Missing revision scores: %s",
		ERROR_MSG_DUPLICATE_REVISION = "Duplicate revision: %d",
		ERROR_MSG_UNEXPECTED_REVISION = "Unexpected revision: %d",
		LOG_MSG_WRITING_REVISION_RESULT_AT_QUEUE_SIZE =
			"Writing revision result %s (queue size %d)";

	private static final Logger
		LOG = LoggerFactory.getLogger(ResultRecorder.class);
	
	private static final int
		DELAY = 10000;
	
	private ResultParser resultParser;
	private ResultPrinter resultPrinter;
	private SynchronizedBoundedBlockingMapQueue<Long, Result> mapQueue;
	private long lastMillis = 0;
	
	public ResultRecorder(
		SynchronizedBoundedBlockingMapQueue<Long, Result> mapQueue,
		ResultParser resultParser, ResultPrinter resultPrinter
	) {
		this.mapQueue = mapQueue;
		this.resultParser = resultParser;
		this.resultPrinter = resultPrinter;
	}
	
	public void run() {
		try {
			while (true) {
				Result parsedResult = resultParser.parseResult();
				if (parsedResult != null) {
					consumeResult(parsedResult);
				}
				else {
					if (mapQueue.size() > 0) {
						String e = createErrorMsg();
						throw new IllegalStateException(e);
					}
					// The last result has been read and written.	
					break; 			
				}
			}
		}
		catch (Throwable e) {
			LOG.error("", e);
		}
	}

	private String createErrorMsg() {
		List<Long> missing = new ArrayList<Long>();
		while (mapQueue.size() > 0) {
			missing.add(mapQueue.peek().getRevisionId());
			mapQueue.remove();
		}
		return String.format(ERROR_MSG_MISSING_REVISION_SCORES, missing);
	}

	private void consumeResult(Result parsedResult)
	throws InterruptedException {
		long revisionId = parsedResult.getRevisionId();
		Result queueResult = mapQueue.get(revisionId);
		if (queueResult == null) {
			String e = String.format(ERROR_MSG_UNEXPECTED_REVISION, revisionId);
			throw new IllegalStateException(e);
		}
		if (queueResult.getScore() != null) {
			String e = String.format(ERROR_MSG_DUPLICATE_REVISION, revisionId);
			throw new IllegalStateException(e);
		}					
		queueResult.setScore(parsedResult.getScore());
		try {
			writeResults(mapQueue, resultPrinter);
		}
		catch (IOException e) {
			LOG.error("", e);
		}
	}
	
	private void writeResults(
		SynchronizedBoundedBlockingMapQueue<Long, Result> mapQueue,
		ResultPrinter resultPrinter
	) throws IOException {
		while(true){
			Result result = mapQueue.peek();
			if (result != null && result.getScore() != null) {
				if (System.currentTimeMillis() - lastMillis > DELAY){
					LOG.debug(String.format(
						LOG_MSG_WRITING_REVISION_RESULT_AT_QUEUE_SIZE,
						result.getRevisionId(), mapQueue.size()));
					lastMillis = System.currentTimeMillis();
				}
				resultPrinter.printResult(result);
				mapQueue.remove();
			}
			else {
				break;
			}			
		}
	}
}

