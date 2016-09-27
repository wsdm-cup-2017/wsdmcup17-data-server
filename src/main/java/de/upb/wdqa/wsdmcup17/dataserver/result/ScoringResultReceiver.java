package de.upb.wdqa.wsdmcup17.dataserver.result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.upb.wdqa.wsdmcup17.dataserver.util.SynchronizedBoundedBlockingMapQueue;

/**
 * Thread parsing and validating scoring results.
 */
public class ScoringResultReceiver implements Runnable {	
	
	final static Logger logger = LoggerFactory.getLogger(ScoringResultReceiver.class);
	
	ScoringResultParser scoringResultParser;
	ScoringResultPrinter scoringResultPrinter;
	SynchronizedBoundedBlockingMapQueue<Long, ScoringResult> mapqueue;
	
	static long lastMillis = 0;
	
	public ScoringResultReceiver(SynchronizedBoundedBlockingMapQueue<Long, ScoringResult> mapqueue, ScoringResultParser scoringResultParser, ScoringResultPrinter scoringResultPrinter){
		this.mapqueue = mapqueue;
		this.scoringResultParser = scoringResultParser;
		this.scoringResultPrinter = scoringResultPrinter;
	}
	
	
	public void run() {
		try{
			while(true){
				ScoringResult parsedResult = scoringResultParser.parseResult();
				if (parsedResult != null){
					ScoringResult queueResult = mapqueue.get(parsedResult.getRevisionId());
					if (queueResult == null){
						throw new IllegalStateException("Unexpected result for revision " + parsedResult.getRevisionId());
					}
					
					if (queueResult.getScore() != null){
						throw new IllegalStateException("Duplicate result for revision " + parsedResult.getRevisionId());
					}					
					
					queueResult.setScore(parsedResult.getScore());
					
					try {
						writeResults(mapqueue, scoringResultPrinter);
					} catch (IOException e) {
						logger.error("", e);
					}
				}
				else{
					if (mapqueue.size() > 0){
						List<Long> missingScores = new ArrayList<Long>();
						while(mapqueue.size() > 0){
							missingScores.add(mapqueue.peek().getRevisionId());
							mapqueue.remove();
						}
						
						throw new IllegalStateException("For the following revisions a score is missing: " + missingScores);
					}
					
					// the last result has been read and written	
					break; 			
				}
			}
		}catch(Throwable e){
			logger.error("", e);
		}
	}
	
    private static void writeResults(SynchronizedBoundedBlockingMapQueue<Long, ScoringResult> mapqueue, ScoringResultPrinter resultPrinter) throws IOException{
    	while(true){
	    	ScoringResult scoringResult = mapqueue.peek();
	    	
	    	if (scoringResult != null && scoringResult.getScore() != null){
	    		if (System.currentTimeMillis() - lastMillis > 10000){
	    			logger.debug("Writing result for revision " + scoringResult.getRevisionId() + " (queue size " + mapqueue.size() +")");
	    			lastMillis = System.currentTimeMillis();
	    		}
	    		resultPrinter.printResult(scoringResult);
	    		mapqueue.remove();
	    	}
	    	else{
	    		break;
	    	}	    	
    	}
    }
}