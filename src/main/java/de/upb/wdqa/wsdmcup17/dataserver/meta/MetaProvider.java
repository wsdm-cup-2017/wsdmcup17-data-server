package de.upb.wdqa.wsdmcup17.dataserver.meta;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import de.upb.wdqa.wsdmcup17.dataserver.util.BinaryItem;
import de.upb.wdqa.wsdmcup17.dataserver.util.FilterProcessor;
import de.upb.wdqa.wsdmcup17.dataserver.util.ItemProcessor;
import de.upb.wdqa.wsdmcup17.dataserver.util.QueueProcessor;
import de.upb.wdqa.wsdmcup17.dataserver.util.SevenZInputStream;

/**
 * Thread reading meta data from a file, parsing the data and putting it into a queue.
 *
 */
public class MetaProvider implements Runnable {
	
	Logger logger = Logger.getLogger(MetaProvider.class);

	MetaParser parser;
	
	public MetaProvider(BlockingQueue<BinaryItem> queue, File file, long firstRevision){
		ItemProcessor nextProcessor;
		nextProcessor = new QueueProcessor(queue);
		nextProcessor = new FilterProcessor(nextProcessor, firstRevision);
		
		InputStream inputStream;
		try {
			inputStream = new SevenZInputStream(file);
			parser = new MetaParser(nextProcessor, inputStream);
		} catch (IOException e) {
			logger.error("", e);
		}
	}	

	public void run() {
		parser.consumeFile();		
	}
	
	public void stop(){
		parser.stop();
	}
	
}
