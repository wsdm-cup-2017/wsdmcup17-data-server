package de.upb.wdqa.wsdmcup17.dataserver.revision;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import de.upb.wdqa.wsdmcup17.dataserver.util.AsyncInputStream;
import de.upb.wdqa.wsdmcup17.dataserver.util.BinaryItem;
import de.upb.wdqa.wsdmcup17.dataserver.util.QueueProcessor;
import de.upb.wdqa.wsdmcup17.dataserver.util.SevenZInputStream;

/**
 * Thread reading revisions from a file, parsing them and putting them into a queue.
 *
 */
public class RevisionProvider implements Runnable{
	
	Logger logger = Logger.getLogger(RevisionProvider.class);
	
	RevisionParser parser;
	
	public RevisionProvider(BlockingQueue<BinaryItem> queue, File file){
		
		InputStream inputStream;
		try {
			inputStream = new SevenZInputStream(file);
			inputStream = new AsyncInputStream(inputStream, "Revision Decompressor", 512 * 1024 * 1024);

			parser = new RevisionParser(new QueueProcessor(queue), inputStream);
		} catch (IOException e) {
			logger.error("", e);
		}
	}	

	public void run() {
		parser.consumeFile();		
	}
	
}