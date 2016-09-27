package de.upb.wdqa.wsdmcup17.dataserver;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.log4j.Appender;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.upb.wdqa.wsdmcup17.dataserver.meta.MetaProvider;
import de.upb.wdqa.wsdmcup17.dataserver.result.ScoringResult;
import de.upb.wdqa.wsdmcup17.dataserver.result.ScoringResultParser;
import de.upb.wdqa.wsdmcup17.dataserver.result.ScoringResultPrinter;
import de.upb.wdqa.wsdmcup17.dataserver.result.ScoringResultReceiver;
import de.upb.wdqa.wsdmcup17.dataserver.revision.RevisionProvider;
import de.upb.wdqa.wsdmcup17.dataserver.util.BinaryItem;
import de.upb.wdqa.wsdmcup17.dataserver.util.NonBlockingLineBufferedInputStream;
import de.upb.wdqa.wsdmcup17.dataserver.util.SynchronizedBoundedBlockingMapQueue;

public class WSDMCup17DataServer 
{
	 final static Logger logger = LoggerFactory.getLogger(WSDMCup17DataServer.class);
	 
	 final static String timeStamp = new SimpleDateFormat("yyyyMMdd__HHmmss").format(new Date());
	 
	 final static int BACKPRESSURE_WINDOW_K = 16;
	 
	 public static final String[] RESULT_CSV_HEADER = {"REVISION_ID", "VANDALISM_SCORE"};
	 
	 final static int REVISIONS_TO_BUFFER = 128;
	 
	 static BlockingQueue<BinaryItem> revisionQueue = new ArrayBlockingQueue<BinaryItem>(REVISIONS_TO_BUFFER);
	 static BlockingQueue<BinaryItem> metaQueue = new ArrayBlockingQueue<BinaryItem>(REVISIONS_TO_BUFFER);
	 
	 static long lastLoggingMillis = 0;
	 
	 String authenticationToken;

	
    public static void main( String[] args ) throws IOException, InterruptedException
    {
    	WSDMCup17ServerConfiguration config = new WSDMCup17ServerConfiguration(args);
    	
    	initLogger(new File(config.getOutputPath(), InetAddress.getLocalHost().getHostName() + "_" + config.getPort() + ".log"));
    	
	    logger.info("Listening for connections on port " + config.getPort() + "...");
    	try(
    		ServerSocket dataServerSocket = new ServerSocket(config.getPort());    			
	    	Socket dataSocket = dataServerSocket.accept();
    			
    		// InputStream
    		InputStream resultStream = dataSocket.getInputStream();    			
    		NonBlockingLineBufferedInputStream bufferedResultStream = new NonBlockingLineBufferedInputStream(resultStream, 10000);
   		){  
    		String authToken = bufferedResultStream.readLine();
    		
    		logger.info("Connection established to " + dataSocket.getInetAddress());
    		
    		File outputFile = new File(config.getOutputPath(), authToken + ".csv");
    		if (!outputFile.getParentFile().equals(config.getOutputPath())){
    			throw new Exception("auth token is invalid");
    		}
    		try(
	    		OutputStream fileOutputStream = new FileOutputStream(outputFile);
	    		OutputStream bufferedFileOutputStream = new BufferedOutputStream(fileOutputStream);
	    		Writer writer = new OutputStreamWriter(bufferedFileOutputStream, "UTF-8");
	    		CSVPrinter csvPrinter = new CSVPrinter(writer,CSVFormat.RFC4180.withHeader(RESULT_CSV_HEADER));
	    		){
    		
    		
	    		SynchronizedBoundedBlockingMapQueue<Long, ScoringResult> mapqueue = new SynchronizedBoundedBlockingMapQueue<>(BACKPRESSURE_WINDOW_K);
		  		
	    		RevisionProvider revisionProvider = new RevisionProvider(revisionQueue, config.getRevisionFile());
	    		Thread revisionThread = new Thread(revisionProvider, "Revision Provider");
	    		revisionThread.start();
	    		
	    		BinaryItem firstRevision = null;
	    		while (firstRevision == null){
	    			firstRevision = revisionQueue.peek();
	    			Thread.sleep(100);
	    		}
	    		
	    		MetaProvider metaProvider = new MetaProvider(metaQueue, config.getMetaFile(), firstRevision.getRevisionId());
	    		Thread metaThread = new Thread(metaProvider, "Meta Provider");
	    		metaThread.start();
	    		
	    		ScoringResultReceiver resultReceiver = new ScoringResultReceiver(mapqueue, new ScoringResultParser(bufferedResultStream), new ScoringResultPrinter(csvPrinter));
	    		Thread  resultReceiverThread = new Thread(resultReceiver, "Receiver");
	    		resultReceiverThread.start();
	    		
	    		try(
	    			// Closing the outputstream would result in closing the socket.
	    		    // However, we have to prevent this because we still want to receive data from the socket.
		    		OutputStream dataStreamInternal = new CloseShieldOutputStream(dataSocket.getOutputStream());
		    		DataOutputStream dataStream = new DataOutputStream(dataStreamInternal);
		        	){    		
	    		
		    		logger.info("Connected to " + dataSocket.getInetAddress());
		    		
		    		Thread.currentThread().setName("Sender");
		    		
					sendData(mapqueue, revisionQueue,metaQueue, dataStream,
							resultStream);				
	    		}
				
	    		// there are no more revisions in the revision file
				revisionThread.join();
				
				// stop reading the meta file when there are no revisions any more in the revision file
				metaProvider.stop();
				metaThread.interrupt();
				metaThread.join();
				
				// signal to the client that there will not be any more revisions
				dataSocket.shutdownOutput();
				
				// waint until all results have been received from the client
				resultReceiverThread.join();
	    	}
    	}
    	catch (Throwable e){
    		logger.error("", e);
    	}
    	finally{    	
    		closeLogger();
    	}
	}    
    
    private static void sendData(SynchronizedBoundedBlockingMapQueue<Long, ScoringResult> mapqueue,
    		BlockingQueue<BinaryItem> revisionQueue, BlockingQueue<BinaryItem> metaQueue,
    		DataOutputStream dataStream, InputStream resultStream) {
		try{			
			while(true){
				BinaryItem revision = revisionQueue.take();
				BinaryItem metaItem = metaQueue.take();
			    
			    if (revision.getRevisionId() == Long.MAX_VALUE){
			    	logger.debug("Sending Tail of XML document");
			    	
			    	sendItem(metaItem, dataStream);
			    	sendItem(revision, dataStream);			    	

			    	break;
			    }
			    else{
			    	if (System.currentTimeMillis() - lastLoggingMillis > 10000){
			    		logger.debug("Sending revision " + revision.getRevisionId() + " (queue size " + mapqueue.size() + ")");
			    		lastLoggingMillis = System.currentTimeMillis();
			    	}
				    mapqueue.put(revision.getRevisionId(), new ScoringResult(revision.getRevisionId(), null));
				    
				    sendItem(metaItem, dataStream);
				    sendItem(revision, dataStream);
			    }			
			}

		}
		catch(Throwable e){
			logger.error("", e);
		}
	}
    
    private static void sendItem(BinaryItem item, DataOutputStream revisionStream) throws IOException{
    	revisionStream.writeInt(item.getBytes().length);
    	revisionStream.write(item.getBytes(), 0, item.getBytes().length);
    }    


	public static void initLogger(File file){
		final String PATTERN = "[%d{yyyy-MM-dd HH:mm:ss}] [%-5p] [%t] [%c{1}] %m%n";
		
		org.apache.log4j.Logger logger = org.apache.log4j.Logger.getRootLogger();
		
		ConsoleAppender consoleAppender = new ConsoleAppender(); 
		consoleAppender.setEncoding("UTF-8");
		consoleAppender.setLayout(new PatternLayout(PATTERN)); 
		consoleAppender.setThreshold(Level.ALL);
		consoleAppender.activateOptions();		
		AsyncAppender asyncConsoleAppender = new AsyncAppender();
		asyncConsoleAppender.addAppender(consoleAppender);
		asyncConsoleAppender.setBufferSize(1024);
		asyncConsoleAppender.activateOptions();
		logger.addAppender(asyncConsoleAppender);
		
		FileAppender fileAppender = new FileAppender();
		fileAppender.setEncoding("UTF-8");
		fileAppender.setFile(file.getAbsolutePath());
		fileAppender.setLayout(new PatternLayout(PATTERN));
		fileAppender.setThreshold(Level.ALL);
		fileAppender.setAppend(false);
		fileAppender.activateOptions();		
		AsyncAppender asyncFileAppender = new AsyncAppender();
		asyncFileAppender.addAppender(fileAppender);
		asyncFileAppender.setBufferSize(1024);
		asyncFileAppender.activateOptions();
		logger.addAppender(asyncFileAppender);

    }
	
	private static void closeLogger(){
		org.apache.log4j.LogManager.shutdown();	
		Enumeration<?> e = org.apache.log4j.Logger.getRootLogger().getAllAppenders();
		while ( e.hasMoreElements() ){
			Appender appender = (Appender)e.nextElement();
			appender.close();
		}
	}
}


