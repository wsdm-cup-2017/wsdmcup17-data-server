package org.wsdmcup17.dataserver;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wsdmcup17.dataserver.metadata.MetadataProvider;
import org.wsdmcup17.dataserver.result.Result;
import org.wsdmcup17.dataserver.result.ResultParser;
import org.wsdmcup17.dataserver.result.ResultPrinter;
import org.wsdmcup17.dataserver.result.ResultRecorder;
import org.wsdmcup17.dataserver.revision.RevisionProvider;
import org.wsdmcup17.dataserver.util.BinaryItem;
import org.wsdmcup17.dataserver.util.NonBlockingLineBufferedInputStream;
import org.wsdmcup17.dataserver.util.SynchronizedBoundedBlockingMapQueue;

public class RequestHandler implements Runnable {

	private static final Logger
		LOG = LoggerFactory.getLogger(RequestHandler.class);
	
	private static final String
		ACCESS_TOKEN_PATTERN = "^\\d{4}(-\\d\\d){5}$",
		LOG_MSG_SENDING_XML_DOCUMENT_TAIL = "Sending Tail of XML document",
		LOG_MSG_SENDING_REVISION_AT_QUEUE_SIZE =
			"Sending revision %s (queue size %d)",
		LOG_MSG_CONNECTED_TO = "Connected to %s.",
		ERROR_MSG_INVALID_TOKEN = "Invalid access token: %s",
		THREAD_NAME_REVISION_PROVIDER = "Revision Provider",
		THREAD_NAME_METADATA_PROVIDER = "Metadata Provider",
		THREAD_NAME_RESULT_RECORDER = "Result Recorder",
		THREAD_NAME_MULTIPLEXER = "Multiplexer",
		UTF_8 = "UTF-8",
		EXT_CSV = ".csv";
	
	private static final int
		BACKPRESSURE_WINDOW = 16,
		REVISIONS_TO_BUFFER = 128,
		STREAM_BUFFER_SIZE = 10000,
		DELAY = 10000;

	private Configuration config;
	
	private BlockingQueue<BinaryItem>
		revisionQueue = new ArrayBlockingQueue<>(REVISIONS_TO_BUFFER),
		metadataQueue = new ArrayBlockingQueue<>(REVISIONS_TO_BUFFER);
	
	private SynchronizedBoundedBlockingMapQueue<Long, Result>
		mapQueue = new SynchronizedBoundedBlockingMapQueue<>(
			BACKPRESSURE_WINDOW);
	
	private long lastMillis = 0;
	
	private Socket clientSocket;
	
	public RequestHandler(Configuration config, Socket clientSocket) {
		this.config = config;
		this.clientSocket = clientSocket;
	}
	
	@Override
	public void run() {
		try (
			InputStream resultStreamPlain = clientSocket.getInputStream();
			OutputStream dataStreamPlain = clientSocket.getOutputStream();
		) {
			InetAddress clientIP = clientSocket.getInetAddress();
			LOG.info(String.format(LOG_MSG_CONNECTED_TO, clientIP));
			handleRequest(resultStreamPlain, dataStreamPlain);
		}
		catch (IOException | InterruptedException e) {
			LOG.error("", e);
		}
		finally {
			try {
				clientSocket.close();
			}
			catch (IOException e) {
				LOG.error("", e);
			}
		}
	}

	public void handleRequest(
		InputStream resultStreamPlain, OutputStream dataStreamPlain
	) throws InterruptedException, IOException {
		try (
			NonBlockingLineBufferedInputStream resultStream =
				new NonBlockingLineBufferedInputStream(
					resultStreamPlain, STREAM_BUFFER_SIZE);
		){ 
			String accessToken = resultStream.readLine();
			boolean tokenValid = checkTokenValidity(accessToken);
			if (!tokenValid){
				String e = String.format(ERROR_MSG_INVALID_TOKEN, accessToken);
				throw new Error(e);
			}
			File outputFile = getOutputFile(accessToken);
			handleRequest(resultStream, dataStreamPlain, outputFile);
		}
	}

	private boolean checkTokenValidity(String accessToken) throws IOException {
		if (Pattern.matches(ACCESS_TOKEN_PATTERN, accessToken) &&
			!getOutputFile(accessToken).exists() &&
			tiraRunInProgress(accessToken)
		) {
			return true;
		}
		else {
			return false;
		}
	}

	private boolean tiraRunInProgress(String accessToken) throws IOException {
		if (!config.getCheckAccessTokenAgainstTira()) return true;
		File tiraPath = config.getTiraPath();
		File vmStates = new File(tiraPath, "state/virtual-machines");
		File[] stateFiles = vmStates.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".sandboxed");
			}
		});
		for (File stateFile : stateFiles) {
			String contents = FileUtils.readFileToString(stateFile, UTF_8);
			if (contents.contains(accessToken)) {
				return true;
			}
		}
		return false;
	}

	private void handleRequest(
		NonBlockingLineBufferedInputStream resultStream,
		OutputStream dataStreamPlain, File outputFile
	) throws InterruptedException, IOException {
		try(
			OutputStream fileOutputStream = new FileOutputStream(outputFile);
			Writer writer = new OutputStreamWriter(fileOutputStream, UTF_8);
			CSVPrinter csvPrinter =
					new CSVPrinter(writer, ResultParser.CSV_FORMAT);
		) {
			Thread revisionThread = createRevisionProviderThread();
			Thread metadataThread = createMetadataProviderThread();
			Thread resultRecorderThread =
					createResultRecorderThread(resultStream, csvPrinter);
			
			sendData(dataStreamPlain);
			
			revisionThread.join();
			metadataThread.interrupt();
			metadataThread.join();
			clientSocket.shutdownOutput();
			resultRecorderThread.join();
		}
	}

	private Thread createRevisionProviderThread() {
		RevisionProvider revisionProvider =
				new RevisionProvider(revisionQueue, config.getRevisionFile());
		Thread revisionThread =
				new Thread(revisionProvider, THREAD_NAME_REVISION_PROVIDER);
		revisionThread.start();
		return revisionThread;
	}

	private Thread createMetadataProviderThread()
	throws InterruptedException {
		BinaryItem firstRevision = null;
		while (firstRevision == null){
			firstRevision = revisionQueue.peek();
			Thread.sleep(100);
		}
		long revisionId = firstRevision.getRevisionId();
		File metadataFile = config.getMetadataFile();
		MetadataProvider metadataProvider =
				new MetadataProvider(metadataQueue, metadataFile, revisionId);
		Thread metaThread =
				new Thread(metadataProvider, THREAD_NAME_METADATA_PROVIDER);
		metaThread.start();
		return metaThread;
	}
	
	private Thread createResultRecorderThread(
		NonBlockingLineBufferedInputStream resultStream, CSVPrinter csvPrinter
	) {
		ResultParser parser = new ResultParser(resultStream);
		ResultPrinter printer = new ResultPrinter(csvPrinter);
		ResultRecorder resultReceiver =
				new ResultRecorder(mapQueue, parser, printer);
		Thread  resultReceiverThread =
				new Thread(resultReceiver, THREAD_NAME_RESULT_RECORDER);
		resultReceiverThread.start();
		return resultReceiverThread;
	}

	private void sendData(OutputStream dataStreamPlain)
	throws InterruptedException, IOException {
		try(
			// Closing the output stream would result in closing the socket. We
			// prevent this because we still want to receive data from it.
			OutputStream dataStreamShielded =
					new CloseShieldOutputStream(dataStreamPlain);
			DataOutputStream dataStream =
					new DataOutputStream(dataStreamShielded);
		){
			Thread.currentThread().setName(THREAD_NAME_MULTIPLEXER);
			sendData(dataStream);
		}
	}
	
	private void sendData(DataOutputStream dataStream)
	throws InterruptedException, IOException {
		while (true) {
			BinaryItem revision = revisionQueue.take();
			BinaryItem metadata = metadataQueue.take();
			long revisionId = revision.getRevisionId();
			if (revisionId == Long.MAX_VALUE) {
				LOG.debug(LOG_MSG_SENDING_XML_DOCUMENT_TAIL);
				sendItem(metadata, dataStream);
				sendItem(revision, dataStream);
				break;
			}
			else {
				if (System.currentTimeMillis() - lastMillis > DELAY) {
					LOG.debug(String.format(
						LOG_MSG_SENDING_REVISION_AT_QUEUE_SIZE, revisionId,
						mapQueue.size()));
					lastMillis = System.currentTimeMillis();
				}
				Result result = new Result(revisionId, null);
				mapQueue.put(revision.getRevisionId(), result);
				sendItem(metadata, dataStream);
				sendItem(revision, dataStream);
			}
		}
	}
	
	private void sendItem(BinaryItem item, DataOutputStream dataStream)
	throws IOException {
		dataStream.writeInt(item.getBytes().length);
		dataStream.write(item.getBytes(), 0, item.getBytes().length);
	}

	private File getOutputFile(String accessToken) {
		String filename = accessToken + EXT_CSV;
		return new File(config.getOutputPath(), filename);
	}
}
