package de.upb.wdqa.wsdmcup17.dataserver;

import java.io.File;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Appender;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {

	private static final Logger
		LOG = LoggerFactory.getLogger(Server.class);
	
	private static final String
		LOG_PATTERN = "[%d{yyyy-MM-dd HH:mm:ss}] [%-5p] [%t] [%c{1}] %m%n",
		LOG_MSG_LISTENING_ON = "Listening on port %s.",
		UTF_8 = "UTF-8",
		EXT_LOG = ".log";
	
	private static final int
		PARALLELISM = 100;
	
	public static final byte
		EOT = 4;

	private Configuration config;
	
	private boolean checkAccessTokenAgainstTira;
	
	public Server(Configuration config, boolean checkAccessTokenAgainstTira) {
		this.config = config;
		this.checkAccessTokenAgainstTira = checkAccessTokenAgainstTira;
	}

	void init() throws UnknownHostException {
		initLogger(getLogFile());
		int port = config.getPort();
		LOG.info(String.format(LOG_MSG_LISTENING_ON, port));
		ExecutorService es = Executors.newWorkStealingPool(PARALLELISM);
		try (ServerSocket serverSocket = new ServerSocket(port)) {
			while (true) {
				Socket clientSocket = serverSocket.accept();
				RequestHandler rh = new RequestHandler(config, clientSocket,
					checkAccessTokenAgainstTira);
				es.execute(rh);
			}
		}
		catch (Throwable e) {
			LOG.error("", e);
		}
		finally {
			es.shutdown();
			closeLogger();
		}
	}

	public void initLogger(File file){
		org.apache.log4j.Logger logger =
				org.apache.log4j.Logger.getRootLogger();
		
		ConsoleAppender consoleAppender = new ConsoleAppender();
		consoleAppender.setEncoding(UTF_8);
		consoleAppender.setLayout(new PatternLayout(LOG_PATTERN));
		consoleAppender.setThreshold(Level.ALL);
		consoleAppender.activateOptions();		
		AsyncAppender asyncConsoleAppender = new AsyncAppender();
		asyncConsoleAppender.addAppender(consoleAppender);
		asyncConsoleAppender.setBufferSize(1024);
		asyncConsoleAppender.activateOptions();
		logger.addAppender(asyncConsoleAppender);
		
		FileAppender fileAppender = new FileAppender();
		fileAppender.setEncoding(UTF_8);
		fileAppender.setFile(file.getAbsolutePath());
		fileAppender.setLayout(new PatternLayout(LOG_PATTERN));
		fileAppender.setThreshold(Level.ALL);
		fileAppender.setAppend(false);
		fileAppender.activateOptions();		
		AsyncAppender asyncFileAppender = new AsyncAppender();
		asyncFileAppender.addAppender(fileAppender);
		asyncFileAppender.setBufferSize(1024);
		asyncFileAppender.activateOptions();
		logger.addAppender(asyncFileAppender);
	}
	
	private void closeLogger() {
		org.apache.log4j.LogManager.shutdown();	
		Enumeration<?> e =
				org.apache.log4j.Logger.getRootLogger().getAllAppenders();
		while (e.hasMoreElements()) {
			Appender appender = (Appender)e.nextElement();
			appender.close();
		}
	}
	
	private File getLogFile()
	throws UnknownHostException {
		int port = config.getPort();
		String hostname = InetAddress.getLocalHost().getHostName();
		File outputPath = config.getOutputPath();
		String filename = hostname + "_" + port + EXT_LOG;
		File logFile = new File(outputPath, filename);
		return logFile;
	}
}
