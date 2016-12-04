package org.wsdmcup17.dataserver;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.FileAppender;

public class Main {
	
	private static final String
		WSDM_CUP_2017_DATA_SERVER = "WSDM Cup 2017 Data Server",
		OPT_REVISION_FILE = "r",
		OPT_REVISION_FILE_LONG = "revision",
		OPT_REVISION_FILE_DESC = "Revision file",
		OPT_METADATA_FILE = "m",
		OPT_METADATA_FILE_LONG = "metadata",
		OPT_METADATA_FILE_DESC = "Metadata file",
		OPT_OUTPUT_PATH = "o",
		OPT_OUTPUT_PATH_LONG = "outputpath",
		OPT_OUTPUT_PATH_DESC = "Output path",
		OPT_PORT = "p",
		OPT_PORT_LONG = "port",
		OPT_PORT_DESC = "Port",
		OPT_TIRA_PATH = "t",
		OPT_TIRA_PATH_LONG = "tirapath",
		OPT_TIRA_PATH_DESC = "TIRA path",
		OPT_TIRA_DATASET_NAME = "d",
		OPT_TIRA_DATASET_NAME_LONG = "datasetname",
		OPT_TIRA_DATASET_NAME_DESC = "TIRA dataset name",
		LOG_PATTERN = "[%d{yyyy-MM-dd HH:mm:ss}] [%-5p] [%t] [%c{0}] %m%n",
		UTF_8 = "UTF-8",
		EXT_LOG = ".log";
	
	private static LoggerContext logContext;

	public static void main(String[] args) throws UnknownHostException {
		CommandLine cmd = parseArgs(args);
		Configuration config = new Configuration(
			cmd.getOptionValue(OPT_REVISION_FILE),
			cmd.getOptionValue(OPT_METADATA_FILE),
			cmd.getOptionValue(OPT_OUTPUT_PATH),
			Integer.parseInt(cmd.getOptionValue(OPT_PORT)),
			cmd.getOptionValue(OPT_TIRA_PATH),
			cmd.getOptionValue(OPT_TIRA_DATASET_NAME)
		);
		initLogger(getLogFile(config));
		try {
			Server server = new Server(config);
			server.start();
		}
		finally {
			closeLogger();
		}
	}
	
	private static CommandLine parseArgs(String[] args){
		Options options = new Options();
	
		Option input = new Option(OPT_REVISION_FILE, OPT_REVISION_FILE_LONG,
				true, OPT_REVISION_FILE_DESC);
		input.setRequired(true);
		options.addOption(input);
		
		Option metadata = new Option(OPT_METADATA_FILE, OPT_METADATA_FILE_LONG,
				true, OPT_METADATA_FILE_DESC);
		metadata.setRequired(true);
		options.addOption(metadata);
	
		Option outputPath = new Option(OPT_OUTPUT_PATH, OPT_OUTPUT_PATH_LONG,
				true, OPT_OUTPUT_PATH_DESC);
		outputPath.setRequired(true);
		options.addOption(outputPath);
		
		Option port = new Option(OPT_PORT, OPT_PORT_LONG, true, OPT_PORT_DESC);
		port.setRequired(true);
		options.addOption(port);
		
		Option tiraPath = new Option(OPT_TIRA_PATH, OPT_TIRA_PATH_LONG, true,
				OPT_TIRA_PATH_DESC);
		tiraPath.setRequired(false);
		options.addOption(tiraPath);
		
		Option tiraDatasetName = new Option(OPT_TIRA_DATASET_NAME,
				OPT_TIRA_DATASET_NAME_LONG, true, OPT_TIRA_DATASET_NAME_DESC);
		tiraDatasetName.setRequired(false);
		options.addOption(tiraDatasetName);
	
		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
	
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp(WSDM_CUP_2017_DATA_SERVER, options);
			System.exit(1);
		}
		return cmd;
	}
	
	public static void initLogger(File file){
		logContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		
		PatternLayoutEncoder encoder;
		encoder = new PatternLayoutEncoder();
		encoder.setContext(logContext);
		encoder.setCharset(Charset.forName(UTF_8));
		encoder.setPattern(LOG_PATTERN);
		encoder.start();
		
		ConsoleAppender<ILoggingEvent> consoleAppender = new ConsoleAppender<ILoggingEvent>();
		consoleAppender.setContext(logContext);
		consoleAppender.setName("console");
		consoleAppender.setEncoder(encoder);
		consoleAppender.start();
	
		AsyncAppender asyncConsoleAppender = new AsyncAppender();
		asyncConsoleAppender.setContext(logContext);
		asyncConsoleAppender.addAppender(consoleAppender);
		asyncConsoleAppender.setQueueSize(1024);
		asyncConsoleAppender.setDiscardingThreshold(0);
		asyncConsoleAppender.start();

		encoder = new PatternLayoutEncoder();
		encoder.setContext(logContext);
		encoder.setCharset(Charset.forName(UTF_8));
		encoder.setPattern(LOG_PATTERN);
		encoder.start();
		
		FileAppender<ILoggingEvent> fileAppender = new FileAppender<ILoggingEvent>();
		fileAppender.setContext(logContext);
		fileAppender.setEncoder(encoder);
		fileAppender.setFile(file.getAbsolutePath());
		fileAppender.setAppend(false);
		fileAppender.start();
		
		AsyncAppender asyncFileAppender = new AsyncAppender();
		asyncFileAppender.setContext(logContext);
		asyncFileAppender.addAppender(fileAppender);
		asyncFileAppender.setQueueSize(1024);
		asyncFileAppender.setDiscardingThreshold(0);
		asyncFileAppender.start();
		
		Logger logger = logContext.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
		logger.detachAndStopAllAppenders();
		logger.addAppender(asyncConsoleAppender);
		logger.addAppender(asyncFileAppender);
	}
	
	private static void closeLogger() {
		logContext.stop();
	}
	
	private static File getLogFile(Configuration config)
	throws UnknownHostException {
		int port = config.getPort();
		String hostname = InetAddress.getLocalHost().getHostName();
		File outputPath = config.getOutputPath();
		String filename = hostname + "_" + port + EXT_LOG;
		File logFile = new File(outputPath, filename);
		return logFile;
	}
}
