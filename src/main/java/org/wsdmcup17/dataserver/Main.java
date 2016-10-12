package org.wsdmcup17.dataserver;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Enumeration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Appender;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;

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
		LOG_PATTERN = "[%d{yyyy-MM-dd HH:mm:ss}] [%-5p] [%t] [%c{1}] %m%n",
		UTF_8 = "UTF-8",
		EXT_LOG = ".log";

	public static void main(String[] args) throws UnknownHostException {
		CommandLine cmd = parseArgs(args);
		Configuration config = 	new Configuration(
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
	
	private static void closeLogger() {
		org.apache.log4j.LogManager.shutdown();	
		Enumeration<?> e =
				org.apache.log4j.Logger.getRootLogger().getAllAppenders();
		while (e.hasMoreElements()) {
			Appender appender = (Appender)e.nextElement();
			appender.close();
		}
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
