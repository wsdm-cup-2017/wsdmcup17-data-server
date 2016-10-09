package org.wsdmcup17.dataserver;

import java.net.UnknownHostException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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
		OPT_PORT_DESC = "Port";

	public static void main(String[] args) throws UnknownHostException {
		CommandLine cmd = parseArgs(args);
		Configuration config = 	new Configuration(
			cmd.getOptionValue(OPT_REVISION_FILE),
			cmd.getOptionValue(OPT_METADATA_FILE),
			cmd.getOptionValue(OPT_OUTPUT_PATH),
			Integer.parseInt(cmd.getOptionValue(OPT_PORT))
		);
		Server server = new Server(config, false);
		server.init();
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
}
