package de.upb.wdqa.wsdmcup17.dataserver;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/*
 * Parses the command line arguments.
 */
public class WSDMCup17ServerConfiguration {
	File revisionFile;
	File metaFile;
	File outputPath;
	int port;
	
	public WSDMCup17ServerConfiguration(String[] args) {
		CommandLine cmd = parseArgs(args);
		
		revisionFile = new File(cmd.getOptionValue("r"));
		metaFile = new File(cmd.getOptionValue("m"));
		outputPath = new File(cmd.getOptionValue("o"));
		port = Integer.parseInt(cmd.getOptionValue("p"));
		
	}
	
	
    static CommandLine parseArgs(String[] args){
	    Options options = new Options();
	
	    Option input = new Option("r", "revision", true, "revision file");
	    input.setRequired(true);
	    options.addOption(input);
	    
	    Option meta = new Option("m", "meta", true, "meta file");
	    meta.setRequired(true);
	    options.addOption(meta);
	
	    Option output = new Option("o", "output", true, "output path");
	    output.setRequired(true);
	    options.addOption(output);
	    
	    Option port = new Option("p", "port", true, "port");
	    port.setRequired(true);
	    options.addOption(port);
	
	    CommandLineParser parser = new DefaultParser();
	    HelpFormatter formatter = new HelpFormatter();
	    CommandLine cmd = null;
	
	    try {
	        cmd = parser.parse(options, args);
	    } catch (ParseException e) {
	        System.out.println(e.getMessage());
	        formatter.printHelp("WSDM Cup 2017 Data Server", options);
	
	        System.exit(1);
	    }
		return cmd;    
    }
	
	public File getRevisionFile() {
		return revisionFile;
	}

	public File getMetaFile() {
		return metaFile;
	}

	public File getOutputPath() {
		return outputPath;
	}

	public int getPort() {
		return port;
	}
}
