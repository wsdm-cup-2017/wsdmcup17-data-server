package org.wsdmcup17.dataserver;

import java.io.File;

public class Configuration {
	
	private File revisionFile;
	private File metadataFile;
	private File outputPath;
	private int port;
	private boolean checkAccessTokenAgainstTira;
	
	public Configuration(String revisionFileName, String metadataFileName,
			String outputPath, int port, boolean checkAccessTokenAgainstTira) {
		this.revisionFile = new File(revisionFileName);
		this.metadataFile = new File(metadataFileName);
		this.outputPath = new File(outputPath);
		this.port = port;
		this.checkAccessTokenAgainstTira = checkAccessTokenAgainstTira;
	}
	
	public File getRevisionFile() {
		return revisionFile;
	}

	public File getMetadataFile() {
		return metadataFile;
	}

	public File getOutputPath() {
		return outputPath;
	}

	public int getPort() {
		return port;
	}
	
	public boolean getCheckAccessTokenAgainstTira() {
		return checkAccessTokenAgainstTira;
	}
}
