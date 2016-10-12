package org.wsdmcup17.dataserver;

import java.io.File;

public class Configuration {
	
	private File revisionFile;
	private File metadataFile;
	private File outputPath;
	private File tiraPath;
	private int port;
	private boolean checkAccessTokenAgainstTira;
	
	public Configuration(String revisionFileName, String metadataFileName,
			String outputPath, String tiraPath, int port) {
		this.revisionFile = new File(revisionFileName);
		this.metadataFile = new File(metadataFileName);
		this.outputPath = new File(outputPath);
		this.tiraPath = tiraPath != null ? new File(tiraPath) : null;
		this.checkAccessTokenAgainstTira = (tiraPath != null);
		this.port = port;
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
	
	public File getTiraPath() {
		return tiraPath;
	}

	public int getPort() {
		return port;
	}
	
	public boolean getCheckAccessTokenAgainstTira() {
		return checkAccessTokenAgainstTira;
	}
}
