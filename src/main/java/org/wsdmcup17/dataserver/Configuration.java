package org.wsdmcup17.dataserver;

import java.io.File;

public class Configuration {
	
	private static final String
		ERROR_MSG_INCONSISTENT_CONFIGURATION =
			"Inconsistent configuration for production: dataset name missing.";
	
	private File revisionFile;
	private File metadataFile;
	private File outputPath;
	private int port;
	private File tiraPath;
	private boolean isInProductionMode;
	private String tiraDatasetName;
	
	public Configuration(String revisionFileName, String metadataFileName,
			String outputPath, int port, String tiraPath,
			String tiraDatasetName) {
		this.revisionFile = new File(revisionFileName);
		this.metadataFile = new File(metadataFileName);
		this.outputPath = new File(outputPath);
		this.port = port;
		this.tiraPath = tiraPath != null ? new File(tiraPath) : null;
		this.tiraDatasetName = tiraDatasetName;
		
		if (tiraPath != null) {
			if (tiraDatasetName != null) {
				this.isInProductionMode = true;
			}
			else {
				throw new Error(ERROR_MSG_INCONSISTENT_CONFIGURATION);
			}
		}
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
	
	public String getTiraDatasetName() {
		return tiraDatasetName;
	}
		
	public boolean isInProductionMode() {
		return isInProductionMode;
	}
}
