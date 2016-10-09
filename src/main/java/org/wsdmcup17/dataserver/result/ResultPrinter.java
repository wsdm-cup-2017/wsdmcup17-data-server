package org.wsdmcup17.dataserver.result;

import java.io.IOException;

import org.apache.commons.csv.CSVPrinter;

/**
 * Writes a {@link Result} to a CSV file.
 */
public class ResultPrinter {
	
	CSVPrinter csvPrinter;
	
	public ResultPrinter(CSVPrinter csvPrinter) {
		this.csvPrinter = csvPrinter;
	}

	public void printResult(Result scoringResult) throws IOException {
		csvPrinter.print(scoringResult.getRevisionId());
		csvPrinter.print(scoringResult.getScore());
		csvPrinter.println();		
	}
}
