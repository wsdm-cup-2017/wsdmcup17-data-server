package de.upb.wdqa.wsdmcup17.dataserver.result;

import java.io.IOException;

import org.apache.commons.csv.CSVPrinter;

/**
 * Writes a {@link ScoringResult} to a CSV file.
 *
 */
public class ScoringResultPrinter {
	
	CSVPrinter csvPrinter;
	
	public ScoringResultPrinter(CSVPrinter csvPrinter) {
		this.csvPrinter = csvPrinter;
	}

	public void printResult(ScoringResult scoringResult) throws IOException {
		csvPrinter.print(scoringResult.getRevisionId());
		csvPrinter.print(scoringResult.getScore());
		csvPrinter.println();		
	}

}
