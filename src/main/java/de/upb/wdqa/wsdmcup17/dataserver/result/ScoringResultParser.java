package de.upb.wdqa.wsdmcup17.dataserver.result;

import java.io.IOException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import de.upb.wdqa.wsdmcup17.dataserver.WSDMCup17DataServer;
import de.upb.wdqa.wsdmcup17.dataserver.util.NonBlockingLineBufferedInputStream;

/** 
 * Parses the scoring results provided in CSV format and converts them to {@link ScoringResult} objects. 
 */
public class ScoringResultParser {
	
	NonBlockingLineBufferedInputStream lineInputStream;
	
	boolean isFirstRow = true;
	
	public ScoringResultParser(NonBlockingLineBufferedInputStream lineInputStream){
		this.lineInputStream = lineInputStream;
	}

	public ScoringResult parseResult() throws IOException {
		 // skip header
		if (isFirstRow){
			String line = lineInputStream.readLine();
			checkHeader(line);
			isFirstRow = false;
		}
		
		String line = lineInputStream.readLine();

		if (line == null){
			return null;
		}
		else{
			return parseLine(line);
		}
	}
	
	private static void checkHeader(String line) throws IOException {
		CSVRecord csvRecord = parseLineRecord(line);
		if(csvRecord.size() != 2){
			throw new IOException("Wrong number of columns: " + csvRecord.size());
		}
		
		if (!WSDMCup17DataServer.RESULT_CSV_HEADER[0].equals(csvRecord.get(WSDMCup17DataServer.RESULT_CSV_HEADER[0])) ||
			!WSDMCup17DataServer.RESULT_CSV_HEADER[1].equals(csvRecord.get(WSDMCup17DataServer.RESULT_CSV_HEADER[1])) ){
			throw new IOException("Wrong header");
		}
	}
	
	private static CSVRecord parseLineRecord(String line) throws IOException{
		CSVParser parser = CSVParser.parse(line, CSVFormat.RFC4180.withHeader(WSDMCup17DataServer.RESULT_CSV_HEADER));
		CSVRecord csvRecord = parser.getRecords().get(0);
		return csvRecord;
	}
	
	public static ScoringResult parseLine(String line) throws IOException{
		CSVRecord csvRecord = parseLineRecord(line);
		long revisionId = Long.parseLong(csvRecord.get("REVISION_ID"));
		float score = Float.parseFloat(csvRecord.get("VANDALISM_SCORE"));
		
		ScoringResult result = new ScoringResult(revisionId, score);
		
		return result;
	}

}
