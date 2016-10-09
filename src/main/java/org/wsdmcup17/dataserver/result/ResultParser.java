package org.wsdmcup17.dataserver.result;

import java.io.IOException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.wsdmcup17.dataserver.util.NonBlockingLineBufferedInputStream;

/**
 * Parses the scoring results provided in CSV format and converts them to
 * {@link Result} objects.
 */
public class ResultParser {
	
	private static final String
		ERROR_MSG_WRONG_HEADER = "Wrong header",
		ERROR_MSG_WRONG_NUMBER_OF_COLUMNS = "Wrong number of columns: %d",
		VANDALISM_SCORE = "VANDALISM_SCORE",
		REVISION_ID = "REVISION_ID";

	public static final String[]
		RESULT_CSV_HEADER = { REVISION_ID, VANDALISM_SCORE };
	
	public static final CSVFormat
		CSV_FORMAT = CSVFormat.RFC4180.withHeader(RESULT_CSV_HEADER);
	
	private NonBlockingLineBufferedInputStream lineInputStream;
	private boolean isFirstRow = true;
	
	public ResultParser(NonBlockingLineBufferedInputStream lineInputStream) {
		this.lineInputStream = lineInputStream;
	}

	public Result parseResult() throws IOException {
		if (isFirstRow) {
			String line = lineInputStream.readLine();
			checkHeader(line);
			isFirstRow = false;
		}
		String line = lineInputStream.readLine();
		if (line == null) {
			return null;
		}
		else {
			return parseLine(line);
		}
	}
	
	private void checkHeader(String line) throws IOException {
		CSVRecord csvRecord = parseLineRecord(line);
		int size = csvRecord.size();
		if(size != 2) {
			String e = String.format(ERROR_MSG_WRONG_NUMBER_OF_COLUMNS, size);
			throw new IOException(e);
		}
		if (!RESULT_CSV_HEADER[0].equals(csvRecord.get(RESULT_CSV_HEADER[0])) ||
			!RESULT_CSV_HEADER[1].equals(csvRecord.get(RESULT_CSV_HEADER[1]))) {
			throw new IOException(ERROR_MSG_WRONG_HEADER);
		}
	}
	
	private CSVRecord parseLineRecord(String line) throws IOException {
		CSVParser parser = CSVParser.parse(line, CSV_FORMAT);
		return parser.getRecords().get(0);
	}
	
	public Result parseLine(String line) throws IOException {
		CSVRecord csvRecord = parseLineRecord(line);
		long revisionId = Long.parseLong(csvRecord.get(REVISION_ID));
		float score = Float.parseFloat(csvRecord.get(VANDALISM_SCORE));
		return new Result(revisionId, score);
	}
}
