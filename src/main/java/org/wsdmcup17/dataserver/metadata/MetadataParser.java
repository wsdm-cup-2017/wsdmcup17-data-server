package org.wsdmcup17.dataserver.metadata;

import java.io.InputStream;

import org.wsdmcup17.dataserver.util.BinaryItem;
import org.wsdmcup17.dataserver.util.ItemProcessor;
import org.wsdmcup17.dataserver.util.LineParser;

/**
 * Parses a CSV file containing Wikidata metadata and creates a
 * {@link BinaryItem} for every revision. The items are forwarded to the given
 * {@link ItemProcessor}.
 */
public class MetadataParser extends LineParser {
	
	private boolean isFirstLine = true;	
	
	public MetadataParser(ItemProcessor processor, InputStream inputStream){
		super(processor, inputStream);
	}	

	@Override
	protected void consumeLine(String line) {
		if (isFirstLine) {
			isFirstLine = false;
		}
		else {
			curRevisionId = Long.valueOf(line.substring(0, line.indexOf(',')));
			processCurItem();
		}
	}
}

