package de.upb.wdqa.wsdmcup17.dataserver.meta;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.upb.wdqa.wsdmcup17.dataserver.util.BinaryItem;
import de.upb.wdqa.wsdmcup17.dataserver.util.ItemProcessor;
import de.upb.wdqa.wsdmcup17.dataserver.util.LineParser;

/**
 * Parses an CSV file containing Wikidata meta data and creates a {@link BinaryItem} for every
 * revision. The items are forwarded to the given {@link ItemProcessor}.
 *
 */
public class MetaParser extends LineParser {
	final static Logger logger = LoggerFactory.getLogger(MetaParser.class);
	
	boolean isFirstLine = true;	
	
	public MetaParser(ItemProcessor processor, InputStream inputStream){
		super(processor, inputStream);
	}	

	@Override
	protected void consumeLine(String line) {
		if (!isFirstLine){
			curRevisionId = Long.valueOf(line.substring(0, line.indexOf(',')));
			
			processCurItem();
		}
		else{
			isFirstLine = false;
		}
	}
}
