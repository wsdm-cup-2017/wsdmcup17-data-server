package de.upb.wdqa.wsdmcup17.dataserver.revision;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.upb.wdqa.wsdmcup17.dataserver.util.BinaryItem;
import de.upb.wdqa.wsdmcup17.dataserver.util.ItemProcessor;
import de.upb.wdqa.wsdmcup17.dataserver.util.LineParser;

/**
 * Parses an Wikimedia XML file and creates a {@link BinaryItem} for every
 * revision. The items are forwarded to the given {@link ItemProcessor}.
 *
 */
public class RevisionParser extends LineParser {
	final static Logger logger = LoggerFactory.getLogger(RevisionParser.class);

	private static final String REVISION_OPENING_TAG = "    <revision>";
	private static final String REVISION_CLOSING_TAG = "    </revision>";
	private static final String REVISION_ID_OPENING_TAG = "      <id>";
	private static final String REVISION_ID_CLOSING_TAG = "</id>";

	enum State {
		EXPECT_REVISION, EXPECT_REVISION_ID, EXPECT_REVISION_CLOSING_TAG
	}

	public RevisionParser(ItemProcessor processor, InputStream inputStream) {
		super(processor, inputStream);
	}

	private static State STATE = State.EXPECT_REVISION;

	@Override
	protected void consumeLine(String line) {
		switch (STATE) {
		case EXPECT_REVISION:
			if (line.equals(REVISION_OPENING_TAG)) {
				STATE = State.EXPECT_REVISION_ID;
			}
			break;
		case EXPECT_REVISION_ID:
			if (line.startsWith(REVISION_ID_OPENING_TAG)) {
				curRevisionId = Long.parseLong(getSubstring(line, REVISION_ID_OPENING_TAG, REVISION_ID_CLOSING_TAG));
				STATE = State.EXPECT_REVISION_CLOSING_TAG;
			}
			break;
		case EXPECT_REVISION_CLOSING_TAG:
			if (line.equals(REVISION_CLOSING_TAG)) {
				STATE = State.EXPECT_REVISION;

				processCurItem();
			}
			break;
		default:
			break;
		}

	}

	static String getSubstring(String s, String start, String end) {
		return s.substring(start.length(), s.length() - end.length());
	}

}
