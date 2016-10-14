package org.wsdmcup17.dataserver.revision;

import java.io.InputStream;

import org.wsdmcup17.dataserver.util.BinaryItem;
import org.wsdmcup17.dataserver.util.ItemProcessor;
import org.wsdmcup17.dataserver.util.LineParser;

/**
 * Parses a Wikimedia XML file and creates a {@link BinaryItem} for every
 * revision. The items are forwarded to the given {@link ItemProcessor}.
 */
public class RevisionParser extends LineParser {

	private static final String REVISION_OPENING_TAG = "    <revision>";
	private static final String REVISION_CLOSING_TAG = "    </revision>";
	private static final String REVISION_ID_OPENING_TAG = "      <id>";
	private static final String REVISION_ID_CLOSING_TAG = "</id>";

	private enum State {
		EXPECT_REVISION, EXPECT_REVISION_ID, EXPECT_REVISION_CLOSING_TAG
	}

	private State state = State.EXPECT_REVISION;

	public RevisionParser(ItemProcessor processor, InputStream inputStream) {
		super(processor, inputStream);
	}

	@Override
	protected void consumeLine(String line) {
		if (line == null){ // end of file
			appendToItem();
			processLastItem();
			return;
		}
		
		switch (state) {
		case EXPECT_REVISION:
			if (line.equals(REVISION_OPENING_TAG)) {
				state = State.EXPECT_REVISION_ID;
				processLastItem();
			}
			break;
		case EXPECT_REVISION_ID:
			if (line.startsWith(REVISION_ID_OPENING_TAG)) {
				String revisionId = getSubstring(
					line, REVISION_ID_OPENING_TAG, REVISION_ID_CLOSING_TAG);
				curRevisionId = Long.parseLong(revisionId);
				state = State.EXPECT_REVISION_CLOSING_TAG;
			}
			break;
		case EXPECT_REVISION_CLOSING_TAG:
			if (line.equals(REVISION_CLOSING_TAG)) {
				state = State.EXPECT_REVISION;
				endItem();
			}
			break;
		default:
			break;
		}
	}

	private String getSubstring(String s, String start, String end) {
		return s.substring(start.length(), s.length() - end.length());
	}
}

