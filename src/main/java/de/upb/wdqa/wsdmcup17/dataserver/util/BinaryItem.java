package de.upb.wdqa.wsdmcup17.dataserver.util;

/**
 * A binary item.
 */
public class BinaryItem {
	long revisionId;	
	byte[] bytes;
	
	
	public BinaryItem(long revisionId, byte[] bytes) {
		this.revisionId = revisionId;
		this.bytes = bytes;
	}

	public long getRevisionId() {
		return revisionId;
	}

	public byte[] getBytes() {
		return bytes;
	}
}
