package org.wsdmcup17.dataserver.util;

/**
 * A binary item.
 */
public class BinaryItem {
	private long revisionId;	
	private byte[] bytes;
	
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
