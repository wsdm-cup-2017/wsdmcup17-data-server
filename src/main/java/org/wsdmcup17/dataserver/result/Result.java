package org.wsdmcup17.dataserver.result;

/**
 * The vandalism score for a given revision id.
 */
public class Result {
	
	private long revisionId;	
	private Float score;
	
	public Result(long revisionId, Float score) {
		this.revisionId = revisionId;
		this.score = score;
	}

	public long getRevisionId() {
		return revisionId;
	}
	
	public Float getScore() {
		return score;
	}

	public void setScore(Float score) {
		this.score = score;		
	}
}
