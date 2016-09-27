package de.upb.wdqa.wsdmcup17.dataserver.result;

/** 
 * The vandalism score for a given revision id.
 */
public class ScoringResult {
	long revisionId;	
	Float score;
	
	public ScoringResult(long revisionId, Float score) {
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