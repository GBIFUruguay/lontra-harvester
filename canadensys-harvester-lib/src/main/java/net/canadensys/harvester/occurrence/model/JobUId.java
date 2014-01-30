package net.canadensys.harvester.occurrence.model;

import java.util.UUID;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Immutable unique identifier object for Jobs.
 * Simply wrap UUID and provides sourceFileId.
 * @author canadensys
 *
 */
public class JobUId {
	
	private String uid;
	
	//sourceFileId is the identifier for a most of the jobs
	private String sourceFileId;
	
	public JobUId() {
		uid = UUID.randomUUID().toString();
	}
	
	public JobUId(String sourceFileId) {
		this();
		this.sourceFileId = sourceFileId;
	}
	
	public String getUID(){
		return uid;
	}

	public String getSourceFileId() {
		return sourceFileId;
	}
	
	public int hashCode() {
		// you pick a hard-coded, randomly chosen, non-zero, odd number
		// ideally different for each class
		return new HashCodeBuilder(13, 411).
				append(uid).
				append(sourceFileId).
				toHashCode();
	}
	
	public boolean equals(Object obj) {
		if (obj == null) { return false; }
		if (obj == this) { return true; }
		if (obj.getClass() != getClass()) {
			return false;
		}
		JobUId eObj = (JobUId) obj;
		return new EqualsBuilder()
			.append(uid, eObj.uid)
			.append(sourceFileId, eObj.sourceFileId)
			.isEquals();
	 }

}
