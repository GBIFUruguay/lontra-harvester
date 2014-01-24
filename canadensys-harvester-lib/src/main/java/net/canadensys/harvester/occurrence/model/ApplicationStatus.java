package net.canadensys.harvester.occurrence.model;

/**
 * TODO Rename class
 * Container object for the state of the application.
 * @author canadensys
 *
 */
public class ApplicationStatus {
	
	public static enum JobStatusEnum {UNDEFINED,WAITING,RUNNING,CANCEL_NODE_ERROR,DONE_ERROR,DONE_SUCCESS};
	
	private JobStatusEnum importStatus = JobStatusEnum.UNDEFINED;
	private JobStatusEnum moveStatus = JobStatusEnum.UNDEFINED;
	private String lastError;
	
	public JobStatusEnum getImportStatus() {
		return importStatus;
	}
	public void setImportStatus(JobStatusEnum importStatus) {
		this.importStatus = importStatus;
	}
	
	public JobStatusEnum getMoveStatus() {
		return moveStatus;
	}
	public void setMoveStatus(JobStatusEnum moveStatus) {
		this.moveStatus = moveStatus;
	}

	public String getLastError() {
		return lastError;
	}
	public void setLastError(String lastError) {
		this.lastError = lastError;
	}
	
	public void resetStatus(){
		importStatus = JobStatusEnum.UNDEFINED;
		moveStatus = JobStatusEnum.UNDEFINED;
	}

}
