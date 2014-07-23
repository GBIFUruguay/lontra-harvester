package net.canadensys.harvester.message.control;

import net.canadensys.harvester.message.ControlMessageIF;

/**
 * Control message used to inform all nodes of the current version of the library .
 * @author canadensys
 *
 */
public class VersionControlMessage implements ControlMessageIF {
	
	private String currentVersion;
	
	public VersionControlMessage(){}
	
	public VersionControlMessage(String currentVersion){
		this.currentVersion = currentVersion;
	}

	public String getCurrentVersion() {
		return currentVersion;
	}
	public void setCurrentVersion(String currentVersion) {
		this.currentVersion = currentVersion;
	}
	
	@Override
	public void setNodeIdentifier(String clientID) {
		// noop
		
	}
	@Override
	public String getNodeIdentifier() {
		//noop
		return null;
	}

}
