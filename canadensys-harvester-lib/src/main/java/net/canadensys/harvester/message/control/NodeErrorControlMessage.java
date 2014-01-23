package net.canadensys.harvester.message.control;

import net.canadensys.harvester.message.ControlMessageIF;

/**
 * Control message used to report an error from a processing node.
 * @author canadensys
 *
 */
public class NodeErrorControlMessage implements ControlMessageIF {
	
	private String nodeIdentifier;
	private String errorMessage;
	
	public NodeErrorControlMessage(){}
	
	public NodeErrorControlMessage(Exception ex){
		errorMessage = ex.getMessage();
	}
	
	public String getNodeIdentifier() {
		return nodeIdentifier;
	}
	public void setNodeIdentifier(String nodeIdentifier) {
		this.nodeIdentifier = nodeIdentifier;
	}
	
	public String getErrorMessage() {
		return errorMessage;
	}
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

}
