package net.canadensys.harvester.message;

/**
 * Used to allow different control message implementations to share the same interface.
 * 
 * @author canadensys
 * 
 */
public interface ControlMessageIF {

	public void setNodeIdentifier(String clientID);

	public String getNodeIdentifier();

}
