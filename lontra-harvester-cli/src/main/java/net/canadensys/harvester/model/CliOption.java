package net.canadensys.harvester.model;

import net.canadensys.harvester.main.JobInitiatorMain.CommandType;

/**
 * Simple class to keep options passed to the command line.
 * 
 * @author cgendreau
 *
 */
public class CliOption {

	private final CommandType commandType;

	private String resourceIdentifier;
	private String exclusionFilePath;

	public CliOption(CommandType commandType) {
		this.commandType = commandType;
	}

	public String getResourceIdentifier() {
		return resourceIdentifier;
	}

	public void setResourceIdentifier(String resourceIdentifier) {
		this.resourceIdentifier = resourceIdentifier;
	}

	public String getExclusionFilePath() {
		return exclusionFilePath;
	}

	public void setExclusionFilePath(String exclusionFilePath) {
		this.exclusionFilePath = exclusionFilePath;
	}

	public CommandType getCommandType() {
		return commandType;
	}

}
