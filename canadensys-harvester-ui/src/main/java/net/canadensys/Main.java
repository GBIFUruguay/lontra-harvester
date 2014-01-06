package net.canadensys;

import net.canadensys.harvester.main.JobInitiatorMain;
import net.canadensys.harvester.main.ProcessingNodeMain;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class Main {
	
	/**
	 * Haverster entry point
	 * @param args
	 */
	public static void main(String[] args) {
		Options cmdLineOptions = new Options();
		cmdLineOptions.addOption("node", false, "Start as processing node");
		cmdLineOptions.addOption("initiator", false, "Start haverster initiator");
		cmdLineOptions.addOption("brokerip", true, "ActiveMQ broker service");
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmdLine = null;
		try {
			cmdLine = parser.parse(cmdLineOptions, args);	
		} catch (ParseException e) {
			System.out.println(e.getMessage());
		}
		
		if(cmdLine != null){
			String ipAddress = cmdLine.getOptionValue("brokerip");
			if(cmdLine.hasOption("initiator")){
				JobInitiatorMain.main();
			}
			else if(cmdLine.hasOption("node")){
				ProcessingNodeMain.main(ipAddress);
			}
			else{
				JobInitiatorMain.main();
			}
		}
	}

}
