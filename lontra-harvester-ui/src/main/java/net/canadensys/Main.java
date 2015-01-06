package net.canadensys;

import net.canadensys.harvester.main.JobInitiatorMain;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class Main {

	/**
	 * Haverster entry point
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Options cmdLineOptions = new Options();
		cmdLineOptions.addOption("brokerip", true, "Override ActiveMQ broker URL");

		CommandLineParser parser = new PosixParser();
		CommandLine cmdLine = null;
		try {
			cmdLine = parser.parse(cmdLineOptions, args);
		}
		catch (ParseException e) {
			System.out.println(e.getMessage());
		}

		if (cmdLine != null) {
			String brokerIpAddress = cmdLine.getOptionValue("brokerip");
			JobInitiatorMain.main(brokerIpAddress);
		}
	}
}
