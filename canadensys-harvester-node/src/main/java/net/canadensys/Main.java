package net.canadensys;

import net.canadensys.harvester.main.ProcessingNodeMain;

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
		cmdLineOptions.addOption("brokerip", true, "ActiveMQ broker service");

		CommandLineParser parser = new PosixParser();
		CommandLine cmdLine = null;
		try {
			cmdLine = parser.parse(cmdLineOptions, args);
		}
		catch (ParseException e) {
			System.out.println(e.getMessage());
		}

		if (cmdLine != null) {
			String ipAddress = cmdLine.getOptionValue("brokerip");
			ProcessingNodeMain.main(ipAddress);
		}
	}
}
