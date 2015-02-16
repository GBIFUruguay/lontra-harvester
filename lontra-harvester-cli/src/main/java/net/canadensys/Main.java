package net.canadensys;

import net.canadensys.harvester.main.DiagnosisMain;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class Main {

	/**
	 * Haverster CLI entry point
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Options cmdLineOptions = new Options();
		// cmdLineOptions.addOption("r", true, "sourcefileid to harvest");
		cmdLineOptions.addOption("check", false, "run diagnosis");

		CommandLineParser parser = new PosixParser();
		CommandLine cmdLine = null;
		try {
			cmdLine = parser.parse(cmdLineOptions, args);
		}
		catch (ParseException e) {
			System.out.println(e.getMessage());
		}

		if (cmdLine != null) {
			if (cmdLine.hasOption("check")) {
				DiagnosisMain.main();
			}
			// String sourcefileid = cmdLine.getOptionValue("r");
			// JobInitiatorMain.main(sourcefileid);
		}
	}
}
