package net.canadensys.harvester.main;

import java.util.List;

import net.canadensys.harvester.config.CLIMinimalConfig;
import net.canadensys.harvester.diagnosis.DiagnosisRun;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class DiagnosisMain {

	@Autowired
	private DiagnosisRun diagnosisRun;

	/**
	 * JobInitiator Entry point
	 * 
	 * @param args
	 */
	public static void main() {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(CLIMinimalConfig.class);
		DiagnosisMain dm = ctx.getBean(DiagnosisMain.class);
		dm.runDiagnosis();
	}

	public void runDiagnosis() {
		List<String> results = diagnosisRun.runDiagnosis();
		for (String line : results) {
			System.out.println(line);
		}
	}

}
