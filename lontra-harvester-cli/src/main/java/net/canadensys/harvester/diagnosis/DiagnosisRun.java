package net.canadensys.harvester.diagnosis;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Class use to run a lontra/database diagnosis.
 * 
 * @author cgendreau
 * 
 */
public class DiagnosisRun {

	private final String SCHEMA_VERSION_FILE = "/schemaVersion.properties";

	@Autowired
	private JdbcTemplate jdbcTemplate;

	public List<String> runDiagnosis() {
		List<String> diagnosisResults = new ArrayList<String>();
		String databaseSchemaVersion = jdbcTemplate.queryForObject("SELECT schema_version FROM db_metadata", String.class);
		diagnosisResults.add("Database schema version: " + databaseSchemaVersion);

		// check lontra-harvester-lib schema version
		InputStream schemaVersionInputStream = getClass().getResourceAsStream(SCHEMA_VERSION_FILE);

		if (schemaVersionInputStream == null) {
			diagnosisResults.add("Can't read lontra schema version");
		}

		try {
			String lontraSchemaVersion = IOUtils.toString(schemaVersionInputStream);
			diagnosisResults.add("lontra schema version: " + lontraSchemaVersion);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return diagnosisResults;
	}

}
