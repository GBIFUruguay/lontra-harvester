package net.canadensys.harvester;

import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ScriptException;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.test.jdbc.JdbcTestUtils;

/**
 * Helper class to manipulate test data between tests.
 * 
 * @author cgendreau
 * 
 */
public class TestDataHelper {

	public static final String TEST_DATA_SCRIPT_LOCATION = "classpath:insert-test-data.sql";

	/**
	 * Delete and insert data from the insert-test-data.sql script.
	 * 
	 * @param appContext
	 * @param template
	 */
	public static void loadTestData(ApplicationContext appContext, JdbcTemplate template) {
		Resource testDataScript = appContext.getResource(TEST_DATA_SCRIPT_LOCATION);

		try {
			JdbcTestUtils.deleteFromTables(template, "dwca_resource");
			JdbcTestUtils.deleteFromTables(template, "db_metadata");
			ScriptUtils.executeSqlScript(template.getDataSource().getConnection(), testDataScript);
		}
		catch (ScriptException e) {
			e.printStackTrace();
			fail();
		}
		catch (SQLException e) {
			e.printStackTrace();
			fail();
		}
	}

}
