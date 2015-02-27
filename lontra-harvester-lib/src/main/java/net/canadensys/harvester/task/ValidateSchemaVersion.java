package net.canadensys.harvester.task;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import net.canadensys.databaseutils.model.DBMetadata;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.exception.TaskExecutionException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Criteria;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.transaction.annotation.Transactional;

/**
 * This task validate the schema version of the database against the one used for building
 * this version of the lontra-harvester.
 * 
 * @author cgendreau
 * 
 */
public class ValidateSchemaVersion implements ItemTaskIF {

	private final String SCHEMA_VERSION_FILE = "/schemaVersion.properties";

	@Autowired
	@Qualifier(value = "publicSessionFactory")
	private SessionFactory sessionFactory;

	@Override
	public String getTitle() {
		return "Validating database schema version";
	}

	@Transactional("publicTransactionManager")
	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters) throws TaskExecutionException {

		Criteria dbMetadataCriteria = sessionFactory.getCurrentSession().createCriteria(DBMetadata.class);
		DBMetadata dbMetadata = (DBMetadata) dbMetadataCriteria.uniqueResult();
		InputStream schemaVersionInputStream = getClass().getResourceAsStream(SCHEMA_VERSION_FILE);

		if (schemaVersionInputStream != null && dbMetadata != null) {
			try {
				String lontraSchemaVersion = IOUtils.toString(schemaVersionInputStream);
				String dbSchemaVersion = dbMetadata.getSchema_version();

				if (!StringUtils.equals(lontraSchemaVersion, dbSchemaVersion)) {
					throw new TaskExecutionException("Version of the database schema doesn't match: lontra=" + lontraSchemaVersion
							+ ", database=" + dbSchemaVersion);
				}
			}
			catch (IOException e) {
				throw new TaskExecutionException("Error while cheking the database schema version", e);
			}
		}
		else {
			throw new TaskExecutionException("Error while cheking the database schema version");
		}
	}

}
