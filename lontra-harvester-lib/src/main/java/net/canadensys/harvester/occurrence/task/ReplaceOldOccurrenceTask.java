package net.canadensys.harvester.occurrence.task;

import java.util.List;
import java.util.Map;

import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.config.DatabaseConfig;
import net.canadensys.harvester.exception.TaskExecutionException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.transaction.annotation.Transactional;

/**
 * Task to move all the records for a specific sourcefileid from buffer to public database schema
 * 
 * @author canadensys
 * 
 */
public class ReplaceOldOccurrenceTask implements ItemTaskIF {
	// get log4j handler
	private static final Logger LOGGER = Logger.getLogger(ReplaceOldOccurrenceTask.class);

	@Autowired
	private DatabaseConfig databaseConfig;

	@Autowired
	@Qualifier(value = "publicSessionFactory")
	private SessionFactory sessionFactory;

	/**
	 * @param sharedParameters
	 *            SharedParameterEnum.SOURCE_FILE_ID required
	 */
	@Transactional("publicTransactionManager")
	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters) {
		Session session = sessionFactory.getCurrentSession();

		String sourceFileId = (String) sharedParameters.get(SharedParameterEnum.SOURCE_FILE_ID);
		String resourceUUID = (String) sharedParameters.get(SharedParameterEnum.RESOURCE_UUID);
		String resourceID = (String) sharedParameters.get(SharedParameterEnum.RESOURCE_ID);

		if (sourceFileId == null || resourceUUID == null) {
			LOGGER.fatal("Misconfigured task : sourceFileId and resourceUUID are required");
			throw new TaskExecutionException("Misconfigured ReplaceOldOccurrenceTask");
		}

		/**
		 * Important: observe if the fields are in the same order and have the same
		 * amount of fields in both public and buffer schema to avoid errors
		 */
		try {
			// delete old records
			SQLQuery query = session.createSQLQuery("DELETE FROM occurrence WHERE sourcefileid=?");
			query.setString(0, sourceFileId);
			query.executeUpdate();
			query = session.createSQLQuery("DELETE FROM occurrence_raw WHERE sourcefileid=?");
			query.setString(0, sourceFileId);
			query.executeUpdate();

			query = session.createSQLQuery("DELETE FROM occurrence_extension WHERE resource_uuid=?");
			query.setString(0, resourceUUID);
			query.executeUpdate();

			query = session.createSQLQuery("DELETE FROM contact WHERE resource_metadata_fkey=?");
			query.setString(0, resourceID);
			query.executeUpdate();

			query = session.createSQLQuery("DELETE FROM resource_metadata WHERE resource_uuid=?");
			query.setString(0, resourceUUID);
			query.executeUpdate();

			// get public occurrence table columns names
			String occurrenceTableColumns = StringUtils.join(getColumnListForTable(session, "public", "occurrence"), ",");
			// copy records from buffer
			String sqlStr = String.format("INSERT INTO occurrence (%1$s) (SELECT %1$s FROM buffer.occurrence WHERE sourcefileid=?)",
					occurrenceTableColumns);
			query = session.createSQLQuery(sqlStr);
			query.setString(0, sourceFileId);
			int numberOfRecords = query.executeUpdate();

			// get public occurrence_raw table columns names
			String occurrenceRawTableColumns = StringUtils.join(getColumnListForTable(session, "public", "occurrence_raw"), ",");
			sqlStr = String.format("INSERT INTO occurrence_raw (%1$s) (SELECT %1$s FROM buffer.occurrence_raw WHERE sourcefileid=?)",
					occurrenceRawTableColumns);
			query = session.createSQLQuery(sqlStr);
			query.setString(0, sourceFileId);
			query.executeUpdate();

			query = session.createSQLQuery("INSERT INTO occurrence_extension (SELECT * FROM buffer.occurrence_extension WHERE resource_uuid=?)");
			query.setString(0, resourceUUID);
			query.executeUpdate();

			query = session.createSQLQuery("INSERT INTO resource_metadata (SELECT * FROM buffer.resource_metadata WHERE resource_uuid = ?)");
			query.setString(0, resourceUUID);
			query.executeUpdate();
			query = session.createSQLQuery("INSERT INTO contact (SELECT * FROM buffer.contact WHERE resource_metadata_fkey=?)");
			query.setString(0, resourceID);
			query.executeUpdate();

			// empty buffer schema for this sourcefileid
			query = session.createSQLQuery("DELETE FROM buffer.occurrence WHERE sourcefileid=?");
			query.setString(0, sourceFileId);
			query.executeUpdate();
			query = session.createSQLQuery("DELETE FROM buffer.occurrence_raw WHERE sourcefileid=?");
			query.setString(0, sourceFileId);
			query.executeUpdate();
			query = session.createSQLQuery("DELETE FROM buffer.occurrence_extension WHERE resource_uuid=?");
			query.setString(0, resourceUUID);
			query.executeUpdate();

			// empty buffer schema for resource_uuid
			query = session.createSQLQuery("DELETE FROM buffer.contact WHERE resource_metadata_fkey = ?");
			query.setString(0, resourceID);
			query.executeUpdate();
			query = session.createSQLQuery("DELETE FROM buffer.resource_metadata WHERE resource_uuid=?");
			query.setString(0, resourceUUID);
			query.executeUpdate();

			sharedParameters.put(SharedParameterEnum.NUMBER_OF_RECORDS, numberOfRecords);
		}
		catch (HibernateException hEx) {
			LOGGER.fatal("Can't replace previous records in public schema.", hEx);
			throw new TaskExecutionException("Can't replace previous records in public schema.");
		}
	}

	/**
	 * Get the name of all columns of a table. The main purpose is to ensure correct colum order when moving
	 * from buffer to public schema.
	 * 
	 * @param session
	 * @param schema
	 * @param table
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private List<String> getColumnListForTable(Session session, String schema, String table) {
		// get occurrence table columns names
		SQLQuery query = session.createSQLQuery(databaseConfig.getSelectColumnNamesSQL());
		query.setString(0, schema);
		query.setString(1, table);

		return query.list();
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	@Override
	public String getTitle() {
		return "Replacing old occurrences";
	}
}
