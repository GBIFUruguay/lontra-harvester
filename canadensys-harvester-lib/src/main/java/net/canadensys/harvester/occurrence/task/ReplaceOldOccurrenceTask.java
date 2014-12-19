package net.canadensys.harvester.occurrence.task;

import java.util.Map;

import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.exception.TaskExecutionException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

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
		String resourceUuid = (String) sharedParameters.get(SharedParameterEnum.RESOURCE_UUID);

		if (sourceFileId == null) {
			LOGGER.fatal("Misconfigured task : sourceFileId cannot be null");
			throw new TaskExecutionException("Misconfigured task");
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
			query = session.createSQLQuery("DELETE FROM contact WHERE resource_uuid=?");
			query.setString(0, resourceUuid);
			query.executeUpdate();
			query = session.createSQLQuery("DELETE FROM resource_metadata WHERE resource_uuid=?");
			query.setString(0, resourceUuid);
			query.executeUpdate();


			// copy records from buffer
			query = session.createSQLQuery("INSERT INTO occurrence (SELECT * FROM buffer.occurrence WHERE sourcefileid=?)");
			query.setString(0, sourceFileId);
			int numberOfRecords = query.executeUpdate();
			query = session.createSQLQuery("INSERT INTO occurrence_raw (SELECT * FROM buffer.occurrence_raw WHERE sourcefileid=?)");
			query.setString(0, sourceFileId);
			query.executeUpdate();
			query = session.createSQLQuery("INSERT INTO resource_metadata (SELECT * FROM buffer.resource_metadata WHERE resource_metadata_fkey = (SELECT dwca_resource_id FROM buffer.resource_metadata where resource_uuid = ?)");
			query.setString(0, resourceUuid);
			query.executeUpdate();
			query = session.createSQLQuery("INSERT INTO contact (SELECT * FROM buffer.contact WHERE resource_uuid=?)");
			query.setString(0, resourceUuid);
			query.executeUpdate();
			
			// empty buffer schema for this sourcefileid
			query = session.createSQLQuery("DELETE FROM buffer.occurrence WHERE sourcefileid=?");
			query.setString(0, sourceFileId);
			query.executeUpdate();
			query = session.createSQLQuery("DELETE FROM buffer.occurrence_raw WHERE sourcefileid=?");
			query.setString(0, sourceFileId);
			query.executeUpdate();
			
			//empty buffer schema for resource_uuid
			query = session.createSQLQuery("DELETE FROM buffer.contact WHERE resource_metadata_fkey = (SELECT dwca_resource_id FROM buffer.resource_metadata where resource_uuid = ?)");
			query.setString(0, resourceUuid);
			query.executeUpdate();
			query = session.createSQLQuery("DELETE FROM buffer.resource_metadata WHERE resource_uuid=?");
			query.setString(0, resourceUuid);
			query.executeUpdate();

			sharedParameters.put(SharedParameterEnum.NUMBER_OF_RECORDS, numberOfRecords);
		}
		catch (HibernateException hEx) {
			LOGGER.fatal("Can't replace previous records in public schema.", hEx);
			throw new TaskExecutionException("Can't replace previous records in public schema.");
		}
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	@Override
	public String getTitle() {
		return "Replacing old occurrences";
	}
}
