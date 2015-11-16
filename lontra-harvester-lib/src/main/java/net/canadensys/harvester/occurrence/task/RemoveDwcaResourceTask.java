package net.canadensys.harvester.occurrence.task;

import java.util.Map;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.transaction.annotation.Transactional;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.config.DatabaseConfig;
import net.canadensys.harvester.exception.TaskExecutionException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

public class RemoveDwcaResourceTask implements ItemTaskIF {

	// get log4j handler
	private static final Logger LOGGER = Logger.getLogger(RemoveDwcaResourceTask.class);

	@Autowired
	private DatabaseConfig databaseConfig;

	private DwcaResourceModel resourceToRemove;

	@Autowired
	@Qualifier(value = "publicSessionFactory")
	private SessionFactory sessionFactory;

	@Override
	public String getTitle() {
		return "RemoveDwcaResourceTask - Removing a given dwca resource and all related information";
	}

	/**
	 * @param sharedParameters
	 *            SharedParameterEnum.SOURCE_FILE_ID required
	 */
	@Transactional("publicTransactionManager")
	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters) throws TaskExecutionException {
		Session session = sessionFactory.getCurrentSession();

		DwcaResourceModel resourceModel = (DwcaResourceModel) sharedParameters.get(SharedParameterEnum.RESOURCE_MODEL);

		if (resourceModel == null) {
			LOGGER.fatal("Misconfigured task : resourceModel is required");
			throw new TaskExecutionException("Misconfigured ReplaceOldOccurrenceTask");
		} else {
			Integer resourceId = (Integer) resourceModel.getId();
			String sourceFileId = resourceModel.getSourcefileid();

			/**
			 * Important: observe if the fields are in the same order and have
			 * the same amount of fields in both public and buffer schema to
			 * avoid errors
			 */
			try {
				// Remove records from occurrence_raw
				SQLQuery query = session.createSQLQuery("DELETE FROM occurrence_raw WHERE sourcefileid=?");
				query.setString(0, sourceFileId);
				query.executeUpdate();
				// Remove records from occurrence
				query = session.createSQLQuery("DELETE FROM occurrence WHERE sourcefileid=?");
				query.setString(0, sourceFileId);
				query.executeUpdate();
				// Remove records from occurrence_extension
				query = session.createSQLQuery("DELETE FROM occurrence_extension WHERE sourcefileid=?");
				query.setString(0, sourceFileId);
				query.executeUpdate();
				// Remove related records from unique_values
				String resourceName = resourceModel.getName();
				query = session.createSQLQuery("DELETE FROM unique_values WHERE key='resourcename' and value=?");
				query.setString(0, resourceName);
				query.executeUpdate();
				query = session.createSQLQuery("DELETE FROM unique_values WHERE key='sourcefileid' and value=?");
				query.setString(0, sourceFileId);
				query.executeUpdate();
				// Remove related contact records
				query = session.createSQLQuery("DELETE FROM contact WHERE resource_metadata_fkey=?");
				query.setInteger(0, resourceId);
				query.executeUpdate();
				// Remove resource_metadata record
				query = session.createSQLQuery("DELETE FROM resource_metadata WHERE dwca_resource_id=?");
				query.setInteger(0, resourceId);
				query.executeUpdate();
				// Remove dwca_resource record
				query = session.createSQLQuery("DELETE FROM dwca_resource WHERE id=?");
				query.setInteger(0, resourceId);
				query.executeUpdate();
			} catch (HibernateException e) {
				e.printStackTrace();
			}
		}
	}

	public void setResourceToRemove(DwcaResourceModel resourceToRemove) {
		this.resourceToRemove = resourceToRemove;
	}
}