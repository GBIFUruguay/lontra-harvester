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

import net.canadensys.dataportal.occurrence.model.PublisherModel;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.config.DatabaseConfig;
import net.canadensys.harvester.exception.TaskExecutionException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

public class RemovePublisherTask implements ItemTaskIF {

	// get log4j handler
	private static final Logger LOGGER = Logger.getLogger(RemovePublisherTask.class);

	@Autowired
	private DatabaseConfig databaseConfig;

	@Autowired
	@Qualifier(value = "publicSessionFactory")
	private SessionFactory sessionFactory;

	@Override
	public String getTitle() {
		return "RemovePublisherTask - Removing a given publisher and all related information";
	}

	/**
	 * @param sharedParameters
	 *            SharedParameterEnum.PUBLISHER_MODEL required
	 */
	@Transactional("publicTransactionManager")
	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters) throws TaskExecutionException {
		Session session = sessionFactory.getCurrentSession();

		PublisherModel publisherModel = (PublisherModel) sharedParameters.get(SharedParameterEnum.PUBLISHER_MODEL);

		if (publisherModel == null) {
			LOGGER.fatal("Misconfigured task : publisherModel is required");
			throw new TaskExecutionException("Misconfigured RemovePublisherTask");
		} else {
			Integer publisherId = (Integer) publisherModel.getAuto_id();

			/**
			 * Important: observe if the fields are in the same order and have
			 * the same amount of fields in both public and buffer schema to
			 * avoid errors
			 */
			try {
				// Remove records from occurrence_raw
				SQLQuery query = session.createSQLQuery("DELETE FROM contact WHERE publisher_fkey=?");
				query.setInteger(0, publisherId);
				query.executeUpdate();
				// Remove records from occurrence
				query = session.createSQLQuery("DELETE FROM publisher WHERE auto_id=?");
				query.setInteger(0, publisherId);
				query.executeUpdate();
			} catch (HibernateException e) {
				e.printStackTrace();
			}
		}
	}
}