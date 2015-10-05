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

import net.canadensys.dataportal.occurrence.model.OccurrenceFieldConstants;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.exception.TaskExecutionException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

/**
 * Complementary task to set occurrence's publishername once a resource is associated to a publisher.
 *
 * @author Pedro Guimarães
 *
 */
public class PublisherNameUpdateTask implements ItemTaskIF {

	// get log4j handler
	private static final Logger LOGGER = Logger.getLogger(PublisherNameUpdateTask.class);

	@Autowired
	@Qualifier(value = "publicSessionFactory")
	private SessionFactory sessionFactory;

	@Transactional("publicTransactionManager")
	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters)
			throws TaskExecutionException {
		String publisherName = (String) sharedParameters.get(SharedParameterEnum.PUBLISHER_NAME);
		Integer resourceId = (Integer) sharedParameters.get(SharedParameterEnum.RESOURCE_ID);
	

		if (publisherName == null || resourceId == null) {
			LOGGER.fatal("Misconfigured task : publisherName and resourceId are required.");
			throw new TaskExecutionException("Misconfigured PublisherNameUpdateTask");
		}

		Session session = sessionFactory.getCurrentSession();

		try {
			
			// Update resource name for this dataset's occurrence records:
			SQLQuery query = session.createSQLQuery("update occurrence set publishername = ? where " + OccurrenceFieldConstants.RESOURCE_ID
					+ " = ?;");

			query.setString(0, publisherName);
			query.setInteger(1, resourceId);
			query.executeUpdate();


			LOGGER.info("PostProcessOccurrence finished successfully.");
		}
		catch (HibernateException hEx) {
			LOGGER.fatal("Can't execute PublisherNameUpdateTask on the database.", hEx);
			throw new TaskExecutionException("Can't execute PublisherNameUpdateTask on the database.");
		}
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	@Override
	public String getTitle() {
		return "Update publishername task.";
	}
}
