package net.canadensys.harvester.occurrence.task;

import java.util.Map;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.exception.TaskExecutionException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.transaction.annotation.Transactional;

/**
 * Task to get info based on a resource id.
 * The info will be shared using the sharedParameters map.
 *
 * @author canadensys
 *
 */
public class GetResourceInfoTask implements ItemTaskIF {

	// get log4j handler
	private static final Logger LOGGER = Logger.getLogger(GetResourceInfoTask.class);

	@Autowired
	@Qualifier(value = "publicSessionFactory")
	private SessionFactory sessionFactory;

	/**
	 * @param sharedParameters
	 *            in:RESOURCE_ID, out:DWCA_URL,RESOURCE_MODEL
	 */
	@Transactional(value = "publicTransactionManager", readOnly = true)
	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters) throws TaskExecutionException {

		Integer resourceId = (Integer) sharedParameters.get(SharedParameterEnum.RESOURCE_ID);
		if (resourceId == null) {
			LOGGER.fatal("Misconfigured task : RESOURCE_ID is required");
			throw new TaskExecutionException("Misconfigured task");
		}

		Criteria searchCriteria = sessionFactory.getCurrentSession().createCriteria(DwcaResourceModel.class);
		searchCriteria.add(Restrictions.eq("id", resourceId));
		DwcaResourceModel resourceModel = (DwcaResourceModel) searchCriteria.uniqueResult();

		if (resourceModel == null) {
			throw new TaskExecutionException("ResourceID " + resourceId + " not found");
		}

		sharedParameters.put(SharedParameterEnum.DWCA_URL, resourceModel.getArchive_url());
		sharedParameters.put(SharedParameterEnum.RESOURCE_MODEL, resourceModel);
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	@Override
	public String getTitle() {
		return "Getting resource info";
	}
}
