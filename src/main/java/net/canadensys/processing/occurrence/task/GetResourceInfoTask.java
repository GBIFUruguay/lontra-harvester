package net.canadensys.processing.occurrence.task;

import java.util.Map;

import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.exception.TaskExecutionException;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.model.ResourceModel;

import org.hibernate.Criteria;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.transaction.annotation.Transactional;

/**
 * Task to get info based on a resource id.
 * The info will be shared using the sharedParameters map.
 * @author canadensys
 *
 */
public class GetResourceInfoTask implements ItemTaskIF{
	
	@Autowired
	@Qualifier(value="publicSessionFactory")
	private SessionFactory sessionFactory;
	
	@Transactional("publicTransactionManager")
	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters)
			throws TaskExecutionException {
		Integer resourceId = (Integer)sharedParameters.get(SharedParameterEnum.RESOURCE_ID);

		Criteria searchCriteria = sessionFactory.getCurrentSession().createCriteria(ResourceModel.class);
		searchCriteria.add(Restrictions.eq("id", resourceId));
		ResourceModel resourceModel = (ResourceModel)searchCriteria.uniqueResult();
		
		if(resourceModel == null){
			throw new TaskExecutionException("ResourceID " + resourceId + " not found");
		}
		sharedParameters.put(SharedParameterEnum.DWCA_URL, resourceModel.getArchive_url());
		sharedParameters.put(SharedParameterEnum.DATASET_SHORTNAME, resourceModel.getSource_file_id());
	}
	
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
}
