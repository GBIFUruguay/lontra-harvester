package net.canadensys.harvester.occurrence.controller;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.annotation.Transactional;

import net.canadensys.dataportal.occurrence.dao.DwcaResourceDAO;
import net.canadensys.dataportal.occurrence.dao.PublisherDAO;
import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.dataportal.occurrence.model.ImportLogModel;
import net.canadensys.dataportal.occurrence.model.PublisherModel;
import net.canadensys.harvester.AbstractProcessingJob;
import net.canadensys.harvester.config.harvester.HarvesterConfigIF;
import net.canadensys.harvester.jms.control.JMSControlProducer;
import net.canadensys.harvester.message.control.VersionControlMessage;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.dao.IPTFeedDAO;
import net.canadensys.harvester.occurrence.job.ComputeUniqueValueJob;
import net.canadensys.harvester.occurrence.job.ImportDwcaJob;
import net.canadensys.harvester.occurrence.job.MoveToPublicSchemaJob;
import net.canadensys.harvester.occurrence.job.PublisherNameUpdateJob;
import net.canadensys.harvester.occurrence.job.RemoveDwcaResourceJob;
import net.canadensys.harvester.occurrence.job.RemovePublisherJob;
import net.canadensys.harvester.occurrence.model.DwcaResourceStatusModel;
import net.canadensys.harvester.occurrence.model.IPTFeedModel;
import net.canadensys.harvester.occurrence.model.JobStatusModel;
import net.canadensys.harvester.occurrence.model.JobStatusModel.JobStatus;
import net.canadensys.harvester.occurrence.status.ResourceStatusCheckerIF;
import net.canadensys.harvester.occurrence.view.model.HarvesterViewModel;

/**
 * Main controller to initiate jobs.
 * This controller is NOT thread safe.
 *
 * @author canadensys
 *
 */
public class StepController implements StepControllerIF {

	private static final String IMPORT_DWCA_JOB_BEAN = "importDwcaJob";

	@Autowired
	private ApplicationContext appContext;

	@Autowired
	private HarvesterConfigIF harvesterConfig;

	@Qualifier("currentVersion")
	@Autowired
	private String currentVersion;

	@Autowired
	@Qualifier(value = "publicSessionFactory")
	private SessionFactory sessionFactory;

	@Autowired
	private IPTFeedDAO iptFeedDAO;

	@Autowired
	private DwcaResourceDAO resourceDAO;

	@Autowired
	private PublisherDAO publisherDAO;

	@Autowired
	private ResourceStatusCheckerIF resourceStatusChecker;

	// Do not use @Autowired, the object is created dynamically
	private ImportDwcaJob importDwcaJob;
	private boolean moveToPublicSchema;
	private boolean computeUniqueValues;

	@Autowired
	private MoveToPublicSchemaJob moveToPublicSchemaJob;

	@Autowired
	private ComputeUniqueValueJob computeUniqueValueJob;
	
	@Autowired
	private RemoveDwcaResourceJob removeDwcaResourceJob;

	@Autowired
	private RemovePublisherJob removePublisherJob;
	
	@Autowired
	private PublisherNameUpdateJob publisherNameUpdateJob;
	
	@Autowired
	private HarvesterViewModel harvesterViewModel;
	private final JobStatusModel jobStatusModel;

	@Autowired
	private JMSControlProducer controlMessageProducer;

	@Autowired
	private NodeStatusController nodeStatusController;

	private AbstractProcessingJob currentJob;

	public StepController() {
		// we always use the same JobStatusModel object and we should be the only listener on it
		jobStatusModel = new JobStatusModel();
		jobStatusModel.addPropertyChangeListener(new JobStatusModelListener());
	}

	@Override
	public void importDwcA(Integer resourceId, boolean moveToPublicSchema, boolean computeUniqueValues) {
		this.moveToPublicSchema = moveToPublicSchema;
		this.computeUniqueValues = computeUniqueValues;

		controlMessageProducer.open();
		// send the app version
		controlMessageProducer.publish(new VersionControlMessage(currentVersion));
		controlMessageProducer.close();

		importDwcaJob = (ImportDwcaJob) appContext.getBean(IMPORT_DWCA_JOB_BEAN);
		// enable node status controller
		nodeStatusController.start();
		importDwcaJob.addToSharedParameters(SharedParameterEnum.RESOURCE_ID, resourceId);
		currentJob = importDwcaJob;

		importDwcaJob.doJob(jobStatusModel);
	}

	@Override
	public void moveToPublicSchema(Integer resourceID, boolean computeUniqueValues) {
		this.computeUniqueValues = computeUniqueValues;
		moveToPublicSchemaJob.addToSharedParameters(SharedParameterEnum.RESOURCE_ID, resourceID);
		moveToPublicSchemaJob.doJob(jobStatusModel);
		currentJob = moveToPublicSchemaJob;
	}

	@Override
	public void computeUniqueValues() {
		computeUniqueValueJob.doJob(jobStatusModel);
		currentJob = computeUniqueValueJob;
	}
	
	@Override 
	public void removeDwcaResource(Map<SharedParameterEnum, Object> sharedParameters) {
		removeDwcaResourceJob.setSharedParameters(sharedParameters);
		removeDwcaResourceJob.doJob(jobStatusModel);
		currentJob = removeDwcaResourceJob;
	}
	
	@Override 
	public void publisherNameUpdate(Map<SharedParameterEnum, Object> sharedParameters) {
		publisherNameUpdateJob.setSharedParameters(sharedParameters);
		publisherNameUpdateJob.doJob(jobStatusModel);
		currentJob = publisherNameUpdateJob;
	}
	
	@Override
	public void removePublisher(Map<SharedParameterEnum, Object> sharedParameters) {
		removePublisherJob.setSharedParameters(sharedParameters);
		removePublisherJob.doJob(jobStatusModel);
		// currentJob = removePublisherJob;
	}

	/**
	 * C.G. This does nothing for now since the old code was working on buffer schema.
	 * Refreshing on buffer schema will not work unless we reharvest and reharvesting will update
	 * the resource data in occurrence tables.
	 * Updates database after resource change.
	 */
	@Override
	public void refreshResource(int resourceId) {
		// updateJob.addToSharedParameters(SharedParameterEnum.RESOURCE_ID, resourceId);
		// JobStatusModel jobStatusModel = new JobStatusModel();
		// harvesterViewModel.encapsulateJobStatus(jobStatusModel);
		// updateJob.doJob(jobStatusModel);
		// currentJob = updateJob;
	}

	@Override
	@Transactional("publicTransactionManager")
	public List<DwcaResourceModel> getResourceModelList() {
		Criteria criteria = sessionFactory.getCurrentSession().createCriteria(DwcaResourceModel.class);
		// Return results in alphabetical order
		criteria.addOrder(Order.asc("name"));
		return criteria.list();
	}
	
	@Override
	@Transactional("publicTransactionManager")
	public List<PublisherModel> getPublisherModelList() {
		return publisherDAO.loadPublishers();
	}

	@Transactional("publicTransactionManager")
	@Override
	public boolean updateResourceModel(DwcaResourceModel resourceModel) {
		return resourceDAO.save(resourceModel);
	}

	@Transactional("publicTransactionManager")
	@Override
	public boolean updatePublisherModel(PublisherModel publisherModel) {
		return publisherDAO.save(publisherModel);
	}

	/**
	 * Get the sorted ImportLogModel list using our own session. Sorted by desc
	 * event_date
	 *
	 * @return
	 */
	@Override
	@SuppressWarnings("unchecked")
	@Transactional("publicTransactionManager")
	public List<ImportLogModel> getSortedImportLogModelList() {
		Criteria criteria = sessionFactory.getCurrentSession().createCriteria(ImportLogModel.class);
		criteria.addOrder(Order.desc("event_end_date_time"));
		return criteria.list();
	}

	@Override
	public void onNodeError() {
		// stop the current job
		currentJob.cancel();
	}

	@Override
	@Transactional("publicTransactionManager")
	public List<IPTFeedModel> getIPTFeed() {

		URL mainIPTUrl = null;
		try {
			mainIPTUrl = new URL(harvesterConfig.getIptRssAddress());
		}
		catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		}
		return iptFeedDAO.getIPTFeed(mainIPTUrl);
	}

	@Override
	public List<DwcaResourceStatusModel> getResourceToHarvest() {
		return resourceStatusChecker.getHarvestRequiredList();
	}

	@Override
	public JobType getJobType(String jobId) {
		if (importDwcaJob != null) {
			if (jobId.equals(importDwcaJob.getJobId())) {
				return JobType.IMPORT_DWC;
			}
		}

		if (moveToPublicSchemaJob != null) {
			if (jobId.equals(moveToPublicSchemaJob.getJobId())) {
				return JobType.MOVE_TO_PUBLIC;
			}
		}

		if (computeUniqueValueJob != null) {
			if (jobId.equals(computeUniqueValueJob.getJobId())) {
				return JobType.COMPUTE_UNIQUE;
			}
		}
		return JobType.GENERIC_JOB;
	}

	/**
	 * Responsible to decide what to do next once a job is completed.
	 *
	 * @param jobId
	 * @param jobStatus
	 */
	private void onJobCompleted(String jobId, JobStatus jobStatus) {

		if (JobStatus.DONE == jobStatus) {
			JobType jobType = getJobType(jobId);
			switch (jobType) {
				case IMPORT_DWC:
					DwcaResourceModel dwcaResourceModel = (DwcaResourceModel) importDwcaJob
					.getFromSharedParameters(SharedParameterEnum.RESOURCE_MODEL);
					if (moveToPublicSchema) {
						moveToPublicSchema(dwcaResourceModel.getId(), computeUniqueValues);
						moveToPublicSchema = false;
					}
					break;
				case MOVE_TO_PUBLIC:
					if (computeUniqueValues) {
						computeUniqueValues();
						computeUniqueValues = false;
					}
					break;
				default:
					break;
			}
		}
	}

	/**
	 * PropertyChangeListener to handle job statuses and propagate changes to JobStatusModel.
	 *
	 * @author cgendreau
	 *
	 */
	private class JobStatusModelListener implements PropertyChangeListener {
		@Override
		public void propertyChange(PropertyChangeEvent evt) {
			// let the view(s) know
			harvesterViewModel.propagate(evt);

			JobStatusModel jsm = (JobStatusModel) evt.getSource();
			if (JobStatusModel.CURRENT_STATUS_PROPERTY.equals(evt.getPropertyName())) {
				JobStatus newStatus = (JobStatus) evt.getNewValue();
				if (newStatus.isJobCompleted()) {
					onJobCompleted(jsm.getCurrentJobId(), newStatus);
				}
			}
		}
	}
}
