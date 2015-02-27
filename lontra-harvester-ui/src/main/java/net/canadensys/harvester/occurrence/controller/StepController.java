package net.canadensys.harvester.occurrence.controller;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import net.canadensys.dataportal.occurrence.dao.DwcaResourceDAO;
import net.canadensys.dataportal.occurrence.dao.PublisherDAO;
import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.dataportal.occurrence.model.ImportLogModel;
import net.canadensys.dataportal.occurrence.model.PublisherModel;
import net.canadensys.harvester.AbstractProcessingJob;
import net.canadensys.harvester.ItemProgressListenerIF;
import net.canadensys.harvester.config.harvester.HarvesterConfigIF;
import net.canadensys.harvester.jms.control.JMSControlProducer;
import net.canadensys.harvester.message.control.VersionControlMessage;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.dao.IPTFeedDAO;
import net.canadensys.harvester.occurrence.job.ComputeUniqueValueJob;
import net.canadensys.harvester.occurrence.job.ImportDwcaJob;
import net.canadensys.harvester.occurrence.job.MoveToPublicSchemaJob;
import net.canadensys.harvester.occurrence.job.UpdateJob;
import net.canadensys.harvester.occurrence.model.IPTFeedModel;
import net.canadensys.harvester.occurrence.model.JobStatusModel;
import net.canadensys.harvester.occurrence.notification.ResourceStatusNotifierIF;
import net.canadensys.harvester.occurrence.view.model.HarvesterViewModel;

import org.hibernate.Criteria;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.transaction.annotation.Transactional;

/**
 * Main controller to initiate jobs.
 * This controller is NOT thread safe.
 * 
 * @author canadensys
 * 
 */
public class StepController implements StepControllerIF {

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
	private ResourceStatusNotifierIF notifier;

	@Autowired
	private ImportDwcaJob importDwcaJob;

	@Autowired
	private MoveToPublicSchemaJob moveToPublicSchemaJob;

	@Autowired
	private UpdateJob updateJob;

	@Autowired
	private ComputeUniqueValueJob computeUniqueValueJob;

	@Autowired
	private HarvesterViewModel harvesterViewModel;

	@Autowired
	private JMSControlProducer controlMessageProducer;

	@Autowired
	private NodeStatusController nodeStatusController;

	private AbstractProcessingJob currentJob;

	public StepController() {
	}

	@Override
	public void registerProgressListener(ItemProgressListenerIF progressListener) {
		importDwcaJob.setItemProgressListener(progressListener);
	}

	/**
	 * FIXME we should NOT reuse the importDwcaJob, it is not an immutable class
	 */
	@Override
	public void importDwcA(Integer resourceId) {
		controlMessageProducer.open();
		// send the app verion
		controlMessageProducer.publish(new VersionControlMessage(currentVersion));
		controlMessageProducer.close();

		// enable node status controller
		nodeStatusController.start();
		importDwcaJob.addToSharedParameters(SharedParameterEnum.RESOURCE_ID, resourceId);
		currentJob = importDwcaJob;

		JobStatusModel jobStatusModel = new JobStatusModel();
		harvesterViewModel.encapsulateJobStatus(jobStatusModel);
		importDwcaJob.doJob(jobStatusModel);
	}

	@Override
	public void importDwcAFromLocalFile(String dwcaFilePath) {
		// enable node status controller
		nodeStatusController.start();
		importDwcaJob.addToSharedParameters(SharedParameterEnum.DWCA_PATH, dwcaFilePath);
		currentJob = importDwcaJob;

		JobStatusModel jobStatusModel = new JobStatusModel();
		harvesterViewModel.encapsulateJobStatus(jobStatusModel);
		importDwcaJob.doJob(jobStatusModel);
	}

	@Override
	public void moveToPublicSchema(Integer resourceID, String resourceName, String publisherName, boolean computeUniqueValues) {
		moveToPublicSchemaJob.addToSharedParameters(SharedParameterEnum.RESOURCE_ID, resourceID);
		moveToPublicSchemaJob.addToSharedParameters(SharedParameterEnum.RESOURCE_NAME, resourceName);
		moveToPublicSchemaJob.addToSharedParameters(SharedParameterEnum.PUBLISHER_NAME, publisherName);
		JobStatusModel jobStatusModel = new JobStatusModel();
		harvesterViewModel.encapsulateJobStatus(jobStatusModel);
		moveToPublicSchemaJob.doJob(jobStatusModel);
		currentJob = moveToPublicSchemaJob;
		if (computeUniqueValues) {
			computeUniqueValues(jobStatusModel);
		}
	}

	/**
	 * Updates database after resource change.
	 */
	@Override
	@Transactional("bufferTransactionManager")
	public void updateStep(String resourceUuid, String resourceName, String publisherName) {
		updateJob.addToSharedParameters(SharedParameterEnum.RESOURCE_UUID, resourceUuid);
		updateJob.addToSharedParameters(SharedParameterEnum.RESOURCE_NAME, resourceName);
		updateJob.addToSharedParameters(SharedParameterEnum.PUBLISHER_NAME, publisherName);
		JobStatusModel jobStatusModel = new JobStatusModel();
		harvesterViewModel.encapsulateJobStatus(jobStatusModel);
		updateJob.doJob(jobStatusModel);
		currentJob = updateJob;
	}

	@Override
	@Transactional("publicTransactionManager")
	public List<DwcaResourceModel> getResourceModelList() {
		return resourceDAO.loadResources();
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
	public List<DwcaResourceModel> getResourceToHarvest() {
		return notifier.getHarvestRequiredList();
	}

	@Override
	public void computeUniqueValues(JobStatusModel jobStatusModel) {
		if (jobStatusModel == null) {
			jobStatusModel = new JobStatusModel();
			harvesterViewModel.encapsulateJobStatus(jobStatusModel);
		}
		computeUniqueValueJob.doJob(jobStatusModel);
		currentJob = computeUniqueValueJob;
	}
}
