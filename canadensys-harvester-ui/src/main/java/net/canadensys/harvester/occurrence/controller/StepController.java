package net.canadensys.harvester.occurrence.controller;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import net.canadensys.harvester.AbstractProcessingJob;
import net.canadensys.harvester.ItemProgressListenerIF;
import net.canadensys.harvester.config.harvester.HarvesterConfigIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.job.ComputeUniqueValueJob;
import net.canadensys.harvester.occurrence.job.ImportDwcaJob;
import net.canadensys.harvester.occurrence.job.MoveToPublicSchemaJob;
import net.canadensys.harvester.occurrence.model.IPTFeedModel;
import net.canadensys.harvester.occurrence.model.ImportLogModel;
import net.canadensys.harvester.occurrence.model.JobStatusModel;
import net.canadensys.harvester.occurrence.model.ResourceModel;
import net.canadensys.harvester.occurrence.view.model.HarvesterViewModel;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;

/**
 * Main controller to initiate jobs.
 * This controller is NOT thread safe.
 * @author canadensys
 *
 */
@Component("stepController")
public class StepController implements StepControllerIF {

	@Autowired
	private HarvesterConfigIF harvesterConfig;

	@Autowired
	@Qualifier(value="publicSessionFactory")
	private SessionFactory sessionFactory;

	@Autowired
	private ImportDwcaJob importDwcaJob;

	@Autowired
	private MoveToPublicSchemaJob moveToPublicSchemaJob;

	@Autowired
	private ComputeUniqueValueJob computeUniqueValueJob;

	@Autowired
	private HarvesterViewModel harvesterViewModel;

	@Autowired
	private NodeStatusController nodeStatusController;

	private AbstractProcessingJob currentJob;

	public StepController(){}

	@Override
	public void registerProgressListener(ItemProgressListenerIF progressListener){
		importDwcaJob.setItemProgressListener(progressListener);
	}

	@Override
	public void importDwcA(Integer resourceId){
		//enable node status controller
		nodeStatusController.start();
		importDwcaJob.addToSharedParameters(SharedParameterEnum.RESOURCE_ID, resourceId);
		currentJob = importDwcaJob;

		JobStatusModel jobStatusModel = new JobStatusModel();
		harvesterViewModel.encapsulateJobStatus(jobStatusModel);
		importDwcaJob.doJob(jobStatusModel);
	}

	@Override
	public void importDwcAFromLocalFile(String dwcaFilePath){
		//enable node status controller
		nodeStatusController.start();
		importDwcaJob.addToSharedParameters(SharedParameterEnum.DWCA_PATH, dwcaFilePath);
		currentJob = importDwcaJob;

		JobStatusModel jobStatusModel = new JobStatusModel();
		harvesterViewModel.encapsulateJobStatus(jobStatusModel);
		importDwcaJob.doJob(jobStatusModel);
	}

	@Override
	public void moveToPublicSchema(String datasetShortName){
		moveToPublicSchemaJob.addToSharedParameters(SharedParameterEnum.DATASET_SHORTNAME, datasetShortName);
		moveToPublicSchemaJob.doJob();
		currentJob = moveToPublicSchemaJob;

		computeUniqueValueJob.doJob();
		currentJob = computeUniqueValueJob;
	}


	@Override
	@SuppressWarnings("unchecked")
	@Transactional("publicTransactionManager")
	public List<ResourceModel> getResourceModelList(){
		Criteria searchCriteria = sessionFactory.getCurrentSession().createCriteria(ResourceModel.class);
		return searchCriteria.list();
	}

	@Transactional("publicTransactionManager")
	@Override
	public boolean updateResourceModel(ResourceModel resourceModel) {
		try{
			sessionFactory.getCurrentSession().saveOrUpdate(resourceModel);
		}
		catch(HibernateException hEx){
			hEx.printStackTrace();
			return false;
		}
		return true;
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

	/**
	 * Get the list of IPTFeedModel from an IPT installation RSS feed.
	 * @param feedURL
	 * @return
	 */
	@Override
	public List<IPTFeedModel> getIPTFeed() {
		List<IPTFeedModel> feedList = new ArrayList<IPTFeedModel>();
		SyndFeedInput input = new SyndFeedInput();
		try {
			SyndFeed feed = input.build(new XmlReader(new URL(harvesterConfig.getIptRssAddress())));
			List<SyndEntry> feedEntries = feed.getEntries();
			for (SyndEntry currEntry : feedEntries) {
				IPTFeedModel feedModel = new IPTFeedModel();
				feedModel.setTitle(currEntry.getTitle());
				feedModel.setUri(currEntry.getUri());
				feedModel.setLink(currEntry.getLink());
				feedModel.setPublishedDate(currEntry.getPublishedDate());
				feedList.add(feedModel);
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (FeedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return feedList;
	}

	@Override
	public void onNodeError(){
		//stop the current job
		currentJob.cancel();
	}
}
