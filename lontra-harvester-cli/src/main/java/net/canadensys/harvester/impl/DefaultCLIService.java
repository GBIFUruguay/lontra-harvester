package net.canadensys.harvester.impl;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.canadensys.dataportal.occurrence.dao.DwcaResourceDAO;
import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.harvester.AbstractProcessingJob;
import net.canadensys.harvester.CLIService;
import net.canadensys.harvester.model.CliOption;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.model.JobStatusModel;
import net.canadensys.harvester.occurrence.model.JobStatusModel.JobStatus;

import org.apache.commons.compress.utils.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

/**
 * This class is NOT thread-safe
 * 
 * @author cgendreau
 *
 */
public class DefaultCLIService implements CLIService {

	@Autowired
	private AbstractProcessingJob importDwcaJob;

	@Autowired
	private AbstractProcessingJob moveToPublicSchemaJob;

	@Autowired
	private AbstractProcessingJob computeUniqueValueJob;

	@Autowired
	private DwcaResourceDAO resourceDAO;

	private final JobStatusModel jobStatusModel;

	public DefaultCLIService() {
		jobStatusModel = new JobStatusModel();
		jobStatusModel.addPropertyChangeListener(new JobStatusModelListener());
	}

	@Override
	@Transactional("publicTransactionManager")
	public DwcaResourceModel loadResourceModel(String resourceIdentifier) {
		int resourceId = -1;
		DwcaResourceModel resourceModel = null;

		// check if the argument represents the resourceid
		try {
			resourceId = Integer.parseInt(resourceIdentifier);
		}
		catch (NumberFormatException ignoreEx) {
		}

		if (resourceId != -1) {
			resourceModel = resourceDAO.load(resourceId);
		}

		// try other possible identifiers
		if (resourceModel == null) {
			resourceModel = resourceDAO.loadByResourceUUID(resourceIdentifier);
		}
		if (resourceModel == null) {
			resourceModel = resourceDAO.loadBySourceFileId(resourceIdentifier);
		}
		return resourceModel;
	}

	@Override
	@Transactional("publicTransactionManager")
	public List<DwcaResourceModel> loadResourceModelList() {
		List<DwcaResourceModel> resourceList = resourceDAO.loadResources();
		Collections.sort(resourceList, new Comparator<DwcaResourceModel>() {
			@Override
			public int compare(DwcaResourceModel o1, DwcaResourceModel o2) {
				return o1.getId().compareTo(o2.getId());
			}
		});
		return resourceList;
	}

	@Override
	public void importDwca(DwcaResourceModel resourceModel, CliOption harvestOption) throws IOException {
		if (resourceModel != null) {
			ExecutorService executor = Executors.newFixedThreadPool(2);
			importDwcaJob.addToSharedParameters(SharedParameterEnum.RESOURCE_ID, resourceModel.getId());

			// exclusion list handling
			if (StringUtils.isNotBlank(harvestOption.getExclusionFilePath())) {
				List<String> exclusionList = loadExclusionList(harvestOption.getExclusionFilePath());
				if (exclusionList != null && exclusionList.size() > 0) {
					System.out.println(exclusionList.size() + " Dwc-A Id to exclude");
					importDwcaJob.addToSharedParameters(SharedParameterEnum.DWCA_ID_EXCLUSION_LIST, exclusionList);
				}
			}

			Runnable importJobThread = new Runnable() {
				@Override
				public void run() {
					importDwcaJob.doJob(jobStatusModel);
				}
			};

			executor.execute(importJobThread);
			executor.shutdown();
			try {
				executor.awaitTermination(12, TimeUnit.HOURS);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		else {
			System.out.println("Nothing to import, resourceModel is null");
			return;
		}
	}

	@Override
	public void moveToPublicSchema(DwcaResourceModel resourceModel) {
		moveToPublicSchemaJob.addToSharedParameters(SharedParameterEnum.RESOURCE_ID, resourceModel.getId());
		moveToPublicSchemaJob.doJob(jobStatusModel);
	}

	@Override
	public void computeUniqueValueJob() {
		computeUniqueValueJob.doJob(jobStatusModel);
	}

	/**
	 * Load an exclusion list from a file. The list returned will include one element per line in the file.
	 * 
	 * @param exclusionFilePath
	 * @return all lines from the file, never null
	 * @throws IOException
	 */
	private List<String> loadExclusionList(String exclusionFilePath) throws IOException {
		File exclusionFile = new File(exclusionFilePath);
		if (!exclusionFile.exists()) {
			throw new FileNotFoundException(exclusionFilePath);
		}
		return FileUtils.readLines(exclusionFile, Charsets.UTF_8);
	}

	private void onJobCompleted(String jobId, JobStatus jobStatus) {

		// importJob completed
		if (jobId.equals(importDwcaJob.getJobId()) && JobStatus.DONE.equals(jobStatus)) {
			DwcaResourceModel dwcaResourceModel = (DwcaResourceModel) importDwcaJob.getFromSharedParameters(SharedParameterEnum.RESOURCE_MODEL);
			moveToPublicSchema(dwcaResourceModel);
		}
		// moveToPublicSchemaJob completed
		else if (jobId.equals(moveToPublicSchemaJob.getJobId()) && JobStatus.DONE.equals(jobStatus)) {
			computeUniqueValueJob();
		}
	}

	/**
	 * Simple PropertyChangeListener to send notifications about the JobStatusModel to the console.
	 * 
	 * @author cgendreau
	 * 
	 */
	private class JobStatusModelListener implements PropertyChangeListener {
		@Override
		public void propertyChange(PropertyChangeEvent evt) {
			JobStatusModel jsm = (JobStatusModel) evt.getSource();
			if (JobStatusModel.CURRENT_STATUS_PROPERTY.equals(evt.getPropertyName())) {
				JobStatus newStatus = (JobStatus) evt.getNewValue();
				if (JobStatus.DONE == newStatus || JobStatus.ERROR == newStatus) {
					onJobCompleted(jsm.getCurrentJobId(), newStatus);
				}
			}
			else {
				System.out.println(">" + evt.getNewValue());
			}
		}
	}

}
