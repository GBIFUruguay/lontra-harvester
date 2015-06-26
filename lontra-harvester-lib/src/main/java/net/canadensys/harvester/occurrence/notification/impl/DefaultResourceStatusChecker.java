package net.canadensys.harvester.occurrence.notification.impl;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.canadensys.dataportal.occurrence.dao.DwcaResourceDAO;
import net.canadensys.dataportal.occurrence.dao.ImportLogDAO;
import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.dataportal.occurrence.model.ImportLogModel;
import net.canadensys.harvester.occurrence.dao.IPTFeedDAO;
import net.canadensys.harvester.occurrence.model.DwcaResourceStatusModel;
import net.canadensys.harvester.occurrence.model.IPTFeedModel;
import net.canadensys.harvester.occurrence.notification.ResourceStatusCheckerIF;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

/**
 * Default implementation of ResourceStatusCheckerIF that is using the IPT RSS
 * feed to get the last publication date. The date is then used to compare with
 * the last import event of each resources.
 *
 * @author cgendreau
 *
 */
public class DefaultResourceStatusChecker implements ResourceStatusCheckerIF {
	private static final Logger LOGGER = Logger.getLogger(DefaultResourceStatusChecker.class);
	private static final String IPT_RSS_SUFFIX = "/rss.do";

	// very simple cache to avoid parsing the RSS feed on each resources
	private final Map<String, List<IPTFeedModel>> feedModelByIPTAddress;

	@Autowired
	private DwcaResourceDAO resourceDAO;

	@Autowired
	private IPTFeedDAO iptFeedDAO;

	@Autowired
	private ImportLogDAO importLogDAO;

	public DefaultResourceStatusChecker() {
		feedModelByIPTAddress = new HashMap<String, List<IPTFeedModel>>();
	}

	@Override
	@Transactional("publicTransactionManager")
	public List<DwcaResourceStatusModel> getHarvestRequiredList() {

		List<DwcaResourceStatusModel> resourceToHarvest = new ArrayList<DwcaResourceStatusModel>();
		List<DwcaResourceModel> resourcesList = resourceDAO.loadResources();

		String iptAddress;
		URL iptURL;

		for (DwcaResourceModel currResource : resourcesList) {
			// we deduce the RSS feed address from the archive URL. This may become a problem in the future.
			iptAddress = StringUtils.substringBeforeLast(currResource.getArchive_url(), "/") + IPT_RSS_SUFFIX;
			if (!feedModelByIPTAddress.containsKey(iptAddress)) {
				try {
					iptURL = new URL(iptAddress);
					feedModelByIPTAddress.put(iptAddress, iptFeedDAO.getIPTFeed(iptURL));
				}
				catch (MalformedURLException e) {
					LOGGER.error("Can't build IPT RSS feed address", e);
				}
			}

			List<IPTFeedModel> feedEntryList = feedModelByIPTAddress.get(iptAddress);
			// String resourceLink;
			ImportLogModel lastImportLog;

			if (StringUtils.isNotBlank(currResource.getGbif_package_id())) {
				for (IPTFeedModel currFeed : feedEntryList) {
					// FIXME
					// strip the version from the URI
					// resourceKey = StringUtils.substringBeforeLast(currFeed.getUri(), "/");

					// resourceLink = currFeed.getLink();

					if (currResource.getGbif_package_id().equalsIgnoreCase(currFeed.extractGbifPackageId())) {
						lastImportLog = importLogDAO.loadLastFrom(currResource.getSourcefileid());
						DwcaResourceStatusModel dwcaResourceStatusModel = new DwcaResourceStatusModel(currResource);
						dwcaResourceStatusModel.setLastPublishedDate(currFeed.getPublishedDate());

						if (lastImportLog != null) {
							// compare date
							if (currFeed.getPublishedDate().after(lastImportLog.getEvent_end_date_time())) {
								dwcaResourceStatusModel.setLastHarvestDate(lastImportLog.getEvent_end_date_time());
								resourceToHarvest.add(dwcaResourceStatusModel);
							}
						}
						// if it was never imported, add it to the list
						else {
							resourceToHarvest.add(dwcaResourceStatusModel);
						}
					}
				}
			}
			else {
				LOGGER.warn("Resource [" + currResource.getSourcefileid() + "] doesn't have a key. Can't validate status.");
			}
		}
		return resourceToHarvest;
	}

}
