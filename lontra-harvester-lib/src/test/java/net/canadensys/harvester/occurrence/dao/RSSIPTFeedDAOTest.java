package net.canadensys.harvester.occurrence.dao;

import static org.junit.Assert.assertEquals;

import java.net.URL;
import java.util.List;

import net.canadensys.harvester.occurrence.dao.impl.RSSIPTFeedDAO;
import net.canadensys.harvester.occurrence.mock.MockSharedParameters;
import net.canadensys.harvester.occurrence.model.IPTFeedModel;

import org.junit.Test;

/**
 * Test RSSIPTFeedDAO to ensure we can read and extract information from an IPT RSS xml file.
 *
 * @author cgendreau
 *
 */
public class RSSIPTFeedDAOTest {

	@Test
	public void testGetIPTFeed() {

		URL iptRssFile = this.getClass().getResource("/ipt_rss.xml");
		RSSIPTFeedDAO rssIPTFeedDAO = new RSSIPTFeedDAO();

		List<IPTFeedModel> feedModelList = rssIPTFeedDAO.getIPTFeed(iptRssFile);

		assertEquals(1, feedModelList.size());
		assertEquals(MockSharedParameters.QMOR_PACKAGE_ID, feedModelList.get(0).extractGbifPackageId());
	}

}
