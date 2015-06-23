package net.canadensys.harvester.occurrence.dao;

import java.net.URL;
import java.util.List;

import net.canadensys.harvester.occurrence.model.IPTFeedModel;

/**
 * Data access to IPT feed
 *
 * @author cgendreau
 *
 */
public interface IPTFeedDAO {

	/**
	 * Get the list of IPTFeedModel from an IPT installation.
	 *
	 * @param iptFeedURL
	 * @return
	 */
	public List<IPTFeedModel> getIPTFeed(URL iptFeedURL);
}
