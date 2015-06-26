package net.canadensys.harvester.occurrence.model;

import java.util.Date;

import org.apache.commons.lang3.StringUtils;

/**
 * Model containing the information for a resource from the IPT RSS feed.
 *
 * @author canadensys
 */
public class IPTFeedModel {

	private String title;
	private String link;
	private String guid;
	private Date publishedDate;

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	public Date getPublishedDate() {
		return publishedDate;
	}

	public void setPublishedDate(Date publishedDate) {
		this.publishedDate = publishedDate;
	}

	/**
	 * Extract the identifier used by GBIF to identify the package without the version.
	 *
	 * @return
	 */
	public String extractGbifPackageId() {
		return StringUtils.substringBeforeLast(guid, "/");
	}

	public String getGuid() {
		return guid;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}
}
