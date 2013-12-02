package net.canadensys.processing.occurrence.model;

import java.util.Date;

/**
 * Model containing the information for a resource from the IPT RSS feed.
 * @author canadensys
 */
public class IPTFeedModel {
	
	private String title;
	private String link;
	private String uri;
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
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public Date getPublishedDate() {
		return publishedDate;
	}
	public void setPublishedDate(Date publishedDate) {
		this.publishedDate = publishedDate;
	}
}
