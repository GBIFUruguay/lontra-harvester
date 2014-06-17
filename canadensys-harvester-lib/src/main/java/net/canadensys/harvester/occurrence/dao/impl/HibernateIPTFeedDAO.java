package net.canadensys.harvester.occurrence.dao.impl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import net.canadensys.harvester.occurrence.dao.IPTFeedDAO;
import net.canadensys.harvester.occurrence.model.IPTFeedModel;

import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;

public class HibernateIPTFeedDAO implements IPTFeedDAO{
	
	@Override
	public List<IPTFeedModel> getIPTFeed(URL iptFeedURL) {
		List<IPTFeedModel> feedList = new ArrayList<IPTFeedModel>();
		SyndFeedInput input = new SyndFeedInput();
		try {
			SyndFeed feed = input.build(new XmlReader(iptFeedURL));
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
}
