package net.canadensys.harvester.occurrence.view;

import java.awt.Dimension;
import java.util.List;
import java.util.Vector;

import javax.swing.JDialog;

import net.canadensys.harvester.occurrence.model.IPTFeedModel;

public class IPTFeedDialog extends AbstractTableBasedDialog{
	
	private static final long serialVersionUID = -2655708810547434951L;

	public IPTFeedDialog(Vector<String> headers){
		super(headers);
		
		this.setTitle(Messages.getString("iptRssFeed.title"));
		this.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		mainPanel.setPreferredSize(new Dimension(800, 400));
	}
	
	public void loadData(List<IPTFeedModel> feedList){
		Vector<Vector<Object>> rowData = new Vector<Vector<Object>>();
		for (IPTFeedModel currIPTFeedModel : feedList) {
			Vector<Object> row = new Vector<Object>();
			row.add(currIPTFeedModel.getTitle());
			row.add(currIPTFeedModel.getLink());
			row.add(currIPTFeedModel.getUri());
			row.add(currIPTFeedModel.getPublishedDate());
			rowData.add(row);
		}
		internalLoadData(rowData);
	}
}
