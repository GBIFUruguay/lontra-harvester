package net.canadensys.harvester.occurrence.view;

import java.util.List;
import java.util.Vector;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import net.canadensys.harvester.occurrence.model.IPTFeedModel;

/**
 * Display IPT RSS feed information.
 * 
 * @author canadensys
 * 
 */
public class IPTFeedDialog extends JPanel {

	private static final long serialVersionUID = -2655708810547434951L;

	public IPTFeedDialog(Vector<String> headers, List<IPTFeedModel> iptFeedModelList) {
		JTable table = new JTable(loadData(iptFeedModelList), headers);
		this.add(new JScrollPane(table));
		this.setVisible(true);
	}

	public Vector<Vector<Object>> loadData(List<IPTFeedModel> iptFeedModelList) {
		Vector<Vector<Object>> rowData = new Vector<Vector<Object>>();
		for (IPTFeedModel currIPTFeedModel : iptFeedModelList) {
			Vector<Object> row = new Vector<Object>();
			row.add(currIPTFeedModel.getTitle());
			row.add(currIPTFeedModel.getLink());
			row.add(currIPTFeedModel.getUri());
			row.add(currIPTFeedModel.getPublishedDate());
			rowData.add(row);
		}
		return rowData;
	}
}
