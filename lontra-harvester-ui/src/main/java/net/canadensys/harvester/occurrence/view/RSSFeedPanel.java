package net.canadensys.harvester.occurrence.view;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
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
public class RSSFeedPanel extends JPanel {

	private static final long serialVersionUID = -2655708810547434951L;

	public RSSFeedPanel(Vector<String> headers, List<IPTFeedModel> iptFeedModelList) {
		
		// Vertical alignment reference index:
		this.setLayout(new GridBagLayout());
		GridBagConstraints c = new GridBagConstraints();
		c.anchor = GridBagConstraints.CENTER;
		JTable table = new JTable(loadData(iptFeedModelList), headers) {
            // Remove table edition:
            public boolean isCellEditable(int rowIndex, int vColIndex) {
                    return false;
            }
            @Override
			public Dimension getPreferredScrollableViewportSize() 
			{
			    int width = 750;
			    int height = 480;
			    return new Dimension(width, height);
			}
		};
		this.add(new JScrollPane(table), c);
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
