package net.canadensys.harvester.occurrence.view;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.List;
import java.util.Vector;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import net.canadensys.dataportal.occurrence.model.ImportLogModel;

/**
 * Dialog displaying the previous import log in a table.
 * 
 * @author canadensys
 * 
 */
public class ImportLogPanel extends JPanel {

	private static final long serialVersionUID = 5963921652318603960L;

	public ImportLogPanel(Vector<String> headers, List<ImportLogModel> importLogModelList) {
		this.setLayout(new GridBagLayout());
		GridBagConstraints c = new GridBagConstraints();
		c.anchor = GridBagConstraints.CENTER;
		JTable table = new JTable(loadData(importLogModelList), headers) {
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
		this.add(new JScrollPane(table));
		this.setVisible(true);
	}

	public Vector<Vector<Object>> loadData(List<ImportLogModel> importLogModelList) {
		Vector<Vector<Object>> rowData = new Vector<Vector<Object>>();
		for (ImportLogModel currImportLogModel : importLogModelList) {
			Vector<Object> row = new Vector<Object>();
			row.add(currImportLogModel.getSourcefileid());
			row.add(currImportLogModel.getRecord_quantity());
			row.add(currImportLogModel.getUpdated_by());
			row.add(currImportLogModel.getEvent_end_date_time());
			rowData.add(row);
		}
		return rowData;
	}
}
