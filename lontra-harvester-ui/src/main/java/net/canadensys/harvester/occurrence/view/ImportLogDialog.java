package net.canadensys.harvester.occurrence.view;

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
public class ImportLogDialog extends JPanel {

	private static final long serialVersionUID = 5963921652318603960L;

	public ImportLogDialog(Vector<String> headers, List<ImportLogModel> importLogModelList) {
		JTable table = new JTable(loadData(importLogModelList), headers);
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
