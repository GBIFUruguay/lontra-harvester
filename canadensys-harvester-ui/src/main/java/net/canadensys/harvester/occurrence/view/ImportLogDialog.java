package net.canadensys.harvester.occurrence.view;

import java.awt.Dimension;
import java.util.List;
import java.util.Vector;

import net.canadensys.harvester.occurrence.model.ImportLogModel;

/**
 * Dialog displaying the previous import log in a table.
 * @author canadensys
 *
 */
public class ImportLogDialog extends AbstractTableBasedDialog {
	
	private static final long serialVersionUID = 5963921652318603960L;

	public ImportLogDialog(Vector<String> headers){
		super(headers);
		this.setTitle(Messages.getString("importLog.title"));
		mainPanel.setPreferredSize(new Dimension(600, 400));
	}
	
	public void loadData(List<ImportLogModel> importLogModelList){
		Vector<Vector<Object>> rowData = new Vector<Vector<Object>>();
		for (ImportLogModel currImportLogModel : importLogModelList) {
			Vector<Object> row = new Vector<Object>();
			row.add(currImportLogModel.getSourcefileid());
			row.add(currImportLogModel.getRecord_quantity());
			row.add(currImportLogModel.getUpdated_by());
			row.add(currImportLogModel.getEvent_end_date_time());
			rowData.add(row);
		}
		internalLoadData(rowData);
	}

}
