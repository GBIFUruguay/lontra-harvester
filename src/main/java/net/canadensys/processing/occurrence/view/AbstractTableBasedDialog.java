package net.canadensys.processing.occurrence.view;

import java.awt.Color;
import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;

/**
 * Abstraction of a table based view.
 * @author canadensys
 *
 */
public abstract class AbstractTableBasedDialog extends JDialog{
	
	private static final long serialVersionUID = 8010138101153686856L;

	protected Vector<String> headers = null;
	
	protected JPanel mainPanel;
	protected JTable table = null;
	protected JScrollPane scrollPane = null;
	protected JButton closeBtn = null;
	
	public AbstractTableBasedDialog(Vector<String> headers){
		this.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		this.setModal(true);
		this.headers = headers;
		mainPanel = new JPanel();
		GridBagConstraints c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=0;
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 1.0;
		c.weighty = 1.0;
		
		setLayout(new GridBagLayout());
		mainPanel.setLayout(new GridBagLayout());
		this.add(mainPanel,c);
	}
	
	protected void internalLoadData(Vector<Vector<Object>> rowData){
		mainPanel.removeAll();
		table = new JTable(rowData, headers);
		table.setGridColor(Color.GRAY);
		table.setDragEnabled(false);
		packTable(table);
		scrollPane = new JScrollPane(table);
		
		GridBagConstraints c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=0;
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 1.0;
		c.weighty = 1.0;
		mainPanel.add(scrollPane,c);
		closeBtn = new JButton(Messages.getString("view.button.close"));
		final JDialog it = this;
		closeBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				it.dispose();
			}
		});
		
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=1;
		mainPanel.add(closeBtn,c);
		pack();
	}
	
	private void packTable(JTable table){
		 int width = 0;
		 for(int col=0; col < table.getColumnCount(); col++){
			 width = table.getColumnModel().getColumn(col).getPreferredWidth();
			 for (int row = 0; row < table.getRowCount(); row++) {
			     TableCellRenderer renderer = table.getCellRenderer(row, col);
			     Component comp = table.prepareRenderer(renderer, row, col);
			     width = Math.max (comp.getPreferredSize().width, width);
			 }
			 table.getColumnModel().getColumn(col).setPreferredWidth(width);
		 }
	}
}
