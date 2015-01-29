package net.canadensys.harvester.occurrence.view;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.dataportal.occurrence.model.PublisherModel;
import net.canadensys.harvester.occurrence.controller.StepControllerIF;

/**
 * Publihsers panel for tabbed pane
 * 
 * @author Pedro Guimarães
 * 
 */
public class PublishersPanel extends JPanel {

	private static final long serialVersionUID = 983475983470237450L;

	private List<PublisherModel> publishers = null;

	// Inherited from OccurrenceHarvesterMainView:
	private StepControllerIF stepController;

	public JButton addPublisherBtn = null;
	public JTable publishersList = null;
	
	public Vector<String> publishersTableHeaders = null; 
	public Vector<String> resourcesTableHeaders = null;

	public PublishersPanel(StepControllerIF stepController) {
		this.stepController = stepController;
		// Fetch list of available publishers from the database:
		publishers = stepController.getPublisherModelList();
		this.setLayout(new GridBagLayout());
		initPublishersTableHeader();
		
		// Vertical alignment reference index:
		int lineIdx = 0;
		GridBagConstraints c = null;

		c = new GridBagConstraints();
		// Define padding:
		c.insets = new Insets(5, 5, 5, 5);
		// Define grid width:
		c.gridwidth = 3;
		c.gridx = 0;
		c.gridy = lineIdx++;
		c.anchor = GridBagConstraints.NORTHWEST;
		this.add(new JLabel(Messages.getString("view.info.publishers")), c);

		// Publishers table:
		c.gridy = lineIdx++;
		JTable table = initTable();
		// Set preferred column sizes:
		//table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		table.getColumnModel().getColumn(0).setPreferredWidth(30);
		table.getColumnModel().getColumn(1).setPreferredWidth(620);
		table.getColumnModel().getColumn(2).setPreferredWidth(100);
		// Center string contents for columns id and record count:
		DefaultTableCellRenderer centerRenderer = new DefaultTableCellRenderer();
		centerRenderer.setHorizontalAlignment( JLabel.CENTER );
		table.getColumnModel().getColumn(0).setCellRenderer( centerRenderer );
		table.getColumnModel().getColumn(2).setCellRenderer( centerRenderer );
		
		this.add(new JScrollPane(table), c);
		this.setVisible(true);
		
		// Add publisher button:
		c.gridx = 2;
		c.gridy = lineIdx++;
		c.anchor = GridBagConstraints.EAST;
		addPublisherBtn = new JButton(Messages.getString("view.button.add.publisher"));
		addPublisherBtn.setToolTipText(Messages.getString("view.button.add.publisher.tooltip"));
		addPublisherBtn.setEnabled(true);
		addPublisherBtn.setVisible(true);
		addPublisherBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onAddPublisher();
			}
		});
		this.add(addPublisherBtn, c);
		
		// Resources label:
		c.gridy = lineIdx++;
		c.anchor = GridBagConstraints.NORTHWEST;
		this.add(new JLabel(Messages.getString("view.info.publishers.resources")), c);

		
		// Resources table, used to display a selected publishers' resources:
		c.gridy = lineIdx++;
		Vector<Vector<Object>> emptyVectors = new Vector<Vector<Object>>();
		Vector<Object> emptyVector = new Vector<Object>();
		emptyVector.add("");
		emptyVectors.add(emptyVector);
		JTable resourcesTable = new JTable(emptyVectors, resourcesTableHeaders) {
			/**
			 * Remove table edition
			 * @see javax.swing.JTable#isCellEditable(int, int)
			 */
			public boolean isCellEditable(int rowIndex, int vColIndex) {
				return false;
			}
			/**
			 * Defines the table's preferred size:
			 */
			@Override
			public Dimension getPreferredScrollableViewportSize() 
			{
			    int width = 750;
			    int height = 150;
			    return new Dimension(width, height);
			}
		};
		this.add(new JScrollPane(resourcesTable), c);
		this.setVisible(true);
	}

	/**
	 * Receives the list of available publishers and returns a vector with a
	 * vector of objects that have the publisher's id, name and record count
	 * 
	 * @param publisherModelList
	 * @return
	 */
	public Vector<Vector<Object>> loadData(
			List<PublisherModel> publisherModelList) {
		Vector<Vector<Object>> rowData = new Vector<Vector<Object>>();
		for (PublisherModel currPublisherModel : publisherModelList) {
			Vector<Object> row = new Vector<Object>();
			row.add(currPublisherModel.getAuto_id());
			row.add(currPublisherModel.getName());
			row.add(currPublisherModel.getRecord_count());
			rowData.add(row);
		}
		return rowData;
	}
	
	/**
	 * Initializes the vector with header information for publishers table
	 */
	private void initPublishersTableHeader() {
		publishersTableHeaders = new Vector<String>();
		publishersTableHeaders.add(Messages.getString("view.publishers.id"));
		publishersTableHeaders.add(Messages.getString("view.publishers.name"));
		publishersTableHeaders.add(Messages.getString("view.publishers.record.count"));
	}
	
	/**
	 * Initializes the vector with header information for publishers table
	 */
	private void initResourcesTableHeader() {
		resourcesTableHeaders = new Vector<String>();
		resourcesTableHeaders.add(Messages.getString("view.resources.id"));
		resourcesTableHeaders.add(Messages.getString("view.resources.name"));
		resourcesTableHeaders.add(Messages.getString("view.resources.record.count"));
	}
	
	/**
	 * Creates a table based on the publishers available in the publishers' list
	 * @return
	 */
	private JTable initTable() {
		JTable table = new JTable(loadData(publishers), publishersTableHeaders) {
			/**
			 * Remove table edition
			 * @see javax.swing.JTable#isCellEditable(int, int)
			 */
			public boolean isCellEditable(int rowIndex, int vColIndex) {
				return false;
			}
			/**
			 * Defines the table's preferred size:
			 */
			@Override
			public Dimension getPreferredScrollableViewportSize() 
			{
			    int width = 750;
			    int height = 200;
			    return new Dimension(width, height);
			}
		};
		return table;
	}
	/**
	 * Add publisher button action
    */
	private void onAddPublisher() {
		AddPublisherDialog apd = new AddPublisherDialog(this);
		PublisherModel publisherModel = new PublisherModel();
		publisherModel = apd.displayPublisher(publisherModel);
		// Case the user hit OK:
		if (apd.getExitValue() == JOptionPane.OK_OPTION) {
			// Save or upload publisher:
			if (!stepController.updatePublisherModel(publisherModel)) {
				JOptionPane.showMessageDialog(this, Messages.getString("resourceView.resource.error.save.msg"),
						Messages.getString("resourceView.resource.error.title"), JOptionPane.ERROR_MESSAGE);
			}
			// reload data to ensure we have the latest changes
			publishers = stepController.getPublisherModelList();
		}
	}


	/**
	 * Rebuild the publishers' table if the list of available publishers is changed.
	 */
	public void updatePublishersTable() {
		publishersList = initTable();
	}
}
