package net.canadensys.harvester.occurrence.view;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.dataportal.occurrence.model.PublisherModel;
import net.canadensys.harvester.occurrence.controller.StepControllerIF;

/**
 * Publishers panel for tabbed pane
 * 
 * @author Pedro Guimarães
 * 
 */
public class PublishersPanel extends JPanel {

	private static final long serialVersionUID = 983475983470237450L;

	private List<PublisherModel> publishers = null;
	private List<DwcaResourceModel> resources = null;

	// Inherited from OccurrenceHarvesterMainView:
	private final StepControllerIF stepController;

	private JButton addPublisherBtn = null;
	private JTable publishersList = null;
	private JTable resourcesList = null;
	private DefaultTableModel publishersTableModel = null;
	private DefaultTableModel resourcesTableModel = null;

	public Vector<String> publishersTableHeaders = null;
	public Vector<String> resourcesTableHeaders = null;

	/**
	 * Main constructor, receives StepController and builds panel components
	 * 
	 * @param stepController
	 */
	public PublishersPanel(StepControllerIF stepController) {
		this.stepController = stepController;
		// Fetch list of available publishers from the database:
		publishers = stepController.getPublisherModelList();
		this.setLayout(new GridBagLayout());

		// Initialize table headers:
		initPublishersTableHeader();
		initResourcesTableHeader();

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
		initPublishersTable();
		this.add(new JScrollPane(publishersList), c);

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
		initResourcesTable();
		this.add(new JScrollPane(resourcesList), c);

		// Habilitate the panel:
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
		resourcesTableHeaders.add(Messages.getString("view.publishers.resources.id"));
		resourcesTableHeaders.add(Messages.getString("view.publishers.resources.name"));
		resourcesTableHeaders.add(Messages.getString("view.publishers.resources.record.count"));
	}

	/**
	 * Creates a table based on the publishers available in the publishers' list
	 * 
	 * @return
	 */
	private void initPublishersTable() {
		if (publishersList == null) {
			publishersList = new JTable() {
				/**
				 * Remove table edition
				 * 
				 * @see javax.swing.JTable#isCellEditable(int, int)
				 */
				@Override
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
			// Set single selection mode only:
			publishersList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

			publishersList.addMouseListener(new MouseAdapter() {
				@Override
				public void mouseClicked(MouseEvent e) {
					if (e.getClickCount() == 1)
						onSelectedPublisher(publishersList.getSelectedRow(), publishersList.getSelectedColumn());
				}
			});
		}

		// Initialize TableModel:
		initPublishersTableModel();

		// Add rows in the table model for each publisher:
		for (PublisherModel p : publishers) {
			Vector<String> aux = new Vector<String>();
			aux.add(p.getAuto_id().toString());
			aux.add(p.getName());
			aux.add(p.getRecord_count().toString());
			publishersTableModel.addRow(aux);
		}
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
				JOptionPane.showMessageDialog(this, Messages.getString("publisherView.publisher.error.save.msg"),
						Messages.getString("publisherView.publisher.error.title"), JOptionPane.ERROR_MESSAGE);
			}
			else {
				JOptionPane.showMessageDialog(this, Messages.getString("publisherView.publisher.success.save.msg"),
						Messages.getString("publisherView.publisher.success.title"), JOptionPane.INFORMATION_MESSAGE);
			}
			// reload data to ensure we have the latest changes:
			publishers = stepController.getPublisherModelList();
			// refresh publisher table to display new publisher:
			updatePublishersTable();
		}
	}

	/**
	 * Rebuild the publishers' table if the list of available publishers is changed.
	 */
	public void updatePublishersTable() {
		initPublishersTable();
	}

	/**
	 * Initialize the TableModel, cleaning the table items.
	 */
	public void initPublishersTableModel() {
		// Init tableModel:
		publishersTableModel = new DefaultTableModel();
		publishersTableModel.setColumnIdentifiers(publishersTableHeaders);

		publishersList.setModel(publishersTableModel);

		// Set preferred column sizes:
		publishersList.getColumnModel().getColumn(0).setPreferredWidth(30);
		publishersList.getColumnModel().getColumn(1).setPreferredWidth(620);
		publishersList.getColumnModel().getColumn(2).setPreferredWidth(100);

		// Center string contents for columns id and record count:
		DefaultTableCellRenderer centerRenderer = new DefaultTableCellRenderer();
		centerRenderer.setHorizontalAlignment(JLabel.CENTER);
		publishersList.getColumnModel().getColumn(0).setCellRenderer(centerRenderer);
		publishersList.getColumnModel().getColumn(2).setCellRenderer(centerRenderer);
	}

	/**
	 * Display a publisher's available resources on the resources table when the
	 * publisher is selected on the publishers list.
	 * 
	 * @param row
	 * @param column
	 */
	public void onSelectedPublisher(int row, int column) {
		int id = Integer.parseInt((String) publishersList.getValueAt(row, 0));
		List<DwcaResourceModel> resources = stepController.getResourceModelList();
		this.resources = new ArrayList<DwcaResourceModel>();
		for (DwcaResourceModel r : resources) {
			if (r.getPublisher() != null) {
				if (r.getPublisher().getAuto_id() == id)
					this.resources.add(r);
			}
		}
		initResourcesTable();
	}

	/**
	 * Creates a table based on the resources available in the publishers' list
	 * 
	 * @return
	 */
	private void initResourcesTable() {
		if (resourcesList == null) {
			resourcesList = new JTable() {
				/**
				 * Remove table edition
				 * 
				 * @see javax.swing.JTable#isCellEditable(int, int)
				 */
				@Override
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
			// Set single selection mode only:
			resourcesList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		}

		// Initialize TableModel:
		initResourcesTableModel();

		if (resources != null && resources.size() > 0) {
			// Add rows in the table model for each resource:
			for (DwcaResourceModel r : resources) {
				Vector<String> aux = new Vector<String>();
				aux.add(r.getId().toString());
				aux.add(r.getName());
				if (r.getRecord_count() == null) {
					aux.add("");
				}
				else {
					aux.add(r.getRecord_count().toString());
				}
				resourcesTableModel.addRow(aux);
			}
		}
	}

	/**
	 * Initialize the resources TableModel.
	 */
	public void initResourcesTableModel() {
		// Init tableModel:
		resourcesTableModel = new DefaultTableModel();
		resourcesTableModel.setColumnIdentifiers(resourcesTableHeaders);

		resourcesList.setModel(resourcesTableModel);

		// Set preferred column sizes:
		resourcesList.getColumnModel().getColumn(0).setPreferredWidth(30);
		resourcesList.getColumnModel().getColumn(1).setPreferredWidth(620);
		resourcesList.getColumnModel().getColumn(2).setPreferredWidth(100);

		// Center string contents for columns id and record count:
		DefaultTableCellRenderer centerRenderer = new DefaultTableCellRenderer();
		centerRenderer.setHorizontalAlignment(JLabel.CENTER);
		resourcesList.getColumnModel().getColumn(0).setCellRenderer(centerRenderer);
		resourcesList.getColumnModel().getColumn(2).setCellRenderer(centerRenderer);
	}
}