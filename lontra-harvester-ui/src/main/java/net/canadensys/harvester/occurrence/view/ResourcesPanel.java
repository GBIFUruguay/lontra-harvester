package net.canadensys.harvester.occurrence.view;

import java.awt.Color;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.harvester.occurrence.controller.StepControllerIF;
import net.canadensys.harvester.occurrence.controller.StepControllerIF.JobType;
import net.canadensys.harvester.occurrence.model.JobStatusModel;
import net.canadensys.harvester.occurrence.model.JobStatusModel.JobStatus;
import net.canadensys.harvester.occurrence.view.model.HarvesterViewModel;

/**
 * Resources panel for tabbed pane
 *
 * @author Canadensys, Pedro Guimarães
 *
 */
public class ResourcesPanel extends JPanel implements PropertyChangeListener {

	private static final long serialVersionUID = 1093812890375L;
	private final String END_LINE = System.getProperty("line.separator");

	private DwcaResourceModel resourceToImport = null;
	private ImageIcon loadingImg = null;
	private JButton importBtn = null;
	private JButton moveToPublicBtn = null;
	private JButton addResourceBtn = null;
	private JButton editResourceBtn = null;
	private JButton computeUniqueValuesBtn = null;
	private JLabel statusLbl = null;
	private JTextField bufferSchemaTxt = null;
	private JTextArea consoleTxtArea = null;
	private JComboBox<String> resourcesCmbBox = null;
	private JCheckBox moveChkBox = null;
	private JCheckBox uniqueValuesChkBox = null;

	// Inherited from OccurrenceHarvesterMainView:
	private final StepControllerIF stepController;

	private List<DwcaResourceModel> knownResources;

	public ResourcesPanel(StepControllerIF stepController, HarvesterViewModel harvesterViewModel) {
		this.stepController = stepController;
		// register to receive all updates to the model
		harvesterViewModel.addPropertyChangeListener(this);

		knownResources = stepController.getResourceModelList();

		this.setLayout(new GridBagLayout());

		// Load icon image:
		loadingImg = new ImageIcon(OccurrenceHarvesterMainView.class.getResource("/ajax-loader.gif"));

		// Vertical alignment reference index:
		int lineIdx = 0;
		GridBagConstraints c = new GridBagConstraints();

		// Resource label:
		c.insets = new Insets(5, 5, 5, 5);
		c.gridx = 0;
		this.add(new JLabel(Messages.getString("view.info.import.dwca")), c);

		// Select resource combo box:
		initResourceComboBox();
		c.gridx = 0;
		c.gridy = ++lineIdx;
		c.gridwidth = 4;
		c.fill = GridBagConstraints.HORIZONTAL;
		// c.anchor = GridBagConstraints.NORTH;
		this.add(resourcesCmbBox, c);

		// Import button:
		c.gridx = 1;
		c.gridy = ++lineIdx;
		c.gridwidth = 1;
		c.fill = GridBagConstraints.HORIZONTAL;
		c.anchor = GridBagConstraints.NORTHEAST;
		importBtn = new JButton(Messages.getString("view.button.import"));
		importBtn.setToolTipText(Messages.getString("view.button.import.tooltip"));
		importBtn.setEnabled(false);
		importBtn.setVisible(true);
		importBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onImportResource();
			}
		});
		this.add(importBtn, c);

		// View/edit resource button:
		c.gridx = 2;
		editResourceBtn = new JButton(Messages.getString("view.button.edit.resource"));
		editResourceBtn.setToolTipText(Messages.getString("view.button.edit.resource.tooltip"));
		editResourceBtn.setVisible(true);
		editResourceBtn.setEnabled(false);
		editResourceBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onEditResource();
			}
		});
		this.add(editResourceBtn, c);

		// Add resource button:
		// c.gridy = ++lineIdx;
		c.gridx = 3;
		addResourceBtn = new JButton(Messages.getString("view.button.add.resource"));
		addResourceBtn.setToolTipText(Messages.getString("view.button.add.resource.tooltip"));
		addResourceBtn.setEnabled(true);
		addResourceBtn.setVisible(true);
		addResourceBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onAddResource();
			}
		});
		this.add(addResourceBtn, c);

		// UI separator
		c.gridx = 0;
		c.gridy = ++lineIdx;
		c.gridwidth = 4;
		c.fill = GridBagConstraints.HORIZONTAL;
		this.add(new JSeparator(), c);

		// DwcA ready to move label:
		c.gridy = ++lineIdx;
		c.fill = GridBagConstraints.NONE;
		c.anchor = GridBagConstraints.NORTHWEST;
		this.add(new JLabel(Messages.getString("view.info.move.dwca")), c);

		// DwcA text field name display:
		bufferSchemaTxt = new JTextField();
		bufferSchemaTxt.setEnabled(false);
		c.gridwidth = 3;
		c.gridx = 0;
		c.gridy = ++lineIdx;
		c.fill = GridBagConstraints.HORIZONTAL;
		c.weightx = 1.0;
		c.anchor = GridBagConstraints.NORTHEAST;
		this.add(bufferSchemaTxt, c);

		// Move to public schema button:
		moveToPublicBtn = new JButton(Messages.getString("view.button.move"));
		moveToPublicBtn.setToolTipText(Messages.getString("view.button.move.tooltip"));
		moveToPublicBtn.setEnabled(false);
		moveToPublicBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onMoveToPublic();
			}
		});
		c.gridwidth = 1;
		c.gridx = 3;
		c.anchor = GridBagConstraints.NORTHEAST;
		c.fill = GridBagConstraints.NONE;
		this.add(moveToPublicBtn, c);

		// Compute unique values to public schema button:
		computeUniqueValuesBtn = new JButton(Messages.getString("view.button.compute.unique.values"));
		computeUniqueValuesBtn.setToolTipText(Messages.getString("view.button.compute.unique.values.tooltip"));
		computeUniqueValuesBtn.setEnabled(true);
		computeUniqueValuesBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onComputeUniqueValues();
			}
		});
		c.gridwidth = 1;
		c.gridx = 3;
		c.gridy = ++lineIdx;
		c.anchor = GridBagConstraints.NORTHEAST;
		c.fill = GridBagConstraints.NONE;
		this.add(computeUniqueValuesBtn, c);

		// Auto move checkbox:
		moveChkBox = new JCheckBox();
		moveChkBox.setText(Messages.getString("view.button.automove"));
		moveChkBox.setToolTipText(Messages.getString("view.button.automove.tip"));
		moveChkBox.setEnabled(true);
		c.gridwidth = 1;
		c.gridy = ++lineIdx;
		c.gridx = 0;
		c.fill = GridBagConstraints.HORIZONTAL;
		this.add(moveChkBox, c);

		// Compute unique values checkbox:
		uniqueValuesChkBox = new JCheckBox();
		uniqueValuesChkBox.setText(Messages.getString("view.button.unique.values"));
		uniqueValuesChkBox.setToolTipText(Messages.getString("view.button.unique.values.tip"));
		uniqueValuesChkBox.setEnabled(true);
		c.gridy = ++lineIdx;
		this.add(uniqueValuesChkBox, c);

		// UI separator
		c.gridx = 0;
		c.gridy = ++lineIdx;
		c.gridwidth = 4;
		c.fill = GridBagConstraints.HORIZONTAL;
		this.add(new JSeparator(), c);

		// UI line break
		c.gridy = ++lineIdx;
		c.anchor = GridBagConstraints.NORTHWEST;
		c.fill = GridBagConstraints.NONE;
		this.add(new JLabel(Messages.getString("view.info.status")), c);

		// UI line break
		c.gridy = ++lineIdx;
		c.anchor = GridBagConstraints.WEST;
		statusLbl = new JLabel(Messages.getString("view.info.status.waiting"), null, JLabel.CENTER);
		statusLbl.setForeground(Color.RED);
		this.add(statusLbl, c);

		// UI line break
		c.gridy = ++lineIdx;
		c.gridwidth = 4;
		c.fill = GridBagConstraints.HORIZONTAL;
		this.add(new JSeparator(), c);

		// UI line break
		c.gridy = ++lineIdx;
		c.gridwidth = 2;
		c.anchor = GridBagConstraints.WEST;
		c.fill = GridBagConstraints.NONE;
		this.add(new JLabel(Messages.getString("view.info.console")), c);

		// UI line break
		consoleTxtArea = new JTextArea();
		consoleTxtArea.setRows(15);
		c.gridy = ++lineIdx;
		c.gridwidth = 4;
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 1.0;
		c.weighty = 1.0;
		this.add(new JScrollPane(consoleTxtArea), c);

		// inner panel
		c = new GridBagConstraints();
		c.gridx = 0;
		c.gridy = 0;
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 1.0;
		c.weighty = 1.0;
	}

	/**
	 * Safely update the content of the status label.
	 *
	 * @param status
	 */
	public void updateStatusLabel(final String status) {
		SwingUtilities.invokeLater(new Runnable() {
			@Override
			public void run() {
				statusLbl.setText(status);
			}
		});
	}

	public void appendConsoleText(final String text) {
		SwingUtilities.invokeLater(new Runnable() {
			@Override
			public void run() {
				consoleTxtArea.append(text);
			}
		});
	}

	private void onImportDwcDone(JobStatus status) {
		switch (status) {
			case DONE:
				statusLbl.setIcon(null);
				bufferSchemaTxt.setText(resourceToImport.getSourcefileid());
				updateStatusLabel(Messages.getString("view.info.status.importDone"));
				// If auto move is set, start move:
				if (moveChkBox.getSelectedObjects() == null) {
					moveToPublicBtn.setEnabled(true);
				}
				break;
			case ERROR:
				statusLbl.setIcon(null);
				updateStatusLabel(Messages.getString("view.info.status.error.importError"));
				JOptionPane.showMessageDialog(this, Messages.getString("view.info.status.error.details"),
						Messages.getString("view.info.status.error"), JOptionPane.ERROR_MESSAGE);
				break;
			case CANCEL:
				statusLbl.setIcon(null);
				updateStatusLabel(Messages.getString("view.info.status.canceled"));
				break;
			default:
				break;
		}
	}

	private void onMoveStarted() {
		// use invokeLater since this method could be called from the outside
		SwingUtilities.invokeLater(new Runnable() {
			@Override
			public void run() {
				moveToPublicBtn.setEnabled(false);
				updateStatusLabel(Messages.getString("view.info.status.moving"));
				statusLbl.setIcon(loadingImg);
			}
		});
	}

	private void onMoveDone(JobStatus status) {
		statusLbl.setIcon(null);
		if (JobStatus.DONE == status) {
			JOptionPane.showMessageDialog(this, Messages.getString("view.info.status.moveCompleted"), Messages.getString("view.info.status.info"),
					JOptionPane.INFORMATION_MESSAGE);
			bufferSchemaTxt.setText("");
			statusLbl.setText(Messages.getString("view.info.status.moveDone"));
			statusLbl.setForeground(Color.BLUE);
		}
		else {
			JOptionPane.showMessageDialog(this, Messages.getString("view.info.status.error.details"), Messages.getString("view.info.status.error"),
					JOptionPane.ERROR_MESSAGE);
			statusLbl.setText(Messages.getString("view.info.status.error.moveError"));
		}
	}

	private void onUpdateDone(JobStatus status) {
		statusLbl.setIcon(null);
		if (JobStatus.DONE == status) {
			JOptionPane.showMessageDialog(this, Messages.getString("view.info.status.updateDone"), Messages.getString("view.info.status.info"),
					JOptionPane.INFORMATION_MESSAGE);
			bufferSchemaTxt.setText("");
			statusLbl.setText(Messages.getString("view.info.status.updateDone"));
			statusLbl.setForeground(Color.BLUE);
		}
		else {
			JOptionPane.showMessageDialog(this, Messages.getString("view.info.status.error.details"), Messages.getString("view.info.status.error"),
					JOptionPane.ERROR_MESSAGE);
			statusLbl.setText(Messages.getString("view.info.status.error.updateError"));
		}
	}

	/**
	 * Initializes the resources combo box by creating a JComboBox and filling
	 * it with resource data from database
	 *
	 */
	private void initResourceComboBox() {
		resourcesCmbBox = new JComboBox<String>();
		resourcesCmbBox.addItem("");
		resourcesCmbBox.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				String selectedResource = (String) resourcesCmbBox.getSelectedItem();
				if (selectedResource != null) {
					if (!selectedResource.equalsIgnoreCase("")) {
						editResourceBtn.setEnabled(true);
						importBtn.setEnabled(true);
					}
					else {
						editResourceBtn.setEnabled(false);
						importBtn.setEnabled(false);
					}
				}
				orderResources();
			}
		});
		orderResources();
	}

	/**
	 * Import a resource (asynchronously) using a SwingWorker.
	 */
	private void onImportResource() {
		// Check there is a valid item selected
		if (resourcesCmbBox.getSelectedItem() != null) {
			// Avoid first entry (void):
			if (resourcesCmbBox.getSelectedIndex() > 0) {
				String selectedResource = (String) resourcesCmbBox.getSelectedItem();
				// Update resource to be imported based on selected item:
				for (DwcaResourceModel resource : stepController.getResourceModelList()) {
					if (resource.getName().equalsIgnoreCase(selectedResource))
						resourceToImport = resource;
				}
				importBtn.setEnabled(false);
				moveToPublicBtn.setEnabled(false);
				addResourceBtn.setEnabled(false);
				editResourceBtn.setEnabled(false);
				computeUniqueValuesBtn.setEnabled(false);
				statusLbl.setIcon(loadingImg);

				final SwingWorker<Void, Object> swingWorker = new SwingWorker<Void, Object>() {
					@Override
					public Void doInBackground() {
						try {
							if (resourceToImport != null) {
								stepController.importDwcA(resourceToImport.getId(), moveChkBox.isSelected(), uniqueValuesChkBox.isSelected());
							}
						}
						catch (Exception e) {
							// should not get there but just in case
							e.printStackTrace();
						}
						// async call, propertyChange(...) will be called once
						// done
						return null;
					}

					@Override
					protected void done() {
					}
				};
				swingWorker.execute();
			}
		}
	}

	/**
	 * Move the previously importer resource to the public schema using a
	 * SwingWorker.
	 */
	private void onMoveToPublic() {
		onMoveStarted();

		final SwingWorker<Boolean, Object> swingWorker = new SwingWorker<Boolean, Object>() {
			@Override
			public Boolean doInBackground() {

				// Check if the indexing is supposed to process unique values or
				// not:
				if (uniqueValuesChkBox.getSelectedObjects() != null) {
					stepController.moveToPublicSchema(resourceToImport.getId(), true);
				}
				else {
					stepController.moveToPublicSchema(resourceToImport.getId(), false);
				}
				return true;
			}

			@Override
			protected void done() {
			}
		};
		swingWorker.execute();
	}

	/**
	 * Edit resource button action.
	 */
	private void onEditResource() {

		importBtn.setEnabled(false);
		moveToPublicBtn.setEnabled(false);
		addResourceBtn.setEnabled(false);
		editResourceBtn.setEnabled(false);
		computeUniqueValuesBtn.setEnabled(false);

		boolean ok = editResourceDialog();

		knownResources = stepController.getResourceModelList();
		updateResourceComboBox();
		// Change back status and buttons display:
		addResourceBtn.setEnabled(true);
		computeUniqueValuesBtn.setEnabled(true);
		// Case the select button was pressed (instead of cancel)
		if (ok) {
			statusLbl.setIcon(null);
			statusLbl.setForeground(Color.BLUE);
			updateStatusLabel(Messages.getString("view.info.status.updateDone"));
		}
	}

	private boolean editResourceDialog() {
		DwcaResourceModel resourceToEdit = null;
		// Check there is a valid item selected
		if (resourcesCmbBox.getSelectedItem() != null) {
			// Avoid first entry (void):
			if (resourcesCmbBox.getSelectedIndex() > 0) {
				String selectedResource = (String) resourcesCmbBox.getSelectedItem();
				// Get resource to be edited based on selected item:
				for (DwcaResourceModel resource : stepController.getResourceModelList()) {
					if (resource.getName().equalsIgnoreCase(selectedResource))
						resourceToEdit = resource;
				}
			}
			// Start resource edition panel
			ResourceDialog erd = new ResourceDialog(this, stepController, resourceToEdit, true);
			int resourceId = resourceToEdit.getId();

			if (erd.getExitValue() == JOptionPane.OK_OPTION) {
				updateStatusLabel(Messages.getString("view.info.status.updating"));
				statusLbl.setIcon(loadingImg);
				if (!stepController.updateResourceModel(erd.getResourceModel())) {
					JOptionPane.showMessageDialog(this, Messages.getString("resourceView.resource.error.save.msg"),
							Messages.getString("resourceView.resource.error.title"), JOptionPane.ERROR_MESSAGE);
				}
				else {
					// Resource has been changed successfully, update database:
					// Update database after move
					// C.G. disabled, see comments in refreshResource
					stepController.refreshResource(resourceId);
				}
				return true;
			}
		}
		return false;
	}

	/**
	 * Add resource button action
	 */
	private void onAddResource() {
		ResourceDialog rd = new ResourceDialog(this, stepController, null, false);
		DwcaResourceModel resourceModel = rd.getResourceModel();
		if (rd.getExitValue() == JOptionPane.OK_OPTION) {
			if (!stepController.updateResourceModel(resourceModel)) {
				JOptionPane.showMessageDialog(this, Messages.getString("resourceView.resource.error.save.msg"),
						Messages.getString("resourceView.resource.error.title"), JOptionPane.ERROR_MESSAGE);
			}

			// reload data to ensure we have the latest changes
			knownResources = stepController.getResourceModelList();
			updateResourceComboBox();
		}
	}

	/**
	 * Update knownCbx if the variable knownResource has changed.
	 */
	private void updateResourceComboBox() {
		resourcesCmbBox.removeAllItems();
		resourcesCmbBox.addItem(null);
		for (DwcaResourceModel resourceModel : knownResources) {
			resourcesCmbBox.addItem(resourceModel.getName());
		}
	}

	public void onComputeUniqueValues() {
		final SwingWorker<Boolean, Object> swingWorker = new SwingWorker<Boolean, Object>() {
			@Override
			public Boolean doInBackground() {
				// Update status:
				computeUniqueValuesBtn.setEnabled(false);
				statusLbl.setIcon(loadingImg);
				statusLbl.setForeground(Color.ORANGE);
				updateStatusLabel(Messages.getString("view.info.status.compute.unique.values"));
				// Call compute unique values task:
				stepController.computeUniqueValues();
				return true;
			}

			@Override
			protected void done() {
				// Update status:
				statusLbl.setIcon(null);
				statusLbl.setForeground(Color.BLUE);
				updateStatusLabel(Messages.getString("view.info.status.compute.unique.values.done"));
				computeUniqueValuesBtn.setEnabled(true);
			}
		};
		swingWorker.execute();
	}

	public void orderResources() {
		// Retrieve available resources list:
		List<DwcaResourceModel> resources = stepController.getResourceModelList();

		// Add an item for each resource name:
		ArrayList<String> names = new ArrayList<String>();
		for (DwcaResourceModel resource : resources) {
			names.add(resource.getName());
		}
		// Reorder alphabetically:
		Collections.sort(names);
		for (String name : names) {
			resourcesCmbBox.addItem(name);
		}
	}

	private void updateProgressText(final String text) {
		SwingUtilities.invokeLater(new Runnable() {
			@Override
			public void run() {
				updateStatusLabel(text);
			}
		});
	}

	@Override
	public void propertyChange(PropertyChangeEvent evt) {
		if (JobStatusModel.CURRENT_STATUS_EXPLANATION_PROPERTY.equals(evt
				.getPropertyName())) {
			appendConsoleText(">" + (String) evt.getNewValue() + END_LINE);
		}
		else if (JobStatusModel.CURRENT_JOB_PROGRESS_PROPERTY.equals(evt
				.getPropertyName())) {
			updateProgressText((String) evt.getNewValue());
		}
		else if (JobStatusModel.CURRENT_STATUS_PROPERTY.equals(evt.getPropertyName())) {
			JobStatus jobStatus = (JobStatus) evt.getNewValue();
			appendConsoleText("STATUS:" + jobStatus + END_LINE);

			JobStatusModel jsm = (JobStatusModel) evt.getSource();
			JobType jobType = stepController.getJobType(jsm.getCurrentJobId());

			if (jobStatus.isJobCompleted()) {
				switch (jobType) {
					case IMPORT_DWC:
						onImportDwcDone(jobStatus);
						break;
					case MOVE_TO_PUBLIC:
						onMoveDone(jobStatus);
						break;
					case COMPUTE_UNIQUE:
						break;

					default:
						break;
				}
			}
			else if (JobStatus.RUNNING == jobStatus && JobType.MOVE_TO_PUBLIC == jobType) {
				onMoveStarted();
			}
			// TODO handle import started the same way
		}
	}

}