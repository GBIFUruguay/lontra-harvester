package net.canadensys.harvester.occurrence.view;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.dataportal.occurrence.model.PublisherModel;
import net.canadensys.harvester.occurrence.controller.StepControllerIF;

import org.apache.commons.lang3.StringUtils;

/**
 * View component to display and edit a resource.
 *
 * @author canadensys, Pedro Guimarães
 *
 */
public class ResourceDialog extends AbstractDialog {

	private static final long serialVersionUID = 672869826178689726L;

	private static final int TXT_FIELD_LENGTH = 50;

	private JLabel idLbl;
	private JLabel idValueLbl;

	private JTextField nameTxt;
	private JTextField urlTxt;
	private JTextField sfIdTxt;
	private JTextField gbifPackageIdTxt;

	private DwcaResourceModel resource = null;

	private ResourcesPanel resourcesPanel;

	public ResourceDialog(Component parent, StepControllerIF stepController, DwcaResourceModel selectedResource,
			boolean isEdition) {
		super(Messages.getString("resourceView.title"), stepController, isEdition);
		this.resource = selectedResource;
		setLocationRelativeTo(parent);
		resourcesPanel = (ResourcesPanel) parent;
		displayResource();
	}

	@Override
	protected void init(JPanel contentPanel) {
		contentPanel.setLayout(new GridBagLayout());
		GridBagConstraints c = new GridBagConstraints();

		/* ID */
		idLbl = new JLabel(Messages.getString("resourceView.resource.id"));
		c = new GridBagConstraints();
		c.gridx = 0;
		c.gridy = 0;
		c.fill = GridBagConstraints.HORIZONTAL;
		c.anchor = GridBagConstraints.EAST;
		contentPanel.add(idLbl, c);

		c = new GridBagConstraints();
		c.gridx = 1;
		c.gridy = 0;
		c.fill = GridBagConstraints.HORIZONTAL;
		idValueLbl = new JLabel();
		contentPanel.add(idValueLbl, c);

		// hide them by default
		idLbl.setVisible(false);
		idValueLbl.setVisible(false);

		/* Resource name */
		JLabel nameLbl = new JLabel(Messages.getString("resourceView.resource.name"));
		nameLbl.setToolTipText(Messages.getString("resourceView.resource.name.tooltip"));
		c = new GridBagConstraints();
		c.gridx = 0;
		c.gridy = 1;
		c.fill = GridBagConstraints.HORIZONTAL;
		c.anchor = GridBagConstraints.EAST;
		contentPanel.add(nameLbl, c);

		c = new GridBagConstraints();
		c.gridx = 1;
		c.gridy = 1;
		c.fill = GridBagConstraints.HORIZONTAL;
		nameTxt = new JTextField(TXT_FIELD_LENGTH);
		contentPanel.add(nameTxt, c);

		/* Resource URL */
		JLabel urlLbl = new JLabel(Messages.getString("resourceView.resource.url"));
		urlLbl.setToolTipText(Messages.getString("resourceView.resource.url.tooltip"));
		c = new GridBagConstraints();
		c.gridx = 0;
		c.gridy = 2;
		c.fill = GridBagConstraints.HORIZONTAL;
		c.anchor = GridBagConstraints.EAST;
		contentPanel.add(urlLbl, c);

		c = new GridBagConstraints();
		c.gridx = 1;
		c.gridy = 2;
		c.fill = GridBagConstraints.HORIZONTAL;
		urlTxt = new JTextField(TXT_FIELD_LENGTH);
		contentPanel.add(urlTxt, c);

		/* SourceFileId */
		JLabel sfIdLbl = new JLabel(Messages.getString("resourceView.resource.sourceFileID"));
		sfIdLbl.setToolTipText(Messages.getString("resourceView.resource.sourceFileID.tooltip"));
		c = new GridBagConstraints();
		c.gridx = 0;
		c.gridy = 3;
		c.fill = GridBagConstraints.HORIZONTAL;
		c.anchor = GridBagConstraints.EAST;
		contentPanel.add(sfIdLbl, c);

		c = new GridBagConstraints();
		c.gridx = 1;
		c.gridy = 3;
		c.fill = GridBagConstraints.HORIZONTAL;
		sfIdTxt = new JTextField(TXT_FIELD_LENGTH);
		sfIdTxt.setEditable(false);
		contentPanel.add(sfIdTxt, c);

		/* Resource GBIF package_id */
		JLabel resourceJLabel = new JLabel(Messages.getString("resourceView.resource.gbifpackageid"));
		resourceJLabel.setToolTipText(Messages.getString("resourceView.resource.gbifpackageid.tooltip"));
		c = new GridBagConstraints();
		c.gridx = 0;
		c.gridy = 4;
		c.fill = GridBagConstraints.HORIZONTAL;
		c.anchor = GridBagConstraints.EAST;
		contentPanel.add(resourceJLabel, c);

		c = new GridBagConstraints();
		c.gridx = 1;
		c.gridy = 4;
		c.fill = GridBagConstraints.HORIZONTAL;
		gbifPackageIdTxt = new JTextField(TXT_FIELD_LENGTH);
		gbifPackageIdTxt.setEditable(false);
		contentPanel.add(gbifPackageIdTxt, c);

		/* Init publishers combo box and add it to the dialog */
		publishersCmbBox = new JComboBox<String>();
		c.gridx = 0;
		c.gridy = 5;
		contentPanel.add(new JLabel(Messages.getString("resourceView.resource.publisher")), c);
		c.gridx = 1;
		c.gridy = 5;
		contentPanel.add(publishersCmbBox, c);

	}

	/**
	 * Display a ResourceModel and allow user to update it.
	 *
	 * @param resource
	 * @return updated ResourceModel or null if resourceModel in parameter was
	 *         null
	 */
	public void displayResource() {
		if (resource != null) {
			nameTxt.setText(resource.getName());
			urlTxt.setText(resource.getArchive_url());
			sfIdTxt.setText(resource.getSourcefileid());
			gbifPackageIdTxt.setText((resource.getGbif_package_id()));
		}
		// Add resource, set gbifPackageId and source file id editable:
		else {
			sfIdTxt.setEditable(true);
			gbifPackageIdTxt.setEditable(true);
		}
		initPublishersComboBox();
		setVisible(true);
	}

	/**
	 * Actions executed when the user presses the Ok button
	 */
	@Override
	protected void onSelect() {
		String nameValue = nameTxt.getText();
		String urlValue = urlTxt.getText();
		String sourceFileIdValue = sfIdTxt.getText();
		String gbifPackageId = gbifPackageIdTxt.getText();
		String publisherName = (String) publishersCmbBox.getSelectedItem();

		PublisherModel publisher = null;

		// This is a new resource, initialize resourceModel
		if (!isEdition) {
			resource = new DwcaResourceModel();
			// Set record_count to 0 by default:
			resource.setRecord_count(0);
		}

		if (StringUtils.isNotBlank(nameValue) && StringUtils.isNotBlank(urlValue)
				&& StringUtils.isNotBlank(sourceFileIdValue)) {
			resource.setName(nameValue);
			resource.setArchive_url(urlValue);
			resource.setSourcefileid(sourceFileIdValue);
			resource.setGbif_package_id(gbifPackageId);
			// Check if it has been set a valid publisher:
			if (!publisherName.equalsIgnoreCase("")) {
				publisher = getPublisherFromName(publisherName);
				// Resource already has a publisher:
				PublisherModel currentPublisher = resource.getPublisher();
				if (currentPublisher != null) {
					// Resource is being associated to another publisher, must
					// update previous publisher record counts:
					if (!resource.getPublisher().getName().equalsIgnoreCase(publisherName)) {
						// Update record count for previous publisher:
						Integer recordCount = currentPublisher.getRecord_count() - resource.getRecord_count();
						currentPublisher.setRecord_count(recordCount);
						stepController.updatePublisherModel(currentPublisher);
					}
				}
				// Set new publisher to the resource
				resource.setPublisher(publisher);
				// Update record count for new publisher:
				Integer recordCount = publisher.getRecord_count() + resource.getRecord_count();
				publisher.setRecord_count(recordCount);
				// Update new publisher information:
				stepController.updatePublisherModel(publisher);
			}
			// In this case, the user is setting the resource to a state where
			// it belongs to no
			// publisher.
			else {
				PublisherModel currentPublisher = resource.getPublisher();
				if (currentPublisher != null) {
					resource.setPublisher(null);
					// Update record count for previous publisher:
					Integer recordCount = currentPublisher.getRecord_count() - resource.getRecord_count();
					currentPublisher.setRecord_count(recordCount);
					stepController.updatePublisherModel(currentPublisher);
				}
			}
			// Save resource updates to db:
			stepController.updateResourceModel(resource);
			// Update resources' occurrence publishername field:
			resourcesPanel.publisherNameUpdate(publisherName);
			// Reload publishers' combo box:
			publishersCmbBox = new JComboBox<String>();
			initPublishersComboBox();
			exitValue = JOptionPane.OK_OPTION;
			dispose();
		} else {
			JOptionPane.showMessageDialog(this, Messages.getString("resourceView.resource.error.missing.msg"),
					Messages.getString("resourceView.resource.error.title"), JOptionPane.ERROR_MESSAGE);
		}
	}

	@Override
	protected void onCancel() {
		exitValue = JOptionPane.CANCEL_OPTION;
		dispose();
	}

	@Override
	protected void postInit() {
		// use 'OK' instead of 'Select'
		selectBtn.setText(Messages.getString("view.button.ok"));
	}

	/**
	 * Initializes the resources combo box by creating a JComboBox and filling
	 * it with resource data from database
	 *
	 */
	private void initPublishersComboBox() {
		// Retrieve available resources list:
		List<PublisherModel> publishers = stepController.getPublisherModelList();

		// Add blank item:
		publishersCmbBox.addItem("");

		// Reorder alphabetically:
		ArrayList<String> names = new ArrayList<String>();
		for (PublisherModel publisher : publishers) {
			names.add(publisher.getName());
		}
		Collections.sort(names);

		// Add an item for each publisher name:
		for (String name : names) {
			publishersCmbBox.addItem(name);
		}

		if (isEdition && resource.getPublisher() != null) {
			publishersCmbBox.setSelectedItem(resource.getPublisher().getName());
		}
	}

	/**
	 * Fetch a PublisherModel from database from its name
	 *
	 * @param publisherName
	 *            the name field of the publisher table
	 * @return
	 */
	private PublisherModel getPublisherFromName(String publisherName) {
		List<PublisherModel> publishers = stepController.getPublisherModelList();
		PublisherModel publisher = null;
		for (PublisherModel p : publishers) {
			if (p.getName().equalsIgnoreCase(publisherName)) {
				publisher = p;
				break;
			}
		}
		return publisher;
	}

	/**
	 * Returns the resource model
	 *
	 * @return
	 */
	public DwcaResourceModel getResourceModel() {
		return this.resource;
	}
}
