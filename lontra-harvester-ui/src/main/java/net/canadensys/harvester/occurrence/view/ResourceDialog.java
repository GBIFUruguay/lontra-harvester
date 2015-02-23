package net.canadensys.harvester.occurrence.view;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
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
	private JTextField resourceUuidTxt;

	private DwcaResourceModel resourceModel = null;

	public ResourceDialog(Component parent, StepControllerIF stepController, DwcaResourceModel selectedResource, boolean isEdition) {
		super(Messages.getString("resourceView.title"), stepController, isEdition);
		this.resourceModel = selectedResource;
		setLocationRelativeTo(parent);
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

		/* Resource UUID */
		JLabel resourceJLabel = new JLabel(Messages.getString("resourceView.resource.uuid"));
		resourceJLabel.setToolTipText(Messages.getString("resourceView.resource.uuid.tooltip"));
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
		resourceUuidTxt = new JTextField(TXT_FIELD_LENGTH);
		resourceUuidTxt.setEditable(false);
		contentPanel.add(resourceUuidTxt, c);

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
	 * @param resourceModel
	 * @return updated ResourceModel or null if resourceModel in parameter was
	 *         null
	 */
	public void displayResource() {
		if (resourceModel != null) {
			nameTxt.setText(resourceModel.getName());
			urlTxt.setText(resourceModel.getArchive_url());
			sfIdTxt.setText(resourceModel.getSourcefileid());
			resourceUuidTxt.setText((resourceModel.getResource_uuid()));
		}
		// Add resource, set UUID and source file id editable:
		else {
			sfIdTxt.setEditable(true);
			resourceUuidTxt.setEditable(true);
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
		String resourceUuid = resourceUuidTxt.getText();
		String publisherName = (String) publishersCmbBox.getSelectedItem();

		// This is a new resource, initialize resourceModel
		if (!isEdition) {
			resourceModel = new DwcaResourceModel();
		}

		if (StringUtils.isNotBlank(nameValue) && StringUtils.isNotBlank(urlValue) && StringUtils.isNotBlank(sourceFileIdValue)) {
			resourceModel.setName(nameValue);
			resourceModel.setArchive_url(urlValue);
			resourceModel.setSourcefileid(sourceFileIdValue);
			resourceModel.setResource_uuid(resourceUuid);
			// Check if it has been set a valid publisher:

			if (!publisherName.equalsIgnoreCase(""))
				resourceModel.setPublisher(getPublisherFromName(publisherName));
			if (!isEdition) {
				// Set record_count to 0 by default:
				resourceModel.setRecord_count(0);
			}
			exitValue = JOptionPane.OK_OPTION;
			dispose();
		}
		else {
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
		List<PublisherModel> publishers = stepController
				.getPublisherModelList();

		// Add blank item:
		publishersCmbBox.addItem("");
		// Add an item for each publisher:
		for (PublisherModel publisher : publishers) {
			publishersCmbBox.addItem(publisher.getName());
		}
		if (isEdition && resourceModel.getPublisher() != null) {
			publishersCmbBox.setSelectedItem(resourceModel.getPublisher().getName());
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
		return this.resourceModel;
	}
}
