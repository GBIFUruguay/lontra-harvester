package net.canadensys.harvester.occurrence.view;

import java.awt.Color;
import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import net.canadensys.dataportal.occurrence.model.PublisherModel;

import org.apache.commons.lang3.StringUtils;

/**
 * View component to display and edit a publisher.
 * 
 * @author Pedro Guimarães
 * 
 */
public class PublisherDialog extends AbstractDialog {

	private static final long serialVersionUID = 1188349656123737L;

	private PublisherModel publisherModel = null;
	private static final int TXT_FIELD_LENGTH = 50;

	private JTextField nameTxt;
	private JTextArea descriptionTxtArea;
	private JTextField addressTxt;
	private JTextField cityTxt;
	private JTextField admAreaTxt;
	private JTextField postalCodeTxt;
	private JTextField homepageTxt;
	private JTextField emailTxt;
	private JTextField phoneTxt;
	private JTextField logoUrlTxt;
	private JTextField latTxt;
	private JTextField longTxt;

	public PublisherDialog(Component parent, PublisherModel publisherToEdit, boolean isEdition) {
		super(Messages.getString("publisherView.title"), null, isEdition);
		this.publisherModel = publisherToEdit;
		setLocationRelativeTo(parent);
		displayPublisher();
	}

	@Override
	protected void init(JPanel contentPanel) {
		contentPanel.setLayout(new GridBagLayout());
		GridBagConstraints c = new GridBagConstraints();

		int yIndex = 0;
		/* Name */
		JLabel label = new JLabel(Messages.getString("publisherView.publisher.name"));
		label.setToolTipText(Messages.getString("publisherView.publisher.name.tooltip"));
		c.gridx = 0;
		c.gridy = yIndex;
		c.fill = GridBagConstraints.HORIZONTAL;
		c.anchor = GridBagConstraints.EAST;
		contentPanel.add(label, c);

		c.gridx = 2;
		nameTxt = new JTextField(TXT_FIELD_LENGTH);
		contentPanel.add(nameTxt, c);

		/* Description */
		label = new JLabel(Messages.getString("publisherView.publisher.description"));
		label.setToolTipText(Messages.getString("publisherView.publisher.description.tooltip"));
		c.gridx = 0;
		c.gridy = ++yIndex;
		c.fill = GridBagConstraints.HORIZONTAL;
		c.anchor = GridBagConstraints.EAST;
		contentPanel.add(label, c);

		c.gridx = 1;
		c.gridy = ++yIndex;
		c.gridwidth = 2;
		descriptionTxtArea = new JTextArea();
		descriptionTxtArea.setRows(5);
		descriptionTxtArea.setColumns(32);
		descriptionTxtArea.setLineWrap(true);
		descriptionTxtArea.setWrapStyleWord(true);
		contentPanel.add(new JScrollPane(descriptionTxtArea), c);

		/* Address */
		label = new JLabel(Messages.getString("publisherView.publisher.address"));
		label.setToolTipText(Messages.getString("publisherView.publisher.address.tooltip"));
		c.gridx = 0;
		c.gridy = ++yIndex;
		c.gridwidth = 1;
		contentPanel.add(label, c);

		c.gridx = 2;
		c.gridy = ++yIndex;
		addressTxt = new JTextField(TXT_FIELD_LENGTH);
		contentPanel.add(addressTxt, c);

		/* City */
		label = new JLabel(Messages.getString("publisherView.publisher.city"));
		label.setToolTipText(Messages.getString("publisherView.publisher.city.tooltip"));
		c.gridx = 0;
		c.gridy = ++yIndex;
		contentPanel.add(label, c);

		c.gridx = 2;
		c.gridy = ++yIndex;
		cityTxt = new JTextField(TXT_FIELD_LENGTH);
		contentPanel.add(cityTxt, c);

		/* Adm. area */
		label = new JLabel(Messages.getString("publisherView.publisher.adm.area"));
		label.setToolTipText(Messages.getString("publisherView.publisher.adm.area.tooltip"));
		c.gridx = 0;
		c.gridy = ++yIndex;
		contentPanel.add(label, c);

		c.gridx = 2;
		c.gridy = ++yIndex;
		admAreaTxt = new JTextField(TXT_FIELD_LENGTH);
		contentPanel.add(admAreaTxt, c);

		/* Postal Code */
		label = new JLabel(Messages.getString("publisherView.publisher.postal.code"));
		label.setToolTipText(Messages.getString("publisherView.publisher.postal.code.tooltip"));
		c.gridx = 0;
		c.gridy = ++yIndex;
		contentPanel.add(label, c);

		c.gridx = 2;
		c.gridy = ++yIndex;
		postalCodeTxt = new JTextField(TXT_FIELD_LENGTH);
		contentPanel.add(postalCodeTxt, c);

		/* Homepage */
		label = new JLabel(Messages.getString("publisherView.publisher.homepage"));
		label.setToolTipText(Messages.getString("publisherView.publisher.homepage.tooltip"));
		c.gridx = 0;
		c.gridy = ++yIndex;
		contentPanel.add(label, c);

		c.gridx = 2;
		c.gridy = ++yIndex;
		homepageTxt = new JTextField(TXT_FIELD_LENGTH);
		contentPanel.add(homepageTxt, c);

		/* Email */
		label = new JLabel(Messages.getString("publisherView.publisher.email"));
		label.setToolTipText(Messages.getString("publisherView.publisher.email.tooltip"));
		c.gridx = 0;
		c.gridy = ++yIndex;
		contentPanel.add(label, c);

		c.gridx = 2;
		c.gridy = ++yIndex;
		emailTxt = new JTextField(TXT_FIELD_LENGTH);
		contentPanel.add(emailTxt, c);

		/* Phone */
		label = new JLabel(Messages.getString("publisherView.publisher.phone"));
		label.setToolTipText(Messages.getString("publisherView.publisher.phone.tooltip"));
		c.gridx = 0;
		c.gridy = ++yIndex;
		contentPanel.add(label, c);

		c.gridx = 2;
		c.gridy = ++yIndex;
		phoneTxt = new JTextField(TXT_FIELD_LENGTH);
		contentPanel.add(phoneTxt, c);

		/* Logo url */
		label = new JLabel(Messages.getString("publisherView.publisher.logo.url"));
		label.setToolTipText(Messages.getString("publisherView.publisher.logo.url.tooltip"));
		c.gridx = 0;
		c.gridy = ++yIndex;
		contentPanel.add(label, c);

		c.gridx = 2;
		c.gridy = ++yIndex;
		logoUrlTxt = new JTextField(TXT_FIELD_LENGTH);
		contentPanel.add(logoUrlTxt, c);

		/* Latitude */
		label = new JLabel(Messages.getString("publisherView.publisher.lat"));
		label.setToolTipText(Messages.getString("publisherView.publisher.lat.tooltip"));
		c.gridx = 0;
		c.gridy = ++yIndex;
		contentPanel.add(label, c);

		c.gridx = 2;
		c.gridy = ++yIndex;
		;
		latTxt = new JTextField(TXT_FIELD_LENGTH);
		contentPanel.add(latTxt, c);

		/* Longitude */
		label = new JLabel(Messages.getString("publisherView.publisher.long"));
		label.setToolTipText(Messages.getString("publisherView.publisher.long.tooltip"));
		c.gridx = 0;
		c.gridy = ++yIndex;
		contentPanel.add(label, c);

		c.gridx = 2;
		c.gridy = ++yIndex;
		longTxt = new JTextField(TXT_FIELD_LENGTH);
		contentPanel.add(longTxt, c);

		/* Longitude */
		label = new JLabel(Messages.getString("publisherView.publisher.mandatory"));
		label.setForeground(Color.RED);
		++yIndex;
		c.gridx = 2;
		c.gridy = ++yIndex;
		c.anchor = GridBagConstraints.EAST;
		contentPanel.add(label, c);
	}

	/**
	 * Display a publisherModel's complete information in a dialog.
	 * 
	 * @param publisherModel
	 * @return updated publisherModel or null if publisherModel in parameter was
	 *         null
	 */
	public void displayPublisher() {
		if (publisherModel != null) {
			if (publisherModel.getAuto_id() != null) {
				if (isEdition) {
					nameTxt.setText(publisherModel.getName());
					nameTxt.setEditable(false);
					descriptionTxtArea.setText(publisherModel.getDescription());
					addressTxt.setText(publisherModel.getAddress());
					cityTxt.setText(publisherModel.getCity());
					admAreaTxt.setText(publisherModel.getAdministrative_area());
					postalCodeTxt.setText(publisherModel.getPostal_code());
					homepageTxt.setText(publisherModel.getHomepage());
					emailTxt.setText(publisherModel.getEmail());
					phoneTxt.setText(publisherModel.getPhone());
					logoUrlTxt.setText(publisherModel.getLogo_url());
					Double latitude = publisherModel.getDecimallatitude();
					if (latitude != null)
						latTxt.setText(latitude.toString());
					Double wingLongitude = publisherModel.getDecimallongitude();
					if (wingLongitude != null)
						longTxt.setText(wingLongitude.toString());
				}
			}
			nameTxt.setText(publisherModel.getName());
		} 
		// modal dialog, blocking function until dispose() is called
		setVisible(true);
	}

	@Override
	protected void onSelect() {
		String name = nameTxt.getText();
		String description = descriptionTxtArea.getText();
		String address = addressTxt.getText();
		String city = cityTxt.getText();
		String admArea = admAreaTxt.getText();
		String postalCode = postalCodeTxt.getText();
		String homepage = homepageTxt.getText();
		String email = emailTxt.getText();
		String phone = phoneTxt.getText();
		String logoUrl = logoUrlTxt.getText();
		String lat = latTxt.getText();
		String lon = longTxt.getText();
		
		// Initialize publisher case it is a new publisher being added
		if (!isEdition) {
			publisherModel = new PublisherModel();
		}	

		// Check for mandatory fields:
		if (StringUtils.isNotBlank(name) && StringUtils.isNotBlank(description) && StringUtils.isNotBlank(homepage) && StringUtils.isNotBlank(email)
				&& StringUtils.isNotBlank(logoUrl)) {
			publisherModel.setName(name);
			publisherModel.setDescription(description);
			publisherModel.setHomepage(homepage);
			publisherModel.setEmail(email);
			publisherModel.setLogo_url(logoUrl);
			// Set non mandatory fields:
			if (StringUtils.isNotBlank(address))
				publisherModel.setAddress(address);
			if (StringUtils.isNotBlank(city))
				publisherModel.setCity(city);
			if (StringUtils.isNotBlank(admArea))
				publisherModel.setAdministrative_area(admArea);
			if (StringUtils.isNotBlank(postalCode))
				publisherModel.setPostal_code(postalCode);
			if (StringUtils.isNotBlank(phone))
				publisherModel.setPhone(phone);
			// Treat special cases of lat and long double values:
			if (StringUtils.isNotBlank(lat)) {
				Double decimalLatitude = 0.0d;
				try {
					decimalLatitude = Double.parseDouble(lat);
				}
				catch (NumberFormatException n) {
					JOptionPane.showMessageDialog(this, n.getMessage(),
							Messages.getString("publisherView.publisher.error.title"), JOptionPane.ERROR_MESSAGE);
					decimalLatitude = 0.0d;
				}
				publisherModel.setDecimallatitude(decimalLatitude);
			}
			if (StringUtils.isNotBlank(lat)) {
				Double decimalLongitude = 0.0d;
				try {
					decimalLongitude = Double.parseDouble(lon);
				}
				catch (NumberFormatException n) {
					JOptionPane.showMessageDialog(this, n.getMessage(),
							Messages.getString("publisherView.publisher.error.title"), JOptionPane.ERROR_MESSAGE);
					decimalLongitude = 0.0d;
				}
				publisherModel.setDecimallongitude(decimalLongitude);
			}
			// This is a publisher addition, default should count 0 records.
			if (!isEdition) {
				publisherModel.setRecord_count(0);
			}
			exitValue = JOptionPane.OK_OPTION;
			// Dispose of dialog
			dispose();
		}
		else {
			JOptionPane.showMessageDialog(this, Messages.getString("publisherView.publisher.error.missing.msg"),
					Messages.getString("publisherView.publisher.error.title"), JOptionPane.ERROR_MESSAGE);
		}
	}

	@Override
	protected void onCancel() {
		exitValue = JOptionPane.CANCEL_OPTION;
		// Dispose of dialog
		dispose();
	}

	@Override
	protected void postInit() {
		// use 'OK' instead of 'Select'
		selectBtn.setText(Messages.getString("view.button.ok"));
	}

	public PublisherModel getPublisherModel() {
		return publisherModel;
	}
}
