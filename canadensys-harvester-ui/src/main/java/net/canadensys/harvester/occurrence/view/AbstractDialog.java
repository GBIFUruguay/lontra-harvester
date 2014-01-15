package net.canadensys.harvester.occurrence.view;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

/**
 * Abstract dialog display with select, cancel buttons.
 * 
 * @author canadensys
 * 
 */
public abstract class AbstractDialog extends JDialog {

	// UI components
	private JPanel mainPanel;
	protected JPanel contentPanel;

	protected JButton selectBtn = null;
	protected JButton cancelBtn = null;

	// initialize with CANCEL_OPTION to handle the case when the dialog is
	// closed with the 'X'
	protected int exitValue = JOptionPane.CANCEL_OPTION;

	public AbstractDialog(String title) {

		this.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		this.setTitle(title);
		this.setModal(true);

		// panel used by subclasses
		contentPanel = new JPanel();
		init(contentPanel);
		innerInit();

		pack();
	}

	private void innerInit() {
		mainPanel = new JPanel();
		GridBagConstraints c = new GridBagConstraints();
		c.gridx = 0;
		c.gridy = 0;
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 1.0;
		c.weighty = 1.0;

		mainPanel.add(contentPanel, c);

		JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new GridBagLayout());

		// select button
		selectBtn = new JButton(Messages.getString("view.button.select"));
		selectBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onSelect();
			}
		});
		c = new GridBagConstraints();
		c.gridx = 0;
		c.gridy = 0;
		buttonPanel.add(selectBtn, c);

		// close button
		cancelBtn = new JButton(Messages.getString("view.button.cancel"));
		cancelBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onCancel();
			}
		});
		c = new GridBagConstraints();
		c.gridx = 1;
		c.gridy = 0;
		buttonPanel.add(cancelBtn, c);

		c = new GridBagConstraints();
		c.gridx = 0;
		c.gridy = 2;
		c.gridwidth = 2;
		mainPanel.add(buttonPanel, c);

		setLayout(new GridBagLayout());
		mainPanel.setLayout(new GridBagLayout());
		c = new GridBagConstraints();
		c.gridx = 0;
		c.gridy = 1;
		mainPanel.add(buttonPanel, c);
		this.add(mainPanel, c);
	}

	/**
	 * Returns the 'option' chosen to exit the dialog.
	 * 
	 * @return JOptionPane.OK_OPTION or JOptionPane.CANCEL_OPTION
	 */
	public int getExitValue() {
		return exitValue;
	}

	protected abstract void init(JPanel contentPanel);

	protected abstract void onSelect();

	protected abstract void onCancel();

}
