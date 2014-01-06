package net.canadensys.processing.occurrence.view;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;

import net.canadensys.processing.occurrence.model.ResourceModel;

/**
 * Dialog used to select a resource.
 * @author canadensys
 *
 */
public class ResourceChooser extends JDialog{
	
	private static final long serialVersionUID = 6415119692476515726L;
	
	private List<ResourceModel> knownResource;
	private Vector<String> knowResourceVector;
	private ResourceModel selectedResource = null;
	
	//UI components
	private JPanel mainPanel;
	
	private JComboBox knownCbx = null;
	
	private JButton selectBtn = null;
	private JButton cancelBtn = null;
	
	public ResourceChooser(List<ResourceModel> knownResource){
		this.knownResource = knownResource;
		this.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		this.setTitle(Messages.getString("resourceChooser.title"));
		this.setModal(true);

		knowResourceVector = new Vector<String>();
		for(ResourceModel resourceModel : knownResource){
			knowResourceVector.add(resourceModel.getName() + "-" + resourceModel.getSource_file_id());
		}
		//add an empty record
		knowResourceVector.add(0, null);
		
		init();
	}
	
	private void init(){
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
				
		//known URL
		JLabel knownUrlLbl = new JLabel(Messages.getString("resourceChooser.knownResources"));
		knownCbx = new JComboBox(knowResourceVector);
		
		c = new GridBagConstraints();
		c.gridx=1;
		c.gridy=0;
		c.weightx = 0.5;
		c.fill = GridBagConstraints.HORIZONTAL;
		mainPanel.add(knownCbx,c);
		
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=0;
		mainPanel.add(knownUrlLbl,c);
		
		JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new GridBagLayout());
		//select button
		selectBtn = new JButton(Messages.getString("view.button.select"));
		selectBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				int idx = knownCbx.getSelectedIndex();
				if(idx > 0){
					selectedResource = knownResource.get(idx-1);
				}
				dispose();
			}
		});
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=0;
		buttonPanel.add(selectBtn,c);
		
		//close button
		cancelBtn = new JButton(Messages.getString("view.button.cancel"));
		cancelBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				dispose();
			}
		});
		c = new GridBagConstraints();
		c.gridx=1;
		c.gridy=0;
		buttonPanel.add(cancelBtn,c);
		
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=1;
		c.gridwidth=2;
		mainPanel.add(buttonPanel,c);
		
		pack();
	}
	
	public ResourceModel getSelectedResource(){
		return selectedResource;
	}

}
