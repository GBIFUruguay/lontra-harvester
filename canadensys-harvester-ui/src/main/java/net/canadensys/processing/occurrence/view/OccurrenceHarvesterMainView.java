package net.canadensys.processing.occurrence.view;

import java.awt.Color;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.filechooser.FileFilter;

import net.canadensys.processing.ItemProgressListenerIF;
import net.canadensys.processing.occurrence.controller.StepControllerIF;
import net.canadensys.processing.occurrence.model.ApplicationStatus;
import net.canadensys.processing.occurrence.model.ApplicationStatus.JobStatusEnum;
import net.canadensys.processing.occurrence.model.IPTFeedModel;
import net.canadensys.processing.occurrence.model.ImportLogModel;
import net.canadensys.processing.occurrence.model.ResourceModel;
import net.canadensys.processing.occurrence.view.model.HarvesterViewModel;

import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Main UI to control the harvester.
 * @author canadensys
 *
 */
@Component
public class OccurrenceHarvesterMainView implements ItemProgressListenerIF,PropertyChangeListener{
	
	private JFrame harvesterFrame = null;
	
	private JPanel mainPanel = null;
	private JTextField pathToImportTxt = null;
	private ResourceModel resourceToImport = null;
	
	//Action buttons
	private JButton openFileBtn = null;
	private JButton openResourceBtn = null;
	private JButton importBtn = null;
	private JButton moveToPublicBtn = null;
	
	private JTextField bufferSchemaTxt = null;
	
	private ImageIcon loadingImg = null;
	private JLabel loadingLbl = null;
	private JTextArea statuxTxtArea = null;
	
	private JButton viewImportLogBtn = null;
	private JButton viewIPTFeedBtn = null;
	
	@Autowired
	private HarvesterViewModel harvesterViewModel;
	
	@Autowired
	@Qualifier(value="stepController")
	private StepControllerIF stepController;
	
	public void initView(){
		loadingImg = new ImageIcon(OccurrenceHarvesterMainView.class.getResource("/ajax-loader.gif"));
		
		harvesterFrame = new JFrame(Messages.getString("view.title"));
		harvesterFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		mainPanel = new JPanel();
		mainPanel.setLayout(new GridBagLayout());
		pathToImportTxt = new JTextField();
		pathToImportTxt.setColumns(30);
		pathToImportTxt.setEditable(false);
		openFileBtn = new JButton(Messages.getString("view.button.openFile"));
		openResourceBtn = new JButton(Messages.getString("view.button.openResource"));
		openFileBtn.setEnabled(false);
		openFileBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onChooseFile();
			}
		});
		
		openResourceBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onOpenResourceBtn();
			}
		});
		
		importBtn = new JButton(Messages.getString("view.button.import"));
		importBtn.setToolTipText(Messages.getString("view.button.import.tooltip"));
		importBtn.setEnabled(false);
		importBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onImportResource();
			}
		});
		
		int lineIdx=0;
		GridBagConstraints c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.anchor = GridBagConstraints.CENTER;
		c.gridwidth = 3;
		JLabel lbl = new JLabel(Messages.getString("view.info.currentDatabase") + harvesterViewModel.getDatabaseLocation());
		lbl.setForeground(Color.BLUE);
		mainPanel.add(lbl,c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.anchor = GridBagConstraints.WEST;
		mainPanel.add(new JLabel(Messages.getString("view.info.import.dwca")),c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.fill = GridBagConstraints.HORIZONTAL;
		c.weightx = 0.8;
		mainPanel.add(pathToImportTxt,c);
		
		c = new GridBagConstraints();
		c.gridx=1;
		c.gridy=lineIdx;
		mainPanel.add(openResourceBtn,c);
		
		c = new GridBagConstraints();
		c.gridx=2;
		c.gridy=lineIdx;
		mainPanel.add(openFileBtn,c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		mainPanel.add(importBtn,c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.gridwidth = 3;
		c.fill = GridBagConstraints.HORIZONTAL;
		mainPanel.add(new JSeparator(),c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.gridwidth = 2;
		c.anchor = GridBagConstraints.WEST;
		mainPanel.add(new JLabel(Messages.getString("view.info.move.dwca")),c);
		
		//UI line break
		lineIdx++;
		moveToPublicBtn = new JButton(Messages.getString("view.button.move"));
		moveToPublicBtn.setToolTipText(Messages.getString("view.button.move.tooltip"));
		moveToPublicBtn.setEnabled(false);
		moveToPublicBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onMoveToPublic();
			}
		});
		c = new GridBagConstraints();
		c.gridx=2;
		c.gridy=lineIdx;
		c.anchor = GridBagConstraints.SOUTH;
		mainPanel.add(moveToPublicBtn ,c);
		
		bufferSchemaTxt = new JTextField();
		bufferSchemaTxt.setEnabled(false);
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 0.8;
		c.weighty = 0.8;
		mainPanel.add(bufferSchemaTxt,c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.gridwidth = 3;
		c.fill = GridBagConstraints.HORIZONTAL;
		mainPanel.add(new JSeparator(),c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.anchor = GridBagConstraints.WEST;
		mainPanel.add(new JLabel(Messages.getString("view.info.status")),c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.anchor = GridBagConstraints.WEST;
		loadingLbl = new JLabel(Messages.getString("view.info.status.waiting"),null, JLabel.CENTER);
		mainPanel.add(loadingLbl,c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.gridwidth = 3;
		c.fill = GridBagConstraints.HORIZONTAL;
		mainPanel.add(new JSeparator(),c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.gridwidth = 2;
		c.anchor = GridBagConstraints.WEST;
		mainPanel.add(new JLabel(Messages.getString("view.info.console")),c);
		
		//UI line break
		lineIdx++;
		statuxTxtArea = new JTextArea();
		statuxTxtArea.setRows(15);
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.gridwidth = 2;
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 1.0;
		c.weighty = 1.0;
		mainPanel.add(new JScrollPane(statuxTxtArea),c);
		
		//UI line break
		lineIdx++;
		viewImportLogBtn = new JButton(Messages.getString("view.button.viewImportLog"));
		viewImportLogBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onViewImportLog();
			}
		});
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.anchor = GridBagConstraints.WEST;
		mainPanel.add(viewImportLogBtn,c);
		
		//UI line break
		lineIdx++;
		viewIPTFeedBtn = new JButton(Messages.getString("view.button.viewIPTrss"));
		viewIPTFeedBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onViewIPTFeed();
			}
		});
		c = new GridBagConstraints();
		c.gridx = 0;
		c.gridy = lineIdx;
		c.anchor = GridBagConstraints.WEST;
		mainPanel.add(viewIPTFeedBtn, c);
		
		//inner panel
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=0;
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 1.0;
		c.weighty = 1.0;
		
		harvesterFrame.setLayout(new GridBagLayout());
		harvesterFrame.add(mainPanel,c);
		harvesterFrame.pack();
		harvesterFrame.setLocationRelativeTo(null);
		harvesterFrame.setVisible(true);
		
		redirectSystemStreams();
		stepController.registerProgressListener(this);
		harvesterViewModel.addPropertyChangeListener(this);
	}
	
	/**
	 * Open a FileChooser to import a specific DarwinCore archive
	 */
	private void onChooseFile(){
		JFileChooser fc = new JFileChooser();
		fc.setDialogTitle(Messages.getString("view.fileChooser.title"));
		fc.setMultiSelectionEnabled(false);
		fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
		fc.setFileFilter(new FileFilter() {
			@Override
			public String getDescription() {
				return Messages.getString("view.fileChooser.description");
			}
			@Override
			public boolean accept(File file) {
				if(FilenameUtils.getExtension(file.getName()).equalsIgnoreCase("zip")){
					return true;
				}
				return false;
			}
		});
		int returnVal = fc.showOpenDialog(harvesterFrame);
		if(returnVal == JFileChooser.APPROVE_OPTION){
			pathToImportTxt.setText(fc.getSelectedFile().getAbsolutePath());
			importBtn.setEnabled(true);
		}
	}
	
	/**
	 * Open a window to select a resource to import.
	 */
	private void onOpenResourceBtn(){
		ResourceChooser urlChooser = new ResourceChooser(stepController.getResourceModelList());
		urlChooser.setLocationRelativeTo(null);
		urlChooser.setVisible(true);
		
		ResourceModel selectedResource = urlChooser.getSelectedResource();
		if(selectedResource != null){
			pathToImportTxt.setText(selectedResource.getName());
			importBtn.setEnabled(true);
		}
		//remember the last selected resource
		resourceToImport = selectedResource;
	}
	
	/**
	 * Import a resource (asynchronously) using a SwingWorker.
	 */
	private void onImportResource(){
		openResourceBtn.setEnabled(false);
		importBtn.setEnabled(false);
		moveToPublicBtn.setEnabled(false);
		loadingLbl.setIcon(loadingImg);
		
		final SwingWorker<Void,Object> swingWorker = new SwingWorker<Void, Object>() {
		       @Override
		       public Void doInBackground() {
		    	   stepController.importDwcA(resourceToImport.getResource_id());
		    	   //async call, onImportDone(...) will be called once done
		    	   return null;
		       }
		       @Override
		       protected void done() {}
		};
		swingWorker.execute();
	}
	
	/**
	 * Move the previously importer resource to the public schema using a SwingWorker.
	 */
	private void onMoveToPublic(){
		moveToPublicBtn.setEnabled(false);
		loadingLbl.setIcon(loadingImg);
		
		final SwingWorker<Boolean,Object> swingWorker = new SwingWorker<Boolean, Object>() {
		       @Override
		       public Boolean doInBackground() {
					stepController.moveToPublicSchema(bufferSchemaTxt.getText());
					return true;
		       }

		       @Override
		       protected void done() {
		    	   try {
					if(get()){
						   onMoveDone(JobStatusEnum.DONE_SUCCESS);
					   }
					   else{
						   onMoveDone(JobStatusEnum.DONE_ERROR);
					   }
					} catch (InterruptedException e) {
						 onMoveDone(JobStatusEnum.DONE_ERROR);
					} catch (ExecutionException e) {
						 onMoveDone(JobStatusEnum.DONE_ERROR);
					}
		      }
		};
		swingWorker.execute();
	}
	
	private void onViewImportLog(){
		Vector<String> headers = new Vector<String>();
		headers.add(Messages.getString("view.importLog.table.sourceFileId"));
		headers.add(Messages.getString("view.importLog.table.recordQty"));
		headers.add(Messages.getString("view.importLog.table.updatedBy"));
		headers.add(Messages.getString("view.importLog.table.date"));
		
		ImportLogDialog dlg = new ImportLogDialog(headers);
		
		List<ImportLogModel> importLogModelList = stepController.getSortedImportLogModelList();
		dlg.loadData(importLogModelList);
		dlg.setLocationRelativeTo(null);
		dlg.setVisible(true);
	}
	
	private void onViewIPTFeed() {
		Vector<String> headers = new Vector<String>();
		headers.add(Messages.getString("view.iptFeed.table.title"));
		headers.add(Messages.getString("view.iptFeed.table.url"));
		headers.add(Messages.getString("view.iptFeed.table.key"));
		headers.add(Messages.getString("view.iptFeed.table.pubDate"));

		IPTFeedDialog dlg = new IPTFeedDialog(headers);

		List<IPTFeedModel> importLogModelList = stepController.getIPTFeed();
		dlg.loadData(importLogModelList);
		dlg.setLocationRelativeTo(null);
		dlg.setVisible(true);
	}
	
	private void onImportDone(JobStatusEnum status, String datasetShortName){
		loadingLbl.setIcon(null);
		 try {
      	   if(JobStatusEnum.DONE_SUCCESS.equals(status)){
      		   bufferSchemaTxt.setText(datasetShortName);
      		   moveToPublicBtn.setEnabled(true);
      		   
      		   SwingUtilities.invokeLater(new Runnable() {
      			    public void run() {
      			    	loadingLbl.setText(Messages.getString("view.info.status.importDone"));
      			    }
      		   });
      	   }
      	   else{
      		   JOptionPane.showMessageDialog(harvesterFrame, Messages.getString("view.info.status.error.details"), Messages.getString("OccurrenceHarvesterMainView.29"), JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$ //$NON-NLS-2$
      		   SwingUtilities.invokeLater(new Runnable() {
     			    public void run() {
     			    	loadingLbl.setText(Messages.getString("view.info.status.error.importError"));
     			    }
     		   });
      	   }
         } catch (Exception e) {
      	   e.printStackTrace();
      	   JOptionPane.showMessageDialog(harvesterFrame, Messages.getString("view.info.status.error.details"), Messages.getString("view.info.status.error"), JOptionPane.ERROR_MESSAGE);
  		   SwingUtilities.invokeLater(new Runnable() {
			    public void run() {
			    	loadingLbl.setText(Messages.getString("view.info.status.error.importError"));
			    }
		   });
         }
	}
	
	private void onMoveDone(JobStatusEnum status){
		loadingLbl.setIcon(null);
		if(JobStatusEnum.DONE_SUCCESS.equals(status)){
			JOptionPane.showMessageDialog(harvesterFrame, Messages.getString("view.info.status.moveCompleted"), Messages.getString("view.info.status.info"), JOptionPane.INFORMATION_MESSAGE);
			bufferSchemaTxt.setText("");
			pathToImportTxt.setText("");
			loadingLbl.setText(Messages.getString("view.info.status.moveDone"));
		}
		else{
			JOptionPane.showMessageDialog(harvesterFrame, Messages.getString("view.info.status.error.details"), Messages.getString("view.info.status.error"), JOptionPane.ERROR_MESSAGE);
			loadingLbl.setText(Messages.getString("view.info.status.error.moveError"));
		}
	}
	
	private void updateTextArea(final String text) {
		SwingUtilities.invokeLater(new Runnable() {
		    public void run() {
		    	statuxTxtArea.append(text);
		    }
		  });
	}
	
	/**
	 * 
	 */
	private void redirectSystemStreams() {
		  OutputStream out = new OutputStream() {
		    @Override
		    public void write(int b) throws IOException {
		      updateTextArea(String.valueOf((char) b));
		    }
		 
		    @Override
		    public void write(byte[] b, int off, int len) throws IOException {
		      updateTextArea(new String(b, off, len));
		    }
		 
		    @Override
		    public void write(byte[] b) throws IOException {
		      write(b, 0, b.length);
		    }
		  };
		 
		  System.setOut(new PrintStream(out, true));
		  System.setErr(new PrintStream(out, true));
	}

	@Override
	public void onProgress(final int current, final int total) {
		SwingUtilities.invokeLater(new Runnable() {
		    public void run() {
		    	loadingLbl.setText(current + "/" + total);
		    }
		  });
	}

	@Override
	public void propertyChange(PropertyChangeEvent evt) {
		if("applicationStatus".equals(evt.getPropertyName())){
			onImportDone(((ApplicationStatus)evt.getNewValue()).getImportStatus(),resourceToImport.getSource_file_id());
		}
	}
}
