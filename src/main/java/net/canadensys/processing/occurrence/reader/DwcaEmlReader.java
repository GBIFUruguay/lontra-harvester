package net.canadensys.processing.occurrence.reader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import net.canadensys.processing.ItemReaderIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.ArchiveFactory;
import org.gbif.dwc.text.UnsupportedArchiveException;
import org.gbif.metadata.eml.Eml;
import org.gbif.metadata.eml.EmlFactory;
import org.xml.sax.SAXException;

/**
 * Item reader for an EML file inside a DarwinCore Archive
 * @author canadensys
 *
 */
public class DwcaEmlReader implements ItemReaderIF<Eml>{

	private String dwcaFilePath = null;
	private Eml eml = null;
	
	@Override
	public Eml read(){
		Eml tmpEml = eml;
		//the read method act like an iterator so we only return the eml once
		if(eml != null){
			eml = null;
		}
		return tmpEml;
	}

	@Override
	public void openReader(Map<SharedParameterEnum,Object> sharedParameters){
		dwcaFilePath = (String)sharedParameters.get(SharedParameterEnum.DWCA_PATH);
		
		File dwcaFile = null;
		try {
			dwcaFile = new File(dwcaFilePath);
			Archive dwcArchive = ArchiveFactory.openArchive(dwcaFile);
			eml = EmlFactory.build(new FileInputStream(dwcArchive.getMetadataLocationFile()));
		} catch (UnsupportedArchiveException e) {
			e.printStackTrace();	
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void closeReader(){
		System.out.println("### CLOSE - DwcaEmlReader ###");
	}
}
