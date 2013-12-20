package net.canadensys.processing.occurrence.reader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.ItemMapperIF;
import net.canadensys.processing.ItemReaderIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.mapper.OccurrenceMapper;

import org.apache.commons.beanutils.PropertyUtils;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.ArchiveFactory;
import org.gbif.dwc.text.ArchiveField;
import org.gbif.dwc.text.ArchiveFile;
import org.gbif.dwc.text.UnsupportedArchiveException;
import org.gbif.utils.file.ClosableIterator;

/**
 * Item reader for Darwin Core Archive
 * @author canadensys
 *
 */
public class DwcaItemReader implements ItemReaderIF<OccurrenceRawModel>{
	
	//TODO should be configurable
	static final Map<String,String> RESERVED_WORDS = new HashMap<String, String>();
	static{
		RESERVED_WORDS.put("class", "_class");
		RESERVED_WORDS.put("group", "_group");
		RESERVED_WORDS.put("order", "_order");
		RESERVED_WORDS.put("references", "_references");
	}
	
	private String dwcaFilePath = null;
	private String[] headers;
	private Map<String,String> defaultValues = null;
	
	private ItemMapperIF<OccurrenceRawModel> mapper = new OccurrenceMapper();
	
	private ClosableIterator<String[]> rowsIt;

	@Override
	public OccurrenceRawModel read(){
		
		if(!rowsIt.hasNext()){
			return null;
		}
		
		//ImmutableMap from Google Collections?
		Map<String,Object> properties = new HashMap<String, Object>();
		int i=0;
		String[] data = rowsIt.next();
		for(String currHeader : headers){
			properties.put(currHeader, data[i]);
			i++;
		}
		//check if some default values must be handled
		if(defaultValues != null){
			for(String defaultValueCol : defaultValues.keySet()){
				properties.put(defaultValueCol, defaultValues.get(defaultValueCol));
			}
		}
		return mapper.mapElement(properties);
	}

	@Override
	public void openReader(Map<SharedParameterEnum,Object> sharedParameters){
		dwcaFilePath = (String)sharedParameters.get(SharedParameterEnum.DWCA_PATH);
		
		File dwcaFile = null;
		try {
			dwcaFile = new File(dwcaFilePath);
			Archive dwcArchive = ArchiveFactory.openArchive(dwcaFile);
			ArchiveFile dwcaCore = dwcArchive.getCore();
			
			//get headers
			List<ArchiveField> sortedFieldList = dwcaCore.getFieldsSorted();
			ArrayList<String> indexedColumns = new ArrayList<String>();
			indexedColumns.add("id");
			for(ArchiveField currArField : sortedFieldList){
				//check if the field is a default column or not
				if(currArField.getIndex() != null){
					indexedColumns.add(getHeaderName(currArField));
				}
				else{
					//lazy init, do not create if not needed for this archive
					if(defaultValues == null){
						defaultValues = new HashMap<String, String>();
					}
					defaultValues.put(getHeaderName(currArField), currArField.getDefaultValue());
				}
			}
			headers = indexedColumns.toArray(new String[0]);
			
			//make sure those headers can be imported correctly
			validateDwcaHeaders();
			
			//get rows
			rowsIt = dwcaCore.getCSVReader().iterator();
			
		} catch (UnsupportedArchiveException e) {
			e.printStackTrace();	
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void closeReader(){
		rowsIt.close();
	}
	
	/**
	 * Handle reserved word and lowercase header from ArchiveField.
	 * @param archiveField
	 * @return
	 */
	private String getHeaderName(ArchiveField archiveField){
		String headerName = archiveField.getTerm().simpleName().toLowerCase();
		if(RESERVED_WORDS.get(headerName) != null){
			headerName = RESERVED_WORDS.get(headerName);
		}
		return headerName;
	}
	
	private void validateDwcaHeaders(){
		OccurrenceRawModel testModel = new OccurrenceRawModel();
		for(String currHeader : headers){
			if(!PropertyUtils.isWriteable(testModel, currHeader)){
				System.out.println("Property " + currHeader + " is not found or writeable in OccurrenceModel");
			}
		}
	}

}
