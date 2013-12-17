package net.canadensys.processing.occurrence.reader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.canadensys.processing.ItemMapperIF;
import net.canadensys.processing.ItemReaderIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.gbif.dwc.terms.ConceptTerm;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.ArchiveFactory;
import org.gbif.dwc.text.ArchiveField;
import org.gbif.dwc.text.ArchiveFile;
import org.gbif.dwc.text.UnsupportedArchiveException;
import org.gbif.utils.file.ClosableIterator;

/**
 * Generic reader to read a DarwinCore Archive extension.
 * 
 * @author canadensys
 *
 * @param <T> object that will contain a line of the extension
 */
public class DwcaExtensionReader<T> implements ItemReaderIF<T>{

	private String dwcaFilePath = null;
	private String dwcaExtensionType = null;
	
	private ItemMapperIF<T> mapper;
	
	private ClosableIterator<String[]> rowsIt;
	private String[] headers;
	
	@Override
	public void openReader(Map<SharedParameterEnum, Object> sharedParameters) {
		dwcaFilePath = (String)sharedParameters.get(SharedParameterEnum.DWCA_PATH);
		dwcaExtensionType = (String)sharedParameters.get(SharedParameterEnum.DWCA_EXTENSION_TYPE);
		
		if(mapper == null){
			throw new IllegalStateException("No mapper defined");
		}
		if(StringUtils.isBlank(dwcaFilePath) || StringUtils.isBlank(dwcaExtensionType)){
			throw new IllegalStateException("sharedParameters missing: DWCA_PATH and DWCA_EXTENSION_TYPE are required.");
		}
		
		File dwcaFile = null;
		try {
			dwcaFile = new File(dwcaFilePath);
			Archive dwcArchive = ArchiveFactory.openArchive(dwcaFile);

			TermFactory tf = new TermFactory();
			ConceptTerm extType = tf.findTerm(dwcaExtensionType);
			ArchiveFile dwcaContentFile = dwcArchive.getExtension(extType);
			
			//get headers
			List<ArchiveField> sortedFieldList = dwcaContentFile.getFieldsSorted();
			ArrayList<String> indexedColumns = new ArrayList<String>();
			indexedColumns.add("id");
			String headerName;
			for(ArchiveField currArField : sortedFieldList){
				//skip default column
				if(currArField.getIndex() != null){
					//take the name lower case
					headerName = currArField.getTerm().simpleName().toLowerCase();
					indexedColumns.add(headerName);
				}
			}
			
			headers = indexedColumns.toArray(new String[0]);
						
			//get rows
			rowsIt = dwcaContentFile.getCSVReader().iterator();
			
		} catch (UnsupportedArchiveException e) {
			e.printStackTrace();	
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void closeReader() {
		rowsIt.close();
	}

	@Override
	public T read() {
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
		return mapper.mapElement(properties);
	}
	
	/**
	 * Set the row mapper to use to translate properties into object
	 * @param mapper
	 */
	public void setMapper(ItemMapperIF<T> mapper){
		this.mapper = mapper;
	}
	
	/**
	 * This method ensure that class of T (user defined model) can handle the data defined by the headers.
	 * This is handy only if the model is using DarwinCore terms as variable names. If the mapper is 
	 * responsible for doing some translation, this method should not be used.
	 * @param typeParameterClass
	 */
	protected void validateDwcaHeaders(Class<T> typeParameterClass){
		try {
			T obj = typeParameterClass.newInstance();
			for(String currHeader : headers){
				if(!PropertyUtils.isWriteable(obj, currHeader)){
					System.out.println("Property " + currHeader + " is not found or writeable in " + obj.getClass().getName());
				}
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}
}
