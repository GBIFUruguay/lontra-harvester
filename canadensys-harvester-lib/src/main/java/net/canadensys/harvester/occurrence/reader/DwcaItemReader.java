package net.canadensys.harvester.occurrence.reader;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.harvester.ItemMapperIF;
import net.canadensys.harvester.ItemReaderIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.mapper.OccurrenceMapper;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.ArchiveFactory;
import org.gbif.dwc.text.UnsupportedArchiveException;

/**
 * Item reader for Darwin Core Archive.
 * @author canadensys
 *
 */
public class DwcaItemReader extends AbstractDwcaReaderSupport implements ItemReaderIF<OccurrenceRawModel>{
	//get log4j handler
	private static final Logger LOGGER = Logger.getLogger(DwcaItemReader.class);
	
	private ItemMapperIF<OccurrenceRawModel> mapper = new OccurrenceMapper();

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
		if(mapper == null){
			throw new IllegalStateException("No mapper defined");
		}
		if(StringUtils.isBlank(dwcaFilePath)){
			throw new IllegalStateException("sharedParameters missing: DWCA_PATH is required.");
		}
		
		File dwcaFile = new File(dwcaFilePath);
		Archive dwcArchive;
		try {
			dwcArchive = ArchiveFactory.openArchive(dwcaFile);
			prepareReader(dwcArchive.getCore());
		} catch (UnsupportedArchiveException e) {
			LOGGER.fatal("Can't open DwcaItemReader", e);
		} catch (IOException e) {
			LOGGER.fatal("Can't open DwcaItemReader", e);
		}
		
		//make sure those headers can be imported correctly
		validateDwcaHeaders();
	}
	
	@Override
	public void closeReader(){
		super.closeReader();
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
