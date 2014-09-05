package net.canadensys.harvester.occurrence.reader;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import net.canadensys.harvester.ItemMapperIF;
import net.canadensys.harvester.ItemReaderIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.ArchiveFactory;
import org.gbif.dwc.text.UnsupportedArchiveException;

/**
 * Generic reader to read a DarwinCore Archive extension.
 * 
 * @author canadensys
 *
 * @param <T> object that will contain a line of the extension
 */
public class DwcaExtensionReader<T> extends AbstractDwcaReaderSupport implements ItemReaderIF<T>{

	private static final Logger LOGGER = Logger.getLogger(DwcaExtensionReader.class);
	
	private final AtomicBoolean canceled = new AtomicBoolean(false);
	private String dwcaExtensionType = null;
	private ItemMapperIF<T> mapper;
	
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
		
		File dwcaFile = new File(dwcaFilePath);
		Archive dwcArchive;
		try {
			dwcArchive = ArchiveFactory.openArchive(dwcaFile);
			prepareReader(dwcArchive.getExtension(dwcaExtensionType,false));
		} catch (UnsupportedArchiveException e) {
			LOGGER.fatal("Can't open DwcaExtensionReader", e);
		} catch (IOException e) {
			LOGGER.fatal("Can't open DwcaExtensionReader", e);
		}
	}

	@Override
	public void closeReader() {
		super.closeReader();
	}

	@Override
	public T read() {
		if(canceled.get() || !rowsIt.hasNext()){
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

	@Override
	public void abort() {
		canceled.set(true);
	}
}
