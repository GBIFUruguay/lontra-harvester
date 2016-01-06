package net.canadensys.harvester.occurrence.mapper;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.harvester.ItemMapperIF;
import net.canadensys.harvester.mapper.TermMapper;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

import com.google.common.collect.Maps;

/**
 * Map properties into OccurrenceRawModel.
 * Set the dwcaid using the "id" property and map 'sanitized' Strings to ensure proper behavior in the following steps.
 * 
 * @author canadensys
 * 
 */
public class OccurrenceMapper implements ItemMapperIF<OccurrenceRawModel> {
	// get log4j handler
	private static final Logger LOGGER = Logger.getLogger(OccurrenceMapper.class);

	private static TermMapper TERM_MAPPER = TermMapper.getInstance(); 
	private static TermFactory TF = TermFactory.instance();
	
	private static final char NULL_CHAR = '\0';

	@Override
	public OccurrenceRawModel mapElement(Map<String, Object> properties) {
		OccurrenceRawModel newOccurrenceRawModel = new OccurrenceRawModel();
		try {
			prepareProperties(properties);
			BeanUtils.populate(newOccurrenceRawModel, properties);
			BeanUtils.setProperty(newOccurrenceRawModel, "dwcaid", properties.get("id"));
		}
		catch (IllegalAccessException e) {
			LOGGER.error("Issue while mapping properties", e);
		}
		catch (InvocationTargetException e) {
			LOGGER.error("Issue while mapping properties", e);
		}
		return newOccurrenceRawModel;
	}

	/**
	 * Prepare the properties for mapping to OccurrenceRawModel.
	 * Sanitize the value if is a String. Remove characters like NUL char.
	 * 
	 * @param properties
	 */
	private void prepareProperties(Map<String, Object> properties) {
		Term term;
		Map<String, Object> toAdd = Maps.newHashMap();
		for (String key : properties.keySet()) {
			if (properties.get(key).getClass() == String.class) {
				if (properties.get(key).toString().indexOf(NULL_CHAR) >= 0) {
					properties.put(key, StringUtils.remove(properties.get(key).toString(), NULL_CHAR));
					LOGGER.warn("Some invalid characters were removed from the record identified with id=" + properties.get("id"));
				}
			}
			term = TF.findTerm(key);
			
			//if a term mapping exists, map the value to the mapped term
			if(TERM_MAPPER.getTermMapping(term) != null) {
				toAdd.put(TERM_MAPPER.getTermMapping(term), properties.get(key));
				
				if(properties.containsKey(TERM_MAPPER.getTermMapping(term))){
					LOGGER.error("The term " + term + " is defined in term mapping but is already present in source data");
				}
			}
		}
		properties.putAll(toAdd);
	}

}
