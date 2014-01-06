package net.canadensys.processing.occurrence.mapper;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.ItemMapperIF;

import org.apache.commons.beanutils.BeanUtils;

/**
 * Map properties into OccurrenceRawModel.
 * Set the dwcaid using the "id" property
 * @author canadensys
 *
 */
public class OccurrenceMapper implements ItemMapperIF<OccurrenceRawModel> {

	@Override
	public OccurrenceRawModel mapElement(Map<String,Object> properties) {
		OccurrenceRawModel newOccurrenceRawModel = new OccurrenceRawModel();
		try {
			BeanUtils.populate(newOccurrenceRawModel, properties);
			BeanUtils.setProperty(newOccurrenceRawModel, "dwcaid", properties.get("id"));
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return newOccurrenceRawModel;
	}
}
