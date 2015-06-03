package net.canadensys.harvester.model;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.log4j.Logger;

/**
 * Allows to group a list of Object(of the same class) under one Object storing properties in String arrays.
 * Main usage is JSON serialization where we will only print property names once.
 *
 * @author cgendreau
 *
 * @param <T>
 */
public class BulkDataObject<T> {

	private static final Logger LOGGER = Logger.getLogger(BulkDataObject.class);

	private List<String> fieldNames;
	private List<String[]> data;

	public BulkDataObject() {
	}

	public BulkDataObject(List<String> fieldNames) {
		this.fieldNames = fieldNames;
		data = new ArrayList<String[]>();
	}

	/**
	 * Add a new Object of instance <T> to the bulk.
	 * Reflection is used to extract properties based on fieldNames.
	 *
	 * @param obj
	 */
	@SuppressWarnings("unchecked")
	public void addObject(T obj) throws IllegalArgumentException {
		try {
			Map<String, String> objDescription = BeanUtilsBean.getInstance().describe(obj);
			String[] objData = new String[fieldNames.size()];
			int i = 0;
			for (String fieldName : fieldNames) {
				if (!objDescription.containsKey(fieldName)) {
					throw new IllegalArgumentException("Can't add object to BulkDataObject: " + fieldName + " is not a valid field name of "
							+ obj.getClass());
				}
				objData[i] = objDescription.get(fieldName);
				i++;
			}
			data.add(objData);
		}
		catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
			throw new IllegalArgumentException("Can't add object to BulkDataObject", ex);
		}
	}

	/**
	 * Add a data line to the current bulk.
	 * The order and the size shall respect the fieldNames list.
	 *
	 * @param obj
	 */
	public void addData(String[] _data) {
		if (fieldNames == null || _data.length != fieldNames.size()) {
			throw new IllegalArgumentException("Size of _data must match fieldNames size.");
		}
		data.add(_data);
	}

	/**
	 * Rebuild an object from the properties.
	 *
	 * @param index
	 *            index of the object to retrieve.
	 * @param obj
	 *            instance <T> to fill with the properties
	 * @return same instance received in parameter.
	 */
	public T retrieveObject(int index, T obj) {
		try {
			HashMap<String, String> properties = new HashMap<String, String>(fieldNames.size());

			int idx = 0;
			String objData[] = data.get(index);
			for (String propName : fieldNames) {
				properties.put(propName, objData[idx]);
				idx++;
			}
			BeanUtilsBean.getInstance().populate(obj, properties);
		}
		catch (IllegalAccessException e) {
			LOGGER.error("Can't populate object from BulkDataObject", e);
		}
		catch (InvocationTargetException e) {
			LOGGER.error("Can't populate object from BulkDataObject", e);
		}
		return obj;
	}

	public List<String> getFieldNames() {
		return fieldNames;
	}

	public void setFieldNames(List<String> fieldNames) {
		this.fieldNames = fieldNames;
	}

	public List<String[]> getData() {
		return data;
	}

	public void setData(List<String[]> data) {
		this.data = data;
	}

}
