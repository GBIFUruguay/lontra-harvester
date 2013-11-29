package net.canadensys.processing;

import java.util.Map;

/**
 * Map a set of properties values to a specific object.
 * Could be used by a ItemReader to return an object from its source.
 * @author canadensys
 *
 * @param <T> type of returned object
 */
public interface ItemMapperIF<T> {
	//maybe a Map<String,Object> ? or <T,V>
	public T mapElement(Map<String,Object> properties);
}
