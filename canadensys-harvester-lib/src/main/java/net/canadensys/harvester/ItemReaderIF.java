package net.canadensys.harvester;

import java.util.Map;

import net.canadensys.harvester.occurrence.SharedParameterEnum;


/**
 * Item reading interface
 * @author canadensys
 *
 * @param <T> type of object to read
 */
public interface ItemReaderIF<T> {
	
	public void openReader(Map<SharedParameterEnum,Object> sharedParameters);
	
	public void closeReader();
	
	public T read();
}
