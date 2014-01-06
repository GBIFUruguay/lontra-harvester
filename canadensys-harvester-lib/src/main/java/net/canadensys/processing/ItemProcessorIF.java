package net.canadensys.processing;

import java.util.Map;

import net.canadensys.processing.exception.ProcessException;
import net.canadensys.processing.occurrence.SharedParameterEnum;

/**
 * Item processing interface
 * @author canadensys
 *
 * @param <T> source type of item to process
 * @param <V> result type of item to process
 */
public interface ItemProcessorIF<T,V> {
	
	/**
	 * Initialization of the ItemProcessor before processing
	 */
	public void init();
	
	/**
	 * Process object T into object V
	 * @param data source object
	 * @param sharedParameters see concrete ItemProcessor documentation
	 * @return
	 */
	public V process(T data, Map<SharedParameterEnum,Object> sharedParameters) throws ProcessException;
	
	/**
	 * Clean up of the ItemProcessor after processing
	 */
	public void destroy();
}
