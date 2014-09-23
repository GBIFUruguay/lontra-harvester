package net.canadensys.harvester.exception;

import net.canadensys.harvester.ItemProcessorIF;

/**
 * Exception that could be thrown while processing
 * 
 * @see ItemProcessorIF
 * @author canadensys
 * 
 */
public class ProcessException extends RuntimeException {

	private static final long serialVersionUID = -5299440133723660379L;

	public ProcessException(String message) {
		super(message);
	}
}
