package net.canadensys.harvester.exception;

import net.canadensys.harvester.ItemWriterIF;

/**
 * Exception that could be thrown while writing data.
 * @see ItemWriterIF
 * @author canadensys
 *
 */
public class WriterException extends Exception {

	private static final long serialVersionUID = 2581413339167696241L;

	public WriterException(String id, String message){
		super("id:"+id +","+message);
	}
	
	public WriterException(String id, String message, Throwable cause){
		super("id:"+id +","+message, cause);
	}
}
