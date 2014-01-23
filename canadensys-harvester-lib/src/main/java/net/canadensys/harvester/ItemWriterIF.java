package net.canadensys.harvester;

import java.util.List;

import net.canadensys.harvester.exception.WriterException;

/**
 * Item writing interface
 * @author canadensys
 *
 * @param <T> type of object to write
 */
public interface ItemWriterIF<T> {
	public void openWriter();
	public void closeWriter();
	
	public void write(List<? extends T> elementList) throws WriterException;
	public void write(T element) throws WriterException;
}

