package net.canadensys.processing.occurrence.mock.writer;

import java.util.ArrayList;
import java.util.List;

import net.canadensys.processing.ItemWriterIF;

/**
 * Used in unit test to accumulate T objects
 * @author canadensys
 *
 * @param <T>
 */
public class MockObjectWriter<T> implements ItemWriterIF<T>{
	
	private List<T> content;
	
	@Override
	public void openWriter() {
		content = new ArrayList<T>();
	}

	@Override
	public void closeWriter() {}

	@Override
	public void write(List<? extends T> elementList) {
	}

	@Override
	public void write(T element) {
		content.add(element);
	}

	public List<T> getContent(){
		return content;
	}
}
