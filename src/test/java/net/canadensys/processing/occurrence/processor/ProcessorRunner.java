package net.canadensys.processing.occurrence.processor;

import java.util.Map;

import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;

public class ProcessorRunner {
	
	/**
	 * Helper class to ensure ItemProcessorIF are tested using init,process and destroy sequence.
	 * @param processor
	 * @param data
	 * @param sharedParameters
	 * @return
	 */
	static <T, S> S runItemProcessor(ItemProcessorIF<T,S> processor, T data, Map<SharedParameterEnum,Object> sharedParameters){
		processor.init();
		S result = processor.process(data, sharedParameters);
		processor.destroy();
		return result;
	}
}
