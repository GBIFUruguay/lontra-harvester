package net.canadensys.harvester.occurrence.step;

import java.util.Calendar;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.harvester.ItemProcessorIF;
import net.canadensys.harvester.ItemReaderIF;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.ProcessingStepIF;
import net.canadensys.harvester.message.ProcessingMessageIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.message.ProcessOccurrenceMessage;
import net.canadensys.harvester.occurrence.message.SaveRawOccurrenceMessage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Step reading a DarwinCore line, process the line, writing the processed lines at a fixed interval as ProcessingMessageIF.
 * NOT thread safe
 * @author canadensys
 *
 */
public class StreamDwcContentStep implements ProcessingStepIF{
	
	private static final int FLUSH_INTERVAL = 100;
	
	@Autowired
	@Qualifier("dwcItemReader")
	private ItemReaderIF<OccurrenceRawModel> reader;
	
	@Autowired
	@Qualifier("jmsWriter")
	private ItemWriterIF<ProcessingMessageIF> writer;
	
	@Autowired
	@Qualifier("lineProcessor")
	private ItemProcessorIF<OccurrenceRawModel, OccurrenceRawModel> lineProcessor;
	
	private int numberOfRecords = 0;
	private Map<SharedParameterEnum,Object> sharedParameters;
	
	@Override
	public void preStep(Map<SharedParameterEnum,Object> sharedParameters) throws IllegalStateException {
		if(writer == null){
			throw new IllegalStateException("No writer defined");
		}
		if(lineProcessor == null){
			throw new IllegalStateException("No processor defined");
		}
		if(reader == null){
			throw new IllegalStateException("No reader defined");
		}
		this.sharedParameters = sharedParameters;
		reader.openReader(sharedParameters);
		writer.openWriter();
		lineProcessor.init();
	}

	@Override
	public void postStep() {
		writer.closeWriter();
		lineProcessor.destroy();
		reader.closeReader();
	}

	@Override
	public void doStep() {
		SaveRawOccurrenceMessage rom = new SaveRawOccurrenceMessage();
		ProcessOccurrenceMessage com = new ProcessOccurrenceMessage();
		
		long t= System.currentTimeMillis();
		OccurrenceRawModel currRawModel = reader.read();
		while(currRawModel != null){
			currRawModel = lineProcessor.process(currRawModel, sharedParameters);

			//should be done by ChunkSplitter
			rom.addRawModel(currRawModel);
			rom.setWhen(Calendar.getInstance().getTime().toString());
			com.addRawModel(currRawModel);
			com.setWhen(Calendar.getInstance().getTime().toString());
			
			currRawModel = reader.read();
			numberOfRecords++;
			
			if(numberOfRecords % FLUSH_INTERVAL == 0){
				writer.write(rom);
				writer.write(com);
				rom = new SaveRawOccurrenceMessage();
				rom.setWhen(Calendar.getInstance().getTime().toString());
				com = new ProcessOccurrenceMessage();
				com.setWhen(Calendar.getInstance().getTime().toString());
			}
		}
		System.out.println("Streaming the file took :" + (System.currentTimeMillis()-t) + " ms");
		
		//flush remaining content
		if(rom.getRawModelList().size() > 0){
			writer.write(rom);
			writer.write(com);
		}
		
		sharedParameters.put(SharedParameterEnum.NUMBER_OF_RECORDS,numberOfRecords);
	}
	
	public int getNumberOfRecords(){
		return numberOfRecords;
	}
	
	public void setReader(ItemReaderIF<OccurrenceRawModel> reader) {
		this.reader = reader;
	}
	public void setWriter(ItemWriterIF<ProcessingMessageIF> writer) {
		this.writer = writer;
	}
	public void setDwcaLineProcessor(
			ItemProcessorIF<OccurrenceRawModel, OccurrenceRawModel> lineProcessor) {
		this.lineProcessor = lineProcessor;
	}
}
