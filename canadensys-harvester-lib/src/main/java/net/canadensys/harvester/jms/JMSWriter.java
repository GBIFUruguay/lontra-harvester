package net.canadensys.harvester.jms;

import java.util.List;

import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.message.ProcessingMessageIF;

public class JMSWriter extends JMSProducer implements ItemWriterIF<ProcessingMessageIF>{

	public JMSWriter(String brokerURL){
		super(brokerURL);
	}
	
	@Override
	public void openWriter() {
		init();
	}
	
	@Override
	public void closeWriter() {
		close();
	}

	@Override
	public void write(List<? extends ProcessingMessageIF> elementList) {
		for(ProcessingMessageIF currMsg : elementList){
			send(currMsg);
		}
	}

	@Override
	public void write(ProcessingMessageIF element) {
		send(element);
	}

}
