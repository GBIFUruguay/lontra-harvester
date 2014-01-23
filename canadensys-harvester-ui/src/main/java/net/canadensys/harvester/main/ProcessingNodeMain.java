package net.canadensys.harvester.main;

import java.util.ArrayList;
import java.util.List;

import net.canadensys.harvester.ProcessingStepIF;
import net.canadensys.harvester.config.ProcessingNodeConfig;
import net.canadensys.harvester.jms.JMSConsumer;
import net.canadensys.harvester.jms.JMSConsumerMessageHandlerIF;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Processing node main class.
 * 
 * @author canadensys
 * 
 */
@Component
public class ProcessingNodeMain {
	// TODO move port to config
	private static final String IP = "tcp://%s:61616";

	@Autowired
	private JMSConsumer jmsConsumer;

	@Autowired
	@Qualifier("insertRawOccurrenceStep")
	private ProcessingStepIF insertRawOccurrenceStep;

	@Autowired
	private ProcessingStepIF processInsertOccurrenceStep;

	@Autowired
	private ProcessingStepIF insertResourceContactStep;

	private final List<ProcessingStepIF> registeredSteps;
	private final List<JMSConsumerMessageHandlerIF> registeredMsgHandlers;

	public ProcessingNodeMain() {
		registeredMsgHandlers = new ArrayList<JMSConsumerMessageHandlerIF>();
		registeredSteps = new ArrayList<ProcessingStepIF>();
	}

	/**
	 * Initiate the node and start listening for messages.
	 * 
	 * @param brokerURL
	 *            a new broker URL or null to use the default one
	 * @param additionalMessageHandler
	 *            user defined message handler (optional)
	 */
	public <T extends JMSConsumerMessageHandlerIF & ProcessingStepIF> void initiate(
			String brokerURL, List<T> additionalMessageHandler) {
		// check if we need to set a new broker URL
		if (StringUtils.isNotBlank(brokerURL)) {
			jmsConsumer.setBrokerURL(brokerURL);
		}

		System.out.println("Broker location : " + jmsConsumer.getBrokerUrl());

		// Declare step handlers (maybe this should be configurable?)
		registeredMsgHandlers
				.add((JMSConsumerMessageHandlerIF) insertRawOccurrenceStep);
		registeredSteps.add(insertRawOccurrenceStep);

		registeredMsgHandlers
				.add((JMSConsumerMessageHandlerIF) processInsertOccurrenceStep);
		registeredSteps.add(processInsertOccurrenceStep);

		registeredMsgHandlers
				.add((JMSConsumerMessageHandlerIF) insertResourceContactStep);
		registeredSteps.add(insertResourceContactStep);

		if (additionalMessageHandler != null) {
			registeredMsgHandlers.addAll(additionalMessageHandler);
			registeredSteps.addAll(additionalMessageHandler);
		}

		// Register all message handlers to the JMS consumer
		for (JMSConsumerMessageHandlerIF currMsgHandler : registeredMsgHandlers) {
			jmsConsumer.registerHandler(currMsgHandler);
		}

		// Then, init all the steps
		for (ProcessingStepIF currStep : registeredSteps) {
			// due to the async behavior, we do not use any sharedParameters (at
			// least for now)
			currStep.preStep(null);
		}

		// TODO register postStep calls

		jmsConsumer.open();
	}

	/**
	 * Processing node entry point
	 * 
	 * @param newBrokerIp
	 */
	public static void main(String newBrokerIp) {
		String newBrokerUrl = null;
		if (StringUtils.isNotBlank(newBrokerIp)) {
			newBrokerUrl = String.format(IP, newBrokerIp);
		}

		// prepare to allow additional (user defined) messageHandler defined as
		// step
		List<MessageHandlerStep> additionalMessageHandler = null;

		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(
				ProcessingNodeConfig.class);
		ProcessingNodeMain processingNodeBean = ctx
				.getBean(ProcessingNodeMain.class);
		processingNodeBean.initiate(newBrokerUrl, additionalMessageHandler);
	}

	/**
	 * Inner class to satisfy the 2 interfaces list of ProcessingNodeMain
	 * initiate method. TODO: Not very elegant, maybe define a real interface.
	 * 
	 * @author canadensys
	 * 
	 */
	private interface MessageHandlerStep extends JMSConsumerMessageHandlerIF,
			ProcessingStepIF {
	}
}
