package net.canadensys.harvester.occurrence.controller;

import net.canadensys.harvester.jms.control.JMSControlConsumer;
import net.canadensys.harvester.jms.control.JMSControlConsumerMessageHandlerIF;
import net.canadensys.harvester.message.ControlMessageIF;
import net.canadensys.harvester.message.control.NodeErrorControlMessage;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Controller that will received notifications from nodes in case of error.
 * @author canadensys
 *
 */
@Component("nodeStatusController")
public class NodeStatusController implements JMSControlConsumerMessageHandlerIF{

	private static final Logger LOGGER = Logger.getLogger(NodeStatusController.class);

	@Autowired
	private JMSControlConsumer errorReceiver;

	@Autowired
	@Qualifier("stepController")
	private StepControllerIF parentCtrl;

	public NodeStatusController(){}

	@Override
	public Class<?> getMessageClass() {
		return NodeErrorControlMessage.class;
	}

	@Override
	public boolean handleMessage(ControlMessageIF message) {
		NodeErrorControlMessage errorCtrlMsg = (NodeErrorControlMessage)message;
		System.out.println("ERROR receive from node " + message.getNodeIdentifier() + ". See log for details");
		LOGGER.fatal("Received from node "+errorCtrlMsg.getNodeIdentifier(), errorCtrlMsg.getEnclosedException());
		return true;
	}

	public void start(){
		errorReceiver.registerHandler(this);
		errorReceiver.open();
	}
}
