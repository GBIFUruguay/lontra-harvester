package net.canadensys.processing.occurrence.processor;

import java.util.Map;

import net.canadensys.dataportal.occurrence.model.ResourceContactModel;
import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.exception.ProcessException;
import net.canadensys.processing.exception.TaskExecutionException;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.apache.log4j.Logger;
import org.gbif.metadata.eml.Address;
import org.gbif.metadata.eml.Agent;
import org.gbif.metadata.eml.Eml;

/**
 * Process org.gbif.metadata.eml.Eml to extract info into a ResourceContactModel
 * @author canadensys
 *
 */
public class ResourceContactProcessor implements ItemProcessorIF<Eml, ResourceContactModel>{

	//get log4j handler
	private static final Logger LOGGER = Logger.getLogger(ResourceContactProcessor.class);
			
	@Override
	public void init() {
		
	}

	@Override
	public ResourceContactModel process(Eml eml, Map<SharedParameterEnum, Object> sharedParameters)
			throws ProcessException {
		String datasetShortname = (String)sharedParameters.get(SharedParameterEnum.DATASET_SHORTNAME);
        
        if(datasetShortname == null){
			LOGGER.fatal("Misconfigured processor : needs  sourceFileId");
			throw new TaskExecutionException("Misconfigured processor");
		}
        Agent agent = eml.getContact();
        ResourceContactModel resourceContactModel = new ResourceContactModel();
        resourceContactModel.setDataset_shortname(datasetShortname);
        resourceContactModel.setDataset_title(eml.getTitle());
        resourceContactModel.setName(agent.getFullName());
        resourceContactModel.setPosition_name(agent.getPosition());
        resourceContactModel.setOrganization_name(agent.getOrganisation());
        
        Address agentAddress = agent.getAddress();
        resourceContactModel.setAddress(agentAddress.getAddress());
        resourceContactModel.setCity(agentAddress.getCity());
        resourceContactModel.setAdministrative_area(agentAddress.getProvince());
        resourceContactModel.setPostal_code(agentAddress.getPostalCode());
        resourceContactModel.setCountry(agentAddress.getCountry());
        
        resourceContactModel.setEmail(agent.getEmail());
        resourceContactModel.setPhone(agent.getPhone());
        
		return resourceContactModel;
	}

	@Override
	public void destroy() {
		
	}
}
