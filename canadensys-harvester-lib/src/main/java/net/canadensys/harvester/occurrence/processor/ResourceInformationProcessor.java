package net.canadensys.harvester.occurrence.processor;

import java.util.List;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.ResourceContactModel;
import net.canadensys.dataportal.occurrence.model.ResourceInformationModel;
import net.canadensys.harvester.ItemProcessorIF;
import net.canadensys.harvester.exception.ProcessException;
import net.canadensys.harvester.exception.TaskExecutionException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.apache.log4j.Logger;
import org.gbif.metadata.eml.Agent;
import org.gbif.metadata.eml.Eml;
import org.gbif.metadata.eml.KeywordSet;

/**
 * Process org.gbif.metadata.eml.Eml to extract info into a
 * ResourceInformationModel
 * 
 * @author canadensys
 * 
 */
public class ResourceInformationProcessor implements ItemProcessorIF<Eml, ResourceInformationModel> {

	// get log4j handler
	private static final Logger LOGGER = Logger.getLogger(ResourceInformationProcessor.class);
	
	private static final String CONTACT = "contact";
	private static final String AGENT = "agent";
	private static final String METADATA_PROVIDER = "metadata_provider";
	private static final String RESOURCE_CREATOR = "resource_creator";

	@Override
	public void init() {

	}

	@Override
	public ResourceInformationModel process(Eml eml, Map<SharedParameterEnum, Object> sharedParameters) throws ProcessException {

		ResourceInformationModel information = null;

		String resourceUuid = (String) sharedParameters.get(SharedParameterEnum.RESOURCE_UUID);
		if (resourceUuid == null) {
			LOGGER.fatal("Misconfigured processor: needs resource_uuid");
			throw new TaskExecutionException("Misconfigured ResourceInformationProcessor");
		}
		// Check if the resource_uuid matches the eml field to ensure what is harvested is what is expected:
		if (eml.getGuid().equalsIgnoreCase(resourceUuid)) {
			information = new ResourceInformationModel();

			/* Set information data from EML file: */
			information.set_abstract(eml.getAbstract());

			// Fetch only first identifier available:
			List<String> alternateIdentifiers = eml.getAlternateIdentifiers();
			if (!alternateIdentifiers.equals(null) && !alternateIdentifiers.isEmpty()) {
				information.setAlternate_identifier(alternateIdentifiers.get(0));
			}
			information.setCitation(eml.getCitationString());
			information.setCollection_identifier(eml.getCollectionId());
			information.setCollection_name(eml.getCollectionName());
			information.setHierarchy_level(eml.getHierarchyLevel());
			information.setIntellectual_rights(eml.getIntellectualRights());
			// Fetch only the first keywords/thesaurus available:
			List<KeywordSet> keyList = eml.getKeywords();
			if (!keyList.equals(null) && !keyList.isEmpty()) {
				KeywordSet keywordSet = keyList.get(0);
				information.setKeyword(keywordSet.getKeywordsString());
				information.setKeyword_thesaurus(keywordSet.getKeywordThesaurus());
			}
			information.setLanguage(eml.getLanguage());
			information.setParent_collection_identifier(eml.getParentCollectionId());
			information.setPublication_date(eml.getPubDate());
			information.setResource_logo_url(eml.getLogoUrl());
			// TODO: verify what field should relate to this: 
			information.setResource_name("");
			information.setResource_uuid(eml.getGuid());
			information.setTitle(eml.getTitle());

			// Add resource contacts information:
			Agent tempAgent = null;
			ResourceContactModel tempContact = null;
			// Resource_uuid:
			String resource_uuid = eml.getGuid();
			// Add contact agent:
			tempAgent = eml.getContact();
			if (tempAgent != null) {
				tempContact = setContactFromAgent(tempAgent, resource_uuid, CONTACT);
				information.addContact(tempContact);
			}	
			// Add resource metadata provider information:
			tempAgent = eml.getMetadataProvider();
			if (tempAgent != null) {
				tempContact = setContactFromAgent(tempAgent, resource_uuid, METADATA_PROVIDER); 
				information.addContact(tempContact);
			}
			// Add resource creator information:
			tempAgent = eml.getResourceCreator();
			if (tempAgent != null) {
				tempContact = setContactFromAgent(tempAgent, resource_uuid, RESOURCE_CREATOR);
				information.addContact(tempContact);
			}
			// Add associatedParties information:
			for (Agent a : eml.getAssociatedParties()) {
				tempContact = setContactFromAgent(a, resource_uuid, AGENT);
				information.addContact(tempContact);
			}
		}
		return information;
	}

	/**
	 * Configure each ResourceContactModel based on an EML Agent, setting it's proper resource_uuid and contact_type  
	 * @param agent
	 * @param resource_uuid
	 * @param contact_type
	 * @return
	 */
	private ResourceContactModel setContactFromAgent(Agent agent, String resource_uuid, String contact_type) {
		ResourceContactModel contact = new ResourceContactModel();
		contact.setAddress(agent.getAddress().getAddress());
		contact.setAdministrative_area(agent.getAddress().getProvince());
		contact.setCity(agent.getAddress().getCity());
		contact.setCountry(agent.getAddress().getCountry());
		contact.setEmail(agent.getEmail());
		contact.setName(agent.getFullName());
		contact.setOrganization_name(agent.getOrganisation());
		contact.setPhone(agent.getPhone());
		contact.setPosition_name(agent.getPosition());
		contact.setPostal_code(agent.getAddress().getPostalCode());		
		contact.setContact_type(contact_type);
		contact.setResource_uuid(resource_uuid);
		return contact;
	}
	@Override
	public void destroy() {
	}
}
