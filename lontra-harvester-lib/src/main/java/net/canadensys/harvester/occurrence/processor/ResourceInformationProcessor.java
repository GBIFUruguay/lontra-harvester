package net.canadensys.harvester.occurrence.processor;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import net.canadensys.dataportal.occurrence.model.ContactModel;
import net.canadensys.dataportal.occurrence.model.ResourceMetadataModel;
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
public class ResourceInformationProcessor implements ItemProcessorIF<Eml, ResourceMetadataModel> {

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
	public ResourceMetadataModel process(Eml eml, Map<SharedParameterEnum, Object> sharedParameters) throws ProcessException {

		ResourceMetadataModel metadata = null;

		String resourceUuid = (String) sharedParameters.get(SharedParameterEnum.RESOURCE_UUID);
		Integer resourceId = (Integer) sharedParameters.get(SharedParameterEnum.RESOURCE_ID);
		if (resourceUuid == null || resourceId == null) {
			LOGGER.fatal("Misconfigured ResourceInformationProcessor: resource_uuid and resource_id are required");
			throw new TaskExecutionException("Misconfigured ResourceInformationProcessor");
		}
		String guid = eml.getGuid();
		// Guid is not the UUID, fetch from alternative identifiers:
		if (eml.getGuid().startsWith("http")) {
			for (String ai : eml.getAlternateIdentifiers()) {
				if (!ai.startsWith("http")) {
					// Sanity UUID check:
					UUID uuid = UUID.fromString(ai);
					if (uuid.toString().equals(ai)) {
						guid = ai;
						break;
					}
				}
			}
		} // Guid is the UUID:
		else {
			UUID uuid = UUID.fromString(guid);
			if (!uuid.toString().equals(guid)) {
				throw new ProcessException("Alternate identifier didn't provide a proper UUID");
			}
		}
		// Check if the resource_uuid matches the eml field to ensure what is harvested is what is expected:
		if (guid.equalsIgnoreCase(resourceUuid)) {
			metadata = new ResourceMetadataModel();
			metadata.setDwca_resource_id(resourceId);

			/* Set information data from EML file: */
			metadata.set_abstract(eml.getAbstract());

			// Fetch only first identifier available:
			List<String> alternateIdentifiers = eml.getAlternateIdentifiers();
			if (!alternateIdentifiers.equals(null) && !alternateIdentifiers.isEmpty()) {
				metadata.setAlternate_identifier(alternateIdentifiers.get(0));
			}
			metadata.setCitation(eml.getCitationString());
			metadata.setCollection_identifier(eml.getCollectionId());
			metadata.setCollection_name(eml.getCollectionName());
			metadata.setHierarchy_level(eml.getHierarchyLevel());
			metadata.setIntellectual_rights(eml.getIntellectualRights());
			// Fetch only the first keywords/thesaurus available:
			List<KeywordSet> keyList = eml.getKeywords();
			if (!keyList.equals(null) && !keyList.isEmpty()) {
				KeywordSet keywordSet = keyList.get(0);
				metadata.setKeyword(keywordSet.getKeywordsString());
				metadata.setKeyword_thesaurus(keywordSet.getKeywordThesaurus());
			}
			metadata.setLanguage(eml.getLanguage());
			metadata.setParent_collection_identifier(eml.getParentCollectionId());
			metadata.setPublication_date(eml.getPubDate());
			metadata.setResource_logo_url(eml.getLogoUrl());
			// TODO: verify what field should relate to this:
			metadata.setResource_name("");
			metadata.setResource_uuid(guid);
			metadata.setTitle(eml.getTitle());

			// Add resource contacts information:
			Agent tempAgent = null;
			ContactModel tempContact = null;
			// Add contact agent:
			tempAgent = eml.getContact();
			if (tempAgent != null) {
				tempContact = buildContactFromAgent(tempAgent, guid, CONTACT);
				metadata.addContact(tempContact);
			}
			// Add resource metadata provider information:
			tempAgent = eml.getMetadataProvider();
			if (tempAgent != null) {
				tempContact = buildContactFromAgent(tempAgent, guid, METADATA_PROVIDER);
				metadata.addContact(tempContact);
			}
			// Add resource creator information:
			tempAgent = eml.getResourceCreator();
			if (tempAgent != null) {
				tempContact = buildContactFromAgent(tempAgent, guid, RESOURCE_CREATOR);
				metadata.addContact(tempContact);
			}
			// Add associatedParties information:
			for (Agent a : eml.getAssociatedParties()) {
				tempContact = buildContactFromAgent(a, guid, AGENT);
				metadata.addContact(tempContact);
			}
		}
		return metadata;
	}

	/**
	 * Configure each ResourceContactModel based on an EML Agent, setting it's proper resource_uuid and contact_type
	 * 
	 * @param agent
	 * @param resource_uuid
	 * @param role
	 * @return
	 */
	private ContactModel buildContactFromAgent(Agent agent, String resource_uuid, String role) {
		ContactModel contact = new ContactModel();
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

		contact.setRole(role);
		return contact;
	}

	@Override
	public void destroy() {
	}
}
