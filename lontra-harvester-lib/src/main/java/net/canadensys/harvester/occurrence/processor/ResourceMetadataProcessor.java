package net.canadensys.harvester.occurrence.processor;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import net.canadensys.dataportal.occurrence.dao.ResourceMetadataDAO;
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
 * Process org.gbif.metadata.eml.Eml to extract info into a ResourceMetadataModel.
 * 
 * @author cgendreau
 * @author Pedro Guimarães
 * 
 */
public class ResourceMetadataProcessor implements ItemProcessorIF<Eml, ResourceMetadataModel> {

	// get log4j handler
	private static final Logger LOGGER = Logger.getLogger(ResourceMetadataProcessor.class);

	@Override
	public void init() {
	}

	@Override
	public void destroy() {
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

		// guid represents the packageId from the EML minus the version part
		String guid = eml.getGuid();

		if (isUUID(guid)) {
			// if the resource_uuid inside the archive is different than what we asked for, do not harvest.
			if (!guid.equalsIgnoreCase(resourceUuid)) {
				throw new ProcessException("The extracted UUID from the EML doesn't match the provided UUID");
			}
		}
		else {
			// for now we support http address in resource_uuid
			// until this issue is fixed: https://github.com/WingLongitude/liger-data-access/issues/23
			if (guid.startsWith("http")) {
				if (!guid.equalsIgnoreCase(resourceUuid)) {
					throw new ProcessException("The extracted packageId from the EML doesn't match the provided UUID");
				}
			}
			else {
				throw new ProcessException("The can't extract packageId from the EML");
			}
		}

		metadata = new ResourceMetadataModel();
		metadata.setDwca_resource_id(resourceId);

		/* Set information data from EML file: */
		metadata.set_abstract(eml.getAbstract());

		// Fetch only first identifier available:
		List<String> alternateIdentifiers = eml.getAlternateIdentifiers();
		if (alternateIdentifiers != null && !alternateIdentifiers.isEmpty()) {
			metadata.setAlternate_identifier(alternateIdentifiers.get(0));
		}
		metadata.setCitation(eml.getCitationString());
		metadata.setCollection_identifier(eml.getCollectionId());
		metadata.setCollection_name(eml.getCollectionName());
		metadata.setHierarchy_level(eml.getHierarchyLevel());
		metadata.setIntellectual_rights(eml.getIntellectualRights());
		// Fetch only the first keywords/thesaurus available:
		List<KeywordSet> keyList = eml.getKeywords();
		if (keyList != null && !keyList.isEmpty()) {
			KeywordSet keywordSet = keyList.get(0);
			metadata.setKeyword(keywordSet.getKeywordsString());
			metadata.setKeyword_thesaurus(keywordSet.getKeywordThesaurus());
		}
		metadata.setLanguage(eml.getLanguage());
		metadata.setParent_collection_identifier(eml.getParentCollectionId());
		metadata.setPublication_date(eml.getPubDate());
		metadata.setResource_logo_url(eml.getLogoUrl());
		// TODO: verify what field should relate to this:
		// C.G. : I think there is none, this is probably 'title'
		// metadata.setResource_name("");
		metadata.setResource_uuid(guid);
		metadata.setTitle(eml.getTitle());

		// Add resource contacts information:
		Agent tempAgent = null;

		// Add contact agents
		tempAgent = eml.getContact();
		if (tempAgent != null) {
			metadata.addContact(buildContactFromAgent(tempAgent, guid, ResourceMetadataDAO.ContactRole.CONTACT));
		}
		// Add resource metadata provider information:
		tempAgent = eml.getMetadataProvider();
		if (tempAgent != null) {
			metadata.addContact(buildContactFromAgent(tempAgent, guid, ResourceMetadataDAO.ContactRole.METADATA_PROVIDER));
		}
		// Add resource creator information:
		tempAgent = eml.getResourceCreator();
		if (tempAgent != null) {
			metadata.addContact(buildContactFromAgent(tempAgent, guid, ResourceMetadataDAO.ContactRole.RESOURCE_CREATOR));
		}
		// Add associatedParties information:
		for (Agent a : eml.getAssociatedParties()) {
			metadata.addContact(buildContactFromAgent(a, guid, ResourceMetadataDAO.ContactRole.AGENT));
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
	private ContactModel buildContactFromAgent(Agent agent, String resource_uuid, ResourceMetadataDAO.ContactRole role) {
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

		contact.setRole(role.getKey());
		return contact;
	}

	/**
	 * Check if a provided string is a UUID
	 */
	private boolean isUUID(String str) {
		try {
			UUID uuid = UUID.fromString(str);
			if (!uuid.toString().equals(str)) {
				return false;
			}
		}
		catch (IllegalArgumentException iaEx) {
			return false;
		}
		return true;
	}
}
