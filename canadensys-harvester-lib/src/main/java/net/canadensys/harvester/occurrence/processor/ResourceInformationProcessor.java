package net.canadensys.harvester.occurrence.processor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
public class ResourceInformationProcessor implements
		ItemProcessorIF<Eml, ResourceInformationModel> {

	// get log4j handler
	private static final Logger LOGGER = Logger
			.getLogger(ResourceInformationProcessor.class);

	@Override
	public void init() {

	}

	@Override
	public ResourceInformationModel process(Eml eml,
			Map<SharedParameterEnum, Object> sharedParameters)
			throws ProcessException {
		String sourceFileId = (String) sharedParameters
				.get(SharedParameterEnum.DATASET_SHORTNAME);

		if (sourceFileId == null) {
			LOGGER.fatal("Misconfigured processor : needs  sourceFileId");
			throw new TaskExecutionException("Misconfigured processor");
		}

		ResourceInformationModel information = new ResourceInformationModel();
		/* Set information data from EML file: */
		information.set_abstract(eml.getAbstract());
		// Fetch only first identifier available:
		information.setAlternate_identifier(eml.getAlternateIdentifiers()
				.get(0));
		information.setCitation(eml.getCitationString());
		information.setCollection_identifier(eml.getCollectionId());
		information.setCollection_name(eml.getCollectionName());
		information.setHierarchy_level(eml.getHierarchyLevel());
		information.setIntellectual_rights(eml.getIntellectualRights());
		// Fetch only the first keywords/thesaurus available:
		KeywordSet keywordSet = eml.getKeywords().get(0);
		information.setKeyword(keywordSet.getKeywordsString());
		information.setKeyword_thesaurus(keywordSet.getKeywordThesaurus());
		information.setLanguage(eml.getLanguage());
		information
				.setParent_collection_identifier(eml.getParentCollectionId());
		information.setPublication_date(eml.getPubDate());
		information.setResource_logo_url(eml.getLogoUrl());
		// What field?
		information.setResource_name("");
		// getGuid maybe getUuid mispell? TODO: check in recent APIs!
		information.setResource_uuid(eml.getGuid());
		information.setTitle(eml.getTitle());

		Set<ResourceContactModel> contacts = new HashSet<ResourceContactModel>();
		ArrayList<Agent> agents = new ArrayList<Agent>();
		// Add resource contacts information: */
		agents.add(eml.getContact());
		// Add resource metadata provider information:
		agents.add(eml.getMetadataProvider());
		// Add resource creator information: 
		agents.add(eml.getResourceCreator());
		// Add associatedParties information:
		for (Agent a : eml.getAssociatedParties()) {
			agents.add(a);
		}
		// Bring all agents data into ResourceContactModel: 
		for (Agent agent : agents) {
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
			contacts.add(contact);
		}
		// Add contacts to the ResourceInformationModel:
		information.setContacts(contacts);

		return information;
	}

	@Override
	public void destroy() {

	}
}
