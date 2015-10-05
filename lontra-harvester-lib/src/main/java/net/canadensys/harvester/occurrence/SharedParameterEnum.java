package net.canadensys.harvester.occurrence;

/**
 * Enumeration of SharedParameter types
 *
 * @author canadensys
 *
 */
public enum SharedParameterEnum {

	DWCA_URL, DWCA_PATH,

	/**
	 * Key used to provide a List<String> of Dwca ID to exclude from the harvesting
	 */
	DWCA_ID_EXCLUSION_LIST,

	/**
	 * Key used to provide the list of DarwinCore terms used in the archive.
	 */
	DWCA_USED_TERMS,
	/**
	 * Key used to provide an instance of Term that represents the type of extension
	 */
	DWCA_EXTENSION_TYPE, RESOURCE_ID,
	/**
	 * Key used to provide an instance of DwcaResourceModel.
	 */
	RESOURCE_MODEL,
	NUMBER_OF_RECORDS, PUBLISHER_NAME,
	/**
	 * key used to provide an instance of PublisherModel
	 */
	PUBLISHER_MODEL
}
