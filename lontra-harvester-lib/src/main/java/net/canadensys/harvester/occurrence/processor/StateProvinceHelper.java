package net.canadensys.harvester.occurrence.processor;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.google.common.collect.Maps;

/**
 * Helper class to deal with state/province related dictionary files
 * 
 * @author cgendreau
 *
 */
public class StateProvinceHelper {

	private static final Logger LOGGER = Logger.getLogger(StateProvinceHelper.class);

	private static final String GEOGRAPHY_FOLDER = "/dictionaries/geography/";

	private static final String ISO3166_2_FILE_FILTER = GEOGRAPHY_FOLDER + "??_ISO3166-2.txt";
	private static final String STATEPROVINCE_FILE_FILTER = GEOGRAPHY_FOLDER + "??_StateProvinceName.txt";

	public static Map<String, File> getISO3166_2DictionaryFiles() {
		return getCountryPrefixedDictionaryFiles(ISO3166_2_FILE_FILTER);
	}

	public static Map<String, File> getStateProvinceNameDictionaryFiles() {
		return getCountryPrefixedDictionaryFiles(STATEPROVINCE_FILE_FILTER);
	}

	private static Map<String, File> getCountryPrefixedDictionaryFiles(String antPattern) {
		PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		Map<String, File> fileMap = Maps.newHashMap();

		try {
			Resource[] resources = resolver.getResources(antPattern);
			for (Resource resource : resources) {
				fileMap.put(StringUtils.substringBefore(resource.getFilename(), "_"), resource.getFile());
			}
		}
		catch (IOException ioEx) {
			LOGGER.error("Can't read stateProvince dictionary file(s)", ioEx);
		}
		return fileMap;
	}

}
