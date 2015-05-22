package net.canadensys.harvester.occurrence.processor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.google.common.collect.Maps;

/**
 * Helper class to deal with state/province related dictionary files loaded as InputStream.
 *
 * @author cgendreau
 *
 */
public class StateProvinceHelper {

	private static final Logger LOGGER = Logger.getLogger(StateProvinceHelper.class);

	private static final String GEOGRAPHY_FOLDER = "/dictionaries/geography/";

	private static final String ISO3166_2_FILE_FILTER = GEOGRAPHY_FOLDER + "??_ISO3166-2.txt";
	private static final String STATEPROVINCE_FILE_FILTER = GEOGRAPHY_FOLDER + "??_StateProvinceName.txt";

	public static Map<String, InputStream> getISO3166_2DictionaryInputStreams() {
		return getCountryPrefixedDictionaryInputStreams(ISO3166_2_FILE_FILTER);
	}

	public static Map<String, InputStream> getStateProvinceNameDictionaryInputStreams() {
		return getCountryPrefixedDictionaryInputStreams(STATEPROVINCE_FILE_FILTER);
	}

	private static Map<String, InputStream> getCountryPrefixedDictionaryInputStreams(String antPattern) {
		PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		Map<String, InputStream> InputStreamMap = Maps.newHashMap();

		try {
			Resource[] resources = resolver.getResources(antPattern);
			for (Resource resource : resources) {
				InputStreamMap.put(StringUtils.substringBefore(resource.getFilename(), "_"), resource.getInputStream());
			}
		}
		catch (IOException ioEx) {
			LOGGER.error("Can't read stateProvince dictionary file(s)", ioEx);
		}
		return InputStreamMap;
	}

}
