package net.canadensys.harvester.mapper;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.utils.file.FileUtils;

import com.google.common.collect.Maps;

/**
 * Allows to map terms to a string based on definitions in file (termsMapping.txt).
 * Terms mapping is used to map a term to a property when, for example, 2 terms should 
 * be considered as the same by the harvester.
 *
 */
public class TermMapper {
	private static final Logger LOGGER = Logger.getLogger(TermMapper.class);
	private static final String TERMS_MAPPING_FILE = "/termsMapping.txt";

	private static TermMapper singletonObject;

	private Map<Term, String> termsMap;

	/**
	 * Singleton, use {@link #getInstance()}.
	 */
	private TermMapper() {
		termsMap = Maps.newHashMap();
		try {
			Map<String, String> termFromFile = Maps.newHashMap();
			FileUtils.streamToMap(TermMapper.class.getResourceAsStream(TERMS_MAPPING_FILE), termFromFile, 0, 1, false);
			TermFactory TF = TermFactory.instance();
			for (String currTerm : termFromFile.keySet()) {
				termsMap.put(TF.findTerm(currTerm), termFromFile.get(currTerm));
			}
		}
		catch (IOException ioEx) {
			LOGGER.error("Can't load terms mapping file", ioEx);
		}
	}

	/**
	 * Get the mapping defined for the provided term.
	 * 
	 * @param term
	 * @return the property name mapped to the provided term or null if no mapping is defined for that term.
	 */
	public String getTermMapping(Term term) {
		return termsMap.get(term);
	}

	public static TermMapper getInstance() {
		synchronized (TermMapper.class) {
			if (singletonObject == null) {
				singletonObject = new TermMapper();
			}
		}
		return singletonObject;
	}
}
