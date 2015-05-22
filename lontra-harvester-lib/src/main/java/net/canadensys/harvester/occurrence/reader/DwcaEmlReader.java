package net.canadensys.harvester.occurrence.reader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.xml.parsers.ParserConfigurationException;

import net.canadensys.harvester.ItemReaderIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.apache.log4j.Logger;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.ArchiveFactory;
import org.gbif.dwc.text.UnsupportedArchiveException;
import org.gbif.metadata.eml.Eml;
import org.gbif.metadata.eml.EmlFactory;
import org.xml.sax.SAXException;

/**
 * Item reader for an EML file inside a DarwinCore Archive
 *
 * @author canadensys
 *
 */
public class DwcaEmlReader implements ItemReaderIF<Eml> {

	private static final Logger LOGGER = Logger.getLogger(DwcaEmlReader.class);

	private final AtomicBoolean canceled = new AtomicBoolean(false);
	private String dwcaFilePath = null;
	private Eml eml = null;

	@Override
	public Eml read() {

		if (canceled.get()) {
			return null;
		}

		Eml tmpEml = eml;
		// the read method act like an iterator so we only return the eml once
		if (eml != null) {
			eml = null;
		}
		return tmpEml;
	}

	@Override
	public void openReader(Map<SharedParameterEnum, Object> sharedParameters) {
		dwcaFilePath = (String) sharedParameters.get(SharedParameterEnum.DWCA_PATH);

		File dwcaFile = null;
		try {
			dwcaFile = new File(dwcaFilePath);
			Archive dwcArchive = ArchiveFactory.openArchive(dwcaFile);
			eml = EmlFactory.build(new FileInputStream(dwcArchive.getMetadataLocationFile()));
		}
		catch (UnsupportedArchiveException e) {
			LOGGER.fatal("Can't open DwcaEmlReader", e);
		}
		catch (IOException e) {
			LOGGER.fatal("Can't open DwcaEmlReader", e);
		}
		catch (SAXException e) {
			LOGGER.fatal("Can't open DwcaEmlReader", e);
		}
		catch (ParserConfigurationException e) {
			LOGGER.fatal("Can't open DwcaEmlReader", e);
		}
	}

	@Override
	public void closeReader() {
	}

	@Override
	public void abort() {
		// TODO implement
	}
}
