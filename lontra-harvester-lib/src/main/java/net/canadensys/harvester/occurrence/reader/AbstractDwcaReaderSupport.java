package net.canadensys.harvester.occurrence.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gbif.dwca.io.ArchiveField;
import org.gbif.dwca.io.ArchiveFile;
import org.gbif.utils.file.ClosableIterator;

public abstract class AbstractDwcaReaderSupport {

	// TODO should be configurable
	static final Map<String, String> RESERVED_WORDS = new HashMap<String, String>();
	static {
		RESERVED_WORDS.put("class", "_class");
		RESERVED_WORDS.put("group", "_group");
		RESERVED_WORDS.put("order", "_order");
		RESERVED_WORDS.put("references", "_references");
	}
	protected static final String DEFAULT_ID_FIELD = "id";

	protected String dwcaFilePath = null;
	protected String[] headers;
	protected Map<String, String> defaultValues = null;

	protected ClosableIterator<String[]> rowsIt;

	/**
	 * Prepare the reader by setting the headers and default values related variables
	 */
	protected void prepareReader(ArchiveFile dwcaComponent) {
		try {
			// get headers
			List<ArchiveField> sortedFieldList = dwcaComponent.getFieldsSorted();
			ArrayList<String> indexedColumns = new ArrayList<String>();

			// check if the id column is used within a term or not
			int idIndex = dwcaComponent.getId().getIndex();
			boolean idColumnIncluded = false;
			for (ArchiveField currArField : sortedFieldList) {
				// check if the field is a default column or not
				if (currArField.getIndex() != null) {
					if (idIndex == currArField.getIndex().intValue()) {
						idColumnIncluded = true;
					}
					indexedColumns.add(getHeaderName(currArField));
				}
				else {
					// lazy init, do not create if not needed for this archive
					if (defaultValues == null) {
						defaultValues = new HashMap<String, String>();
					}
					defaultValues.put(getHeaderName(currArField), currArField.getDefaultValue());
				}
			}
			if (!idColumnIncluded) {
				indexedColumns.add(idIndex, DEFAULT_ID_FIELD);
			}
			headers = indexedColumns.toArray(new String[0]);

			// get rows
			rowsIt = dwcaComponent.getCSVReader().iterator();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected void closeReader() {
		rowsIt.close();
	}

	/**
	 * Handle reserved word and lowercase header from ArchiveField.
	 *
	 * @param archiveField
	 * @return
	 */
	protected String getHeaderName(ArchiveField archiveField) {
		String headerName = archiveField.getTerm().simpleName().toLowerCase();
		if (RESERVED_WORDS.get(headerName) != null) {
			headerName = RESERVED_WORDS.get(headerName);
		}
		return headerName;
	}

}
