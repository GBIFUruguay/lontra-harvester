package net.canadensys.harvester;

import java.util.List;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;

/**
 * CLI servive layer interface.
 * 
 * @author cgendreau
 *
 */
public interface CLIService {

	List<DwcaResourceModel> loadResourceModelList();

	DwcaResourceModel loadResourceModel(String resourceIdentifier);

	void importDwca(DwcaResourceModel resourceModel);

	void moveToPublicSchema(DwcaResourceModel resourceModel);
}
