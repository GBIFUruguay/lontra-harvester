package net.canadensys.harvester;

import java.io.IOException;
import java.util.List;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.harvester.model.CliOption;

/**
 * CLI service layer interface.
 * 
 * @author cgendreau
 *
 */
public interface CLIService {

	List<DwcaResourceModel> getResourceList();

	DwcaResourceModel loadResourceModel(String resourceIdentifier);

	/**
	 * 
	 * @param resourceModel
	 * @param cliOption
	 */
	void importDwca(DwcaResourceModel resourceModel, CliOption cliOption) throws IOException;

	void moveToPublicSchema(DwcaResourceModel resourceModel);

	void computeUniqueValueJob();
}
