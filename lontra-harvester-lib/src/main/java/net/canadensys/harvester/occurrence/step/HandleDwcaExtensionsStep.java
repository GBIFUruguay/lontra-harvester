package net.canadensys.harvester.occurrence.step;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.canadensys.harvester.ItemReaderIF;
import net.canadensys.harvester.StepIF;
import net.canadensys.harvester.StepResult;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.step.stream.AbstractStreamStep;

import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;

/**
 * This step will read information about the extension available in the Dwc-A, check if it's a supported extension
 * and then dynamically create one StreamDwcExtensionContentStep per supported extension(s) to stream the content.
 *
 * @author cgendreau
 *
 */
public class HandleDwcaExtensionsStep implements StepIF {

	public static List<Term> SUPPORTED_EXTENSION = new ArrayList<Term>();
	static{
		SUPPORTED_EXTENSION.add(GbifTerm.Multimedia);
	}

	@Autowired
	private ApplicationContext appContext;

	@Autowired
	@Qualifier("dwcaExtensionInfoReader")
	private ItemReaderIF<Term> dwcaInfoReader;

	private Map<SharedParameterEnum,Object> sharedParameters;

	@Override
	public String getTitle() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void preStep(Map<SharedParameterEnum, Object> sharedParameters)
			throws IllegalStateException {
		this.sharedParameters = sharedParameters;

		dwcaInfoReader.openReader(sharedParameters);
	}

	@Override
	public void postStep() {
		dwcaInfoReader.closeReader();
	}

	/**
	 * @return the total number of records contained in extension(s).
	 */
	@Override
	public StepResult doStep() {
		int numberOfRecords = 0;
		Term currExtension = dwcaInfoReader.read();

		while(currExtension != null){
			if(SUPPORTED_EXTENSION.contains(currExtension)){
				AbstractStreamStep streamDwcExtensionContentStep = (AbstractStreamStep)appContext.getBean("streamDwcExtensionContentStep");

				//tricky part, shallow copy(not a deep copy) sharedParameters to indicate each readers which extension to use
				//this is probably not the best way to achieve that
				Map<SharedParameterEnum, Object> innerSharedParameters = new HashMap<SharedParameterEnum,Object>(sharedParameters);
				//use the simpleName, at least for now
				innerSharedParameters.put(SharedParameterEnum.DWCA_EXTENSION_TYPE, currExtension);
				streamDwcExtensionContentStep.preStep(innerSharedParameters);
				StepResult result = streamDwcExtensionContentStep.doStep();
				numberOfRecords += result.getNumberOfRecord();
				streamDwcExtensionContentStep.postStep();

				System.out.println(currExtension + " extension contains " + result.getNumberOfRecord() + " records");
			}
			currExtension = dwcaInfoReader.read();
		}
		return new StepResult(numberOfRecords);
	}

	@Override
	public void cancel() {
		// TODO Auto-generated method stub

	}

}
