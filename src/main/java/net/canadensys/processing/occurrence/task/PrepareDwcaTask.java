package net.canadensys.processing.occurrence.task;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.exception.TaskExecutionException;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.utils.ZipUtils;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.log4j.Logger;

/**
 * Task to prepare a Darwin Core Archive.
 * Preparation include : download (if necessary), unzip (if necessary), set shared variables
 * @author canadensys
 *
 */
public class PrepareDwcaTask implements ItemTaskIF{
	//get log4j handler
	private static final Logger LOGGER = Logger.getLogger(PrepareDwcaTask.class);
	private static final String IPT_PREFIX = "dwca-";
	private static final String WORKING_FOLDER = "work";
	
	private boolean allowDatasetShortnameExtraction = false;
	
	/**
	 * Allow to find the Darwin Core archive location from the sharedParameters.
	 * @param sharedParameters
	 * @return the dwca location or null if the location could not be found or is conflicting
	 */
	private String extractDwcaFileLocation(Map<SharedParameterEnum,Object> sharedParameters){
		String dwcaURL = (String)sharedParameters.get(SharedParameterEnum.DWCA_URL);
		String dwcaPath = (String)sharedParameters.get(SharedParameterEnum.DWCA_PATH);
		
		if(StringUtils.isNotBlank(dwcaURL) && StringUtils.isNotBlank(dwcaPath)){
			LOGGER.fatal("Conflicted DwcaFileLocation : " + dwcaURL + " and : " + dwcaPath);
			return null;
		}
		
		if(StringUtils.isNotBlank(dwcaURL)){
			return dwcaURL;
		}
		
		if(StringUtils.isNotBlank(dwcaPath)){
			return dwcaPath;
		}
		return null;
	}
	
	/**
	 * @param sharedParameters out:SharedParameterEnum.DWCA_PATH,SharedParameterEnum.DATASET_SHORTNAME(if not already set)
	 */
	@Override
	public void execute(Map<SharedParameterEnum,Object> sharedParameters){
		boolean dwcaFileExists = false;
		File dwcaFile = null;
		String dwcaIdentifier;
		String dwcaFileLocation = extractDwcaFileLocation(sharedParameters);
		
		//make sure the files exists
		if(dwcaFileLocation != null){
			//Is this a URL?
			UrlValidator urlValidator = new UrlValidator();
			if(urlValidator.isValid(dwcaFileLocation)){
				File workFolder = new File(WORKING_FOLDER);
				//make sure the folder exists
				workFolder.mkdir();
				URL dlUrl;
				try {
					dlUrl = new URL(dwcaFileLocation);
					//Get the filename as defined by Content-Disposition:filename="dwca-mt-specimens.zip"
		        	String filename = dlUrl.openConnection().getHeaderField("Content-Disposition");
		        	if(StringUtils.isNotBlank(filename)){
			            String destinationFile = workFolder.getAbsolutePath() +File.separator+ filename.replaceAll("\"", "").replace("filename=", "");
			            
			            downloadDwca(dlUrl, destinationFile);
			            dwcaFileLocation  = destinationFile;
		        	}
				} catch (MalformedURLException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			dwcaFile = new File(dwcaFileLocation);
			dwcaFileExists = dwcaFile.exists();
		}
		if(!dwcaFileExists){
			throw new TaskExecutionException("Could not find the DarwinCore archive file "  + dwcaFileLocation);
		}
		
		//set the unique identifier for this resource
		if(dwcaFile.isDirectory()){
			dwcaIdentifier = dwcaFile.getName();
		}
		else{
			dwcaIdentifier = FilenameUtils.getBaseName(dwcaFileLocation);
		}
		
		
		//remove common IPT prefix
		if(StringUtils.startsWith(dwcaIdentifier, IPT_PREFIX)){
			dwcaIdentifier = StringUtils.removeStart(dwcaIdentifier, IPT_PREFIX);
		}
		
		if(FilenameUtils.isExtension(dwcaFileLocation, "zip")){
			String unzippedFolder = FilenameUtils.removeExtension(dwcaFileLocation);
			if(!ZipUtils.unzipFileOrFolder(new File(dwcaFileLocation), unzippedFolder)){
				throw new TaskExecutionException("Error while unziping the DarwinCore Archive");
			}
			//use the unzipped folder
			dwcaFileLocation = unzippedFolder;
		}
		
		//sanity check
		if(StringUtils.isBlank(dwcaIdentifier)){
			LOGGER.fatal("dwcaIdentifier cannot be empty");
			throw new TaskExecutionException("dwcaIdentifier cannot be empty");
		}
		
		//if SharedParameterEnum.DWCA_PATH was previously there, we replace it
		sharedParameters.put(SharedParameterEnum.DWCA_PATH, dwcaFileLocation);
		if(allowDatasetShortnameExtraction && sharedParameters.get(SharedParameterEnum.DATASET_SHORTNAME) == null){
			sharedParameters.put(SharedParameterEnum.DATASET_SHORTNAME, dwcaIdentifier);
		}
	}
	
	/**
	 * Download a DarwinCore archive from a URL and save it locally.
	 * @param url
	 * @param destinationFile
	 * @return
	 */
	private boolean downloadDwca(URL url, String destinationFile){
	 	File fl = null;
        OutputStream os = null;
        InputStream is = null;
        boolean success = false;
        try {
            fl = new File(destinationFile);
            os = new FileOutputStream(fl);
            is = url.openStream();

            //Download the file
            IOUtils.copy(is, os);
            
            success = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (os != null) { 
                try {
					os.close();
				} catch (IOException e) {} 
            }
            if (is != null) { 
                try {
					is.close();
				} catch (IOException e) {} 
            }
        }
        return success;
	}

	public boolean isAllowDatasetShortnameExtraction() {
		return allowDatasetShortnameExtraction;
	}
	/**
	 * Should we allow this task to set the SharedParameterEnum.DATASET_SHORTNAME using the name of the file
	 * in case this parameter is not set?
	 * @param allowDatasetShortnameExtraction
	 */
	public void setAllowDatasetShortnameExtraction(
			boolean allowDatasetShortnameExtraction) {
		this.allowDatasetShortnameExtraction = allowDatasetShortnameExtraction;
	}

}
