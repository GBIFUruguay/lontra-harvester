package net.canadensys.processing.occurrence.task;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import net.canadensys.processing.ItemProgressListenerIF;
import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.exception.TaskExecutionException;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.apache.log4j.Logger;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.google.common.util.concurrent.FutureCallback;

/**
 * Task to check and wait for processing completion.
 * Notification will be sent using the FutureCallback object.
 * @author canadensys
 *
 */
public class CheckProcessingCompletenessTask implements ItemTaskIF{

	private static final int MAX_WAITING_SECONDS = 10;
	private static final Logger LOGGER = Logger.getLogger(CheckProcessingCompletenessTask.class);
	
	@Autowired
	@Qualifier(value="bufferSessionFactory")
	private SessionFactory sessionFactory;
	
	private List<ItemProgressListenerIF> itemListenerList;
	private int secondsWaiting = 0;
	
	/**
	 * @param sharedParameters get BatchConstant.NUMBER_OF_RECORDS and BatchConstant.DWCA_IDENTIFIER_TAG
	 */
	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters) {
		final Integer numberOfRecords = (Integer)sharedParameters.get(SharedParameterEnum.NUMBER_OF_RECORDS);
		final String datasetShortname = (String)sharedParameters.get(SharedParameterEnum.DATASET_SHORTNAME);
		final FutureCallback<Void> jobCallback = (FutureCallback<Void>)sharedParameters.get(SharedParameterEnum.CALLBACK);
		if(numberOfRecords == null || datasetShortname == null || jobCallback == null){
			LOGGER.fatal("Misconfigured task : needs numberOfRecords, datasetShortname and callback");
			throw new TaskExecutionException("Misconfigured task");
		}
		
		Thread checkThread = new Thread(new Runnable() {
			private int previousCount = 0;
			@Override
			public void run() {
				Session session = sessionFactory.openSession();
				SQLQuery query = session.createSQLQuery("SELECT count(*) FROM buffer.occurrence_raw WHERE sourcefileid=?");
				query.setString(0, datasetShortname);
				
				BigInteger currNumberOfResult = (BigInteger)query.uniqueResult();
				while(currNumberOfResult.intValue() < numberOfRecords){
					currNumberOfResult = (BigInteger)query.uniqueResult();
					//make sure we don't get stuck here is something goes wrong with the clients
					if(previousCount == currNumberOfResult.intValue()){
						secondsWaiting++;
						if(secondsWaiting == MAX_WAITING_SECONDS){
							break;
						}
					}
					else{
						secondsWaiting = 0;
					}
					previousCount = currNumberOfResult.intValue();
					notifyListeners(currNumberOfResult.intValue(),numberOfRecords);
					
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
						break;
					}
				}
				session.close();
				
				if(secondsWaiting < MAX_WAITING_SECONDS){
					jobCallback.onSuccess(null);
				}
				else{
					jobCallback.onFailure(new TimeoutException("No progress made in more than " + MAX_WAITING_SECONDS + " seconds."));
				}
			}
		});
		checkThread.start();
	}
	
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
	
	private void notifyListeners(int current,int total){
		if(itemListenerList != null){
			for(ItemProgressListenerIF currListener : itemListenerList){
				currListener.onProgress(current, total);
			}
		}
	}
	
	public void addItemProgressListenerIF(ItemProgressListenerIF listener){
		if(itemListenerList == null){
			itemListenerList = new ArrayList<ItemProgressListenerIF>();
		}
		itemListenerList.add(listener);
	}
}
