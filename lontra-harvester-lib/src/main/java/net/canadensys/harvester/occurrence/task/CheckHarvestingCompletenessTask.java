package net.canadensys.harvester.occurrence.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import net.canadensys.dataportal.occurrence.model.OccurrenceFieldConstants;
import net.canadensys.harvester.ItemProgressListenerIF;
import net.canadensys.harvester.LongRunningTaskIF;
import net.canadensys.harvester.exception.TaskExecutionException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Task to check and wait for processing completion.
 * Notification will be sent using the ItemProgressListenerIF listener.
 * @author canadensys
 *
 */
public class CheckHarvestingCompletenessTask implements LongRunningTaskIF{

	private static final int MAX_WAITING_SECONDS = 10;
	private static final Logger LOGGER = Logger.getLogger(CheckHarvestingCompletenessTask.class);

	@Autowired
	@Qualifier(value="bufferSessionFactory")
	private SessionFactory sessionFactory;

	private List<ItemProgressListenerIF> itemListenerList;
	private int secondsWaiting = 0;
	private final AtomicBoolean taskCanceled = new AtomicBoolean(false);

	private String targetedTable;
	private Integer expectedNumberOfRecords;

	/**
	 * @param sharedParametersSharedParameterEnum
	 *            .RESOURCE_ID required
	 */
	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters) {
		final Integer _expectedNumberOfRecords = expectedNumberOfRecords;

		if (_expectedNumberOfRecords == null || sharedParameters.get(SharedParameterEnum.RESOURCE_ID) == null) {
			LOGGER.fatal("Misconfigured task : resourceId and numberOfRecords are required");
			throw new TaskExecutionException("Misconfigured task");
		}
		if (StringUtils.isBlank(targetedTable)) {
			LOGGER.fatal("Misconfigured task : targetedTable is required");
			throw new TaskExecutionException("Misconfigured task");
		}

		final String identifierColumn = OccurrenceFieldConstants.RESOURCE_ID;
		final Integer resourceId = (Integer) sharedParameters.get(SharedParameterEnum.RESOURCE_ID);

		Thread checkThread = new Thread(new Runnable() {
			private int previousCount = 0;
			@Override
			public void run() {
				Session session = sessionFactory.openSession();
				SQLQuery query = session.createSQLQuery("SELECT count(*) FROM buffer." + targetedTable + " WHERE " + identifierColumn + "=?");
				query.setInteger(0, resourceId);
				try{
					Number currNumberOfResult = (Number)query.uniqueResult();
					while(!taskCanceled.get() && (currNumberOfResult.intValue() < _expectedNumberOfRecords.intValue())){
						currNumberOfResult = (Number)query.uniqueResult();
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
						notifyListeners(targetedTable,currNumberOfResult.intValue(),_expectedNumberOfRecords);

						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
							break;
						}
					}
				}catch(HibernateException hEx){
					notifyListenersOnFailure(hEx);
				}
				session.close();

				if(taskCanceled.get()){
					notifyListenersOnCancel();
				}
				else{
					if(secondsWaiting < MAX_WAITING_SECONDS){
						notifyListenersOnSuccess();
					}
					else{
						notifyListenersOnFailure(new TimeoutException("No progress made in more than " + MAX_WAITING_SECONDS + " seconds."));
					}
				}
			}
		});
		checkThread.start();
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	private void notifyListeners(String context,int current,int total){
		if(itemListenerList != null){
			for(ItemProgressListenerIF currListener : itemListenerList){
				currListener.onProgress(context,current, total);
			}
		}
	}
	private void notifyListenersOnSuccess(){
		if(itemListenerList != null){
			for(ItemProgressListenerIF currListener : itemListenerList){
				currListener.onSuccess(targetedTable);
			}
		}
	}
	private void notifyListenersOnCancel(){
		if(itemListenerList != null){
			for(ItemProgressListenerIF currListener : itemListenerList){
				currListener.onCancel(targetedTable);
			}
		}
	}
	private void notifyListenersOnFailure(Throwable t){
		if(itemListenerList != null){
			for(ItemProgressListenerIF currListener : itemListenerList){
				currListener.onError(targetedTable,t);
			}
		}
	}

	public void addItemProgressListenerIF(ItemProgressListenerIF listener){
		if(itemListenerList == null){
			itemListenerList = new ArrayList<ItemProgressListenerIF>();
		}
		itemListenerList.add(listener);
	}

	/**
	 * Set SQL query details to check completeness.
	 *
	 * @param targetedTable
	 *            in which we want to count the rows
	 * @param expectedNumberOfRecords
	 */
	public void configure(String targetedTable, Integer expectedNumberOfRecords) {
		this.targetedTable = targetedTable;
		this.expectedNumberOfRecords = expectedNumberOfRecords;
	}

	@Override
	public void cancel() {
		taskCanceled.set(true);
	}

	@Override
	public String getTitle() {
		return "Waiting for completion";
	}
}
