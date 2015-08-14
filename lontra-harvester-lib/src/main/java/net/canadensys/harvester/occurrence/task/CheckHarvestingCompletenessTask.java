package net.canadensys.harvester.occurrence.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import net.canadensys.dataportal.occurrence.model.OccurrenceFieldConstants;
import net.canadensys.harvester.ItemProgressListenerIF;
import net.canadensys.harvester.LongRunningTaskIF;
import net.canadensys.harvester.exception.TaskExecutionException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.google.common.collect.Lists;

/**
 * Task to check and wait for processing completion.
 * Notification will be sent using the ItemProgressListenerIF listener.
 * This class is NOT Thread Safe and the ThreadPool will always use only one Thread and check different
 * contexts sequentially.
 *
 * TODO Improve this class with a better usage of the ThreadPool and maybe use
 * Guava com.google.common.util.concurrent package.
 *
 * We can add a listener to be called when ALL the ListenableFuture will have finished which should be
 * used to trigger the notifyCompletion() instead of current numberOfCompletedCommand.
 *
 * ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
 * //add to listenableFutures list (see service.submit())
 * ListenableFuture<List<Boolean>> allListenableFuture = Futures.successfulAsList(listenableFutures);
 * Futures.addCallback(allListenableFuture, new FutureCallback<List<Boolean>>() {
 * ...
 * });
 *
 * @author canadensys
 *
 */
public class CheckHarvestingCompletenessTask implements LongRunningTaskIF {

	private static final int MAX_WAITING_SECONDS = 10;
	private static final Logger LOGGER = Logger.getLogger(CheckHarvestingCompletenessTask.class);

	@Autowired
	@Qualifier(value = "bufferSessionFactory")
	private SessionFactory sessionFactory;

	private List<ItemProgressListenerIF> itemListenerList;

	private final AtomicBoolean taskCanceled = new AtomicBoolean(false);

	private ExecutorService threadPool;
	private final List<CompletenessTarget> completenessTargets = Lists.newArrayList();
	private final AtomicInteger numberOfCompletedCommand = new AtomicInteger();

	/**
	 * @param sharedParametersSharedParameterEnum
	 *            .RESOURCE_ID required
	 */
	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters) {
		if (sharedParameters.get(SharedParameterEnum.RESOURCE_ID) == null) {
			LOGGER.fatal("Misconfigured task : resourceId is required");
			throw new TaskExecutionException("Misconfigured task");
		}
		if (completenessTargets.isEmpty()) {
			LOGGER.fatal("Misconfigured task : at least one target is required");
			throw new TaskExecutionException("Misconfigured task");
		}

		// we use only one Thread since we only want to run the command asynchronously and then decide if we should
		// run the next command or not. In theory, we could run all the Thread at the 'same' time but it would require
		// a better design of the ItemProgressListenerIF implementation that are not thread safe for now.
		threadPool = Executors.newSingleThreadExecutor();
		final Integer resourceId = (Integer) sharedParameters.get(SharedParameterEnum.RESOURCE_ID);
		for (CompletenessTarget ct : completenessTargets) {
			threadPool.submit(createRunnableCommand(resourceId, ct.getTargetedTable(), ct.getExpectedNumberOfRecords()));
		}
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	/**
	 * Handle success of a command and determine if all the commands were executed.
	 *
	 * @param context
	 */
	private void handleSuccess(String context) {
		int _numberOfCompletedCommand = numberOfCompletedCommand.incrementAndGet();
		// notify success for the context first
		notifySuccess(context);

		if (_numberOfCompletedCommand == completenessTargets.size()) {
			threadPool.shutdown();
			notifyCompletion();
		}
	}

	/**
	 * Responsible to shutdown the ThreadPool to avoid other thread (if any) to start.
	 *
	 * @param context
	 * @param t
	 */
	private void handleFailure(String context, Throwable t) {
		threadPool.shutdownNow();
		notifyFailure(context, t);
	}

	protected void notifyProgress(String context, int current, int total) {
		if (itemListenerList != null) {
			for (ItemProgressListenerIF currListener : itemListenerList) {
				currListener.onProgress(context, current, total);
			}
		}
	}

	protected void notifySuccess(String context) {
		if (itemListenerList != null) {
			for (ItemProgressListenerIF currListener : itemListenerList) {
				currListener.onSuccess(context);
			}
		}
	}

	protected void notifyCancel(String context) {
		if (itemListenerList != null) {
			for (ItemProgressListenerIF currListener : itemListenerList) {
				currListener.onCancel(context);
			}
		}
	}

	protected void notifyFailure(String context, Throwable t) {
		if (itemListenerList != null) {
			for (ItemProgressListenerIF currListener : itemListenerList) {
				currListener.onError(context, t);
			}
		}
	}

	protected void notifyCompletion() {
		if (itemListenerList != null) {
			for (ItemProgressListenerIF currListener : itemListenerList) {
				currListener.onCompletion();
			}
		}
	}

	public void addItemProgressListenerIF(ItemProgressListenerIF listener) {
		if (itemListenerList == null) {
			itemListenerList = new ArrayList<ItemProgressListenerIF>();
		}
		itemListenerList.add(listener);
	}

	public void purgeListeners() {
		if (itemListenerList != null) {
			itemListenerList.clear();
		}
	}

	/**
	 * You can add more than one target for a job but they will be check one after the other in
	 * the same order they were added by this method.
	 *
	 * @param targetedTable
	 * @param expectedNumberOfRecords
	 */
	public void addTarget(String targetedTable, Integer expectedNumberOfRecords) {
		completenessTargets.add(new CompletenessTarget(targetedTable, expectedNumberOfRecords));
	}

	@Override
	public void cancel() {
		threadPool.shutdownNow();
		taskCanceled.set(true);
	}

	@Override
	public String getTitle() {
		return "Waiting for completion";
	}

	/**
	 * Create an instance of Runnable for a specific targetedTable (aka context).
	 *
	 * @param resourceId
	 * @param targetedTable
	 * @param expectedNumberOfRecords
	 * @return
	 */
	private Runnable createRunnableCommand(final int resourceId, final String targetedTable, final int expectedNumberOfRecords) {
		final String identifierColumn = OccurrenceFieldConstants.RESOURCE_ID;

		return new Runnable() {
			private int previousCount = 0;
			private int secondsWaiting = 0;

			@Override
			public void run() {
				Session session = sessionFactory.openSession();
				SQLQuery query = session.createSQLQuery("SELECT count(*) FROM buffer." + targetedTable + " WHERE " + identifierColumn + "=?");
				query.setInteger(0, resourceId);
				try {
					Number currNumberOfResult = (Number) query.uniqueResult();
					while (!taskCanceled.get() && (currNumberOfResult.intValue() < expectedNumberOfRecords)) {
						currNumberOfResult = (Number) query.uniqueResult();
						// make sure we don't get stuck here is something goes wrong with the clients
						if (previousCount == currNumberOfResult.intValue()) {
							secondsWaiting++;
							if (secondsWaiting == MAX_WAITING_SECONDS) {
								break;
							}
						}
						else {
							secondsWaiting = 0;
						}
						previousCount = currNumberOfResult.intValue();
						notifyProgress(targetedTable, currNumberOfResult.intValue(), expectedNumberOfRecords);

						try {
							Thread.sleep(1000);
						}
						catch (InterruptedException e) {
							e.printStackTrace();
							break;
						}
					}
				}
				catch (HibernateException hEx) {
					handleFailure(targetedTable, hEx);
				}
				finally {
					session.close();
				}

				if (taskCanceled.get()) {
					notifyCancel(targetedTable);
				}
				else {
					if (secondsWaiting < MAX_WAITING_SECONDS) {
						// make sure to notify the progress completed
						notifyProgress(targetedTable, expectedNumberOfRecords, expectedNumberOfRecords);
						handleSuccess(targetedTable);
					}
					else {
						handleFailure(targetedTable, new TimeoutException("No progress made in more than " + MAX_WAITING_SECONDS
								+ " seconds."));
					}
				}
			}
		};
	}

	/**
	 * Simple pair object to hold a targetTable and the expected number of records.
	 *
	 * @author cgendreau
	 *
	 */
	private static class CompletenessTarget {
		private final String targetedTable;
		private final Integer expectedNumberOfRecords;

		CompletenessTarget(String targetedTable, Integer expectedNumberOfRecords) {
			this.targetedTable = targetedTable;
			this.expectedNumberOfRecords = expectedNumberOfRecords;
		}

		public String getTargetedTable() {
			return targetedTable;
		}

		public Integer getExpectedNumberOfRecords() {
			return expectedNumberOfRecords;
		}
	}
}
