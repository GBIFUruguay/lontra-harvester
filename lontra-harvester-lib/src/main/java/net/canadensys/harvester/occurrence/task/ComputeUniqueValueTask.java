package net.canadensys.harvester.occurrence.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.exception.TaskExecutionException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.utils.StringUtils;

import org.apache.log4j.Logger;
import org.hibernate.CacheMode;
import org.hibernate.HibernateException;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.type.StandardBasicTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.transaction.annotation.Transactional;

/**
 * This task should only be used if you do not have any indexing software (e.g.
 * ElasticSearch ...) Task to pre-compute all possible unique values and their
 * counts for some fields. To ensure maximum performance we run this once, after
 * moving the data to public schema.
 *
 * @author canadensys
 *
 */
public class ComputeUniqueValueTask implements ItemTaskIF {

	// get log4j handler
	private static final Logger LOGGER = Logger.getLogger(ComputeUniqueValueTask.class);

	// for performance reason we use varchar(255) (and not text)
	private static final int MAX_VALUE_LENGTH = 255;

	private static final int FETCH_SIZE = 1000;
	private static String ABSTRACT_INSERT = "INSERT INTO unique_values (key,occurrence_count,value,unaccented_value) VALUES (:key,:occ_count,:value,:unaccented_value)";
	private static String ABSTRACT_SELECT = "SELECT COUNT(%field) occurrence_count,%field FROM occurrence WHERE %field IS NOT NULL AND %field <> '' GROUP BY %field";

	// name of columns from the occurrence table to index in the unique_values
	// must fit with SearchServiceConfig (Explorer)
	private static List<String> columns = new ArrayList<String>();
	static {
		columns.add("country");
		columns.add("family");
		columns.add("continent");
		columns.add("taxonrank");
		columns.add("collectioncode");
		columns.add("datasetname");
		columns.add("stateprovince");
		columns.add("kingdom");
		columns.add("scientificname");
		columns.add("_order");
		columns.add("recordedby");
		columns.add("institutioncode");
		columns.add("_class");
		columns.add("phylum");
		columns.add("county");
		columns.add("municipality");
		columns.add("sourcefileid");
		columns.add("publishername");
		columns.add("resourcename");
	}

	@Autowired
	@Qualifier(value = "publicSessionFactory")
	private SessionFactory sessionFactory;

	@Transactional("publicTransactionManager")
	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters) {

		Session session = sessionFactory.getCurrentSession();
		session.createSQLQuery("DELETE FROM unique_values").executeUpdate();
		session.createSQLQuery("ALTER SEQUENCE unique_values_id_seq RESTART WITH 1").executeUpdate();
		Object[] currentValue;
		try {
			for (String currCol : columns) {
				ScrollableResults cursor = session.createSQLQuery(ABSTRACT_SELECT.replaceAll("%field", currCol))
						.addScalar("occurrence_count", StandardBasicTypes.INTEGER).addScalar(currCol, StandardBasicTypes.STRING).setCacheable(false)
						.setCacheMode(CacheMode.IGNORE).setFetchSize(FETCH_SIZE).scroll();
				while (cursor.next()) {
					currentValue = cursor.get();
					if (((String) currentValue[1]).length() < MAX_VALUE_LENGTH) {
						session.createSQLQuery(ABSTRACT_INSERT).setParameter("key", currCol).setParameter("occ_count", currentValue[0])
						.setParameter("value", currentValue[1])
						.setParameter("unaccented_value", StringUtils.unaccent(((String) currentValue[1]).toLowerCase())).executeUpdate();
					}
				}
				cursor.close();
			}
		}
		catch (HibernateException hEx) {
			LOGGER.fatal("Can't compute unique values", hEx);
			throw new TaskExecutionException("Can't compute unique values");
		}
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	@Override
	public String getTitle() {
		return "Computing unique values";
	}
}
