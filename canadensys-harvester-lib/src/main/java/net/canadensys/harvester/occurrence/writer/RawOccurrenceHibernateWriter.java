package net.canadensys.harvester.occurrence.writer;

import java.util.List;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.harvester.ItemWriterIF;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Item writer for OccurrenceRawModel using Hibernate.
 * 
 * @author canadensys
 * 
 */
public class RawOccurrenceHibernateWriter implements
		ItemWriterIF<OccurrenceRawModel> {

	private static final Logger LOGGER = Logger
			.getLogger(RawOccurrenceHibernateWriter.class);

	@Autowired
	@Qualifier(value = "bufferSessionFactory")
	private SessionFactory sessionFactory;

	private StatelessSession session;

	@Override
	public void closeWriter() {
		session.close();
	}

	@Override
	public void openWriter() {
		session = sessionFactory.openStatelessSession();
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	@Override
	public void write(List<? extends OccurrenceRawModel> elementList) {
		try {
			Transaction tx = session.beginTransaction();
			for (OccurrenceRawModel currRawOccurrence : elementList) {
				session.insert(currRawOccurrence);
			}
			tx.commit();
		} catch (HibernateException hEx) {
			LOGGER.fatal("Failed to write OccurrenceRawModel", hEx);
			if (session.getTransaction() != null) {
				session.getTransaction().rollback();
			}
		}
	}

	@Override
	public void write(OccurrenceRawModel rawModel) {
		try {
			Session currSession = sessionFactory.getCurrentSession();
			currSession.beginTransaction();
			currSession.save(rawModel);
			currSession.getTransaction().commit();
		} catch (HibernateException hEx) {
			LOGGER.fatal("Failed to write OccurrenceRawModel", hEx);
			if (session.getTransaction() != null) {
				session.getTransaction().rollback();
			}
		}
	}

}
