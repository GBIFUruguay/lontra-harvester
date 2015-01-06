package net.canadensys.harvester.occurrence.writer;

import java.util.List;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.exception.WriterException;

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
public class RawOccurrenceHibernateWriter implements ItemWriterIF<OccurrenceRawModel> {

	private static final Logger LOGGER = Logger.getLogger(RawOccurrenceHibernateWriter.class);

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
	public void write(List<? extends OccurrenceRawModel> elementList) throws WriterException {
		Transaction tx = null;
		String lastDwcaId = "";
		try {
			tx = session.beginTransaction();
			for (OccurrenceRawModel currRawOccurrence : elementList) {
				lastDwcaId = currRawOccurrence.getDwcaid();
				session.insert(currRawOccurrence);
			}
			tx.commit();
		}
		catch (HibernateException hEx) {
			LOGGER.fatal("Failed to write OccurrenceRawModel [" + lastDwcaId + "]", hEx);
			if (tx != null) {
				tx.rollback();
			}
			throw new WriterException(lastDwcaId, hEx.getMessage());
		}
	}

	@Override
	public void write(OccurrenceRawModel rawModel) throws WriterException {
		Transaction tx = null;
		try {
			Session currSession = sessionFactory.getCurrentSession();
			tx = currSession.beginTransaction();
			currSession.save(rawModel);
			tx.commit();
		}
		catch (HibernateException hEx) {
			LOGGER.fatal("Failed to write OccurrenceRawModel [" + rawModel.getDwcaid() + "]", hEx);
			if (tx != null) {
				tx.rollback();
			}
			throw new WriterException(rawModel.getDwcaid(), hEx.getMessage());
		}
	}

}
