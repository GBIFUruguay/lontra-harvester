package net.canadensys.harvester.writer;

import java.util.List;

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
import org.springframework.transaction.annotation.Transactional;

/**
 * Generic item writer for Hibernate using buffer schema.
 * 
 * @author canadensys
 * 
 * @param <T>
 */
public class GenericHibernateWriter<T> implements ItemWriterIF<T> {

	private static final Logger LOGGER = Logger.getLogger(GenericHibernateWriter.class);

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
	public void write(List<? extends T> elementList) throws WriterException {
		Transaction tx = null;
		T lastElement = null;
		try {
			tx = session.beginTransaction();
			for (T currOccurrence : elementList) {
				lastElement = currOccurrence;
				session.insert(currOccurrence);
			}
			tx.commit();
		}
		catch (HibernateException hEx) {
			LOGGER.fatal("Failed to write model", hEx);
			if (session.getTransaction() != null) {
				session.getTransaction().rollback();
			}
			if (tx != null) {
				tx.rollback();
			}
			String id = (lastElement != null) ? lastElement.toString() : "?";
			throw new WriterException(id, hEx.getMessage(), hEx);
		}
	}

	@Transactional("bufferTransactionManager")
	@Override
	public void write(T model) throws WriterException {
		try {
			Session currSession = sessionFactory.getCurrentSession();
			currSession.save(model);
		}
		catch (HibernateException hEx) {
			LOGGER.fatal("Failed to write model", hEx);
			if (session.getTransaction() != null) {
				session.getTransaction().rollback();
			}
			throw new WriterException(model.toString(), hEx.getMessage(), hEx);
		}
	}
}
