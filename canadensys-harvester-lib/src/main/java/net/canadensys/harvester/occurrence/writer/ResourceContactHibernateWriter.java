package net.canadensys.harvester.occurrence.writer;

import java.util.List;

import net.canadensys.dataportal.occurrence.model.ResourceContactModel;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.exception.WriterException;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Item writer for ResourceContactModel using Hibernate.
 * 
 * @author canadensys
 * 
 */
public class ResourceContactHibernateWriter implements
		ItemWriterIF<ResourceContactModel> {

	private static final Logger LOGGER = Logger
			.getLogger(ResourceContactHibernateWriter.class);

	@Autowired
	@Qualifier(value = "bufferSessionFactory")
	private SessionFactory sessionFactory;

	private Session session;

	@Override
	public void closeWriter() {
		session.close();
	}

	@Override
	public void openWriter() {
		session = sessionFactory.openSession();
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	@Override
	public void write(List<? extends ResourceContactModel> elementList) throws WriterException{
		try {
			session.beginTransaction();
			for (ResourceContactModel resourceContactModel : elementList) {
				session.save(resourceContactModel);
			}
			session.getTransaction().commit();
		} catch (HibernateException hEx) {
			LOGGER.fatal("Failed to write resourceContact", hEx);
			if (session.getTransaction() != null) {
				session.getTransaction().rollback();
			}
			throw new WriterException(hEx.getMessage());
		}
	}

	@Override
	public void write(ResourceContactModel resourceContactModel) throws WriterException{
		try {
			session.beginTransaction();
			session.save(resourceContactModel);
			session.getTransaction().commit();
		} catch (HibernateException hEx) {
			LOGGER.fatal("Failed to write resourceContact", hEx);
			if (session.getTransaction() != null) {
				session.getTransaction().rollback();
			}
			throw new WriterException(hEx.getMessage());
		}
	}
}
