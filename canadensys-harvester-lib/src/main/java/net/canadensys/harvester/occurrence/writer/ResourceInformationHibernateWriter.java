package net.canadensys.harvester.occurrence.writer;

import java.util.List;

import net.canadensys.dataportal.occurrence.model.ResourceMetadataModel;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.exception.WriterException;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Item writer for ResourceInformationModel using Hibernate.
 * 
 * @author canadensys
 * 
 */
public class ResourceInformationHibernateWriter implements ItemWriterIF<ResourceMetadataModel> {

	private static final Logger LOGGER = Logger.getLogger(ResourceInformationHibernateWriter.class);

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
	public void write(List<? extends ResourceMetadataModel> elementList) throws WriterException {
		String lastId = "";
		try {
			session.beginTransaction();
			for (ResourceMetadataModel resourceMetadataModel : elementList) {
				lastId = resourceMetadataModel.getDwca_resource_id() != null ? resourceMetadataModel.getDwca_resource_id().toString() : "?";
			}
			session.getTransaction().commit();
		}
		catch (HibernateException hEx) {
			LOGGER.fatal("Failed to write resourceInformation", hEx);
			if (session.getTransaction() != null) {
				session.getTransaction().rollback();
			}
			throw new WriterException(lastId, hEx.getMessage());
		}
	}

	@Override
	public void write(ResourceMetadataModel resourceMetadataModel) throws WriterException {
		openWriter();
		try {
			session.beginTransaction();
			session.save(resourceMetadataModel);
			session.getTransaction().commit();
		}
		catch (HibernateException hEx) {
			LOGGER.fatal("Failed to write resourceInformation", hEx);
			if (session.getTransaction() != null) {
				session.getTransaction().rollback();
			}
			String id = resourceMetadataModel.getDwca_resource_id() != null ? resourceMetadataModel.getDwca_resource_id().toString() : "?";
			throw new WriterException(id, hEx.getMessage());
		}
	}
}
