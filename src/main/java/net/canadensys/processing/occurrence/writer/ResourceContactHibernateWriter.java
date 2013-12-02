package net.canadensys.processing.occurrence.writer;

import java.util.List;

import net.canadensys.dataportal.occurrence.model.ResourceContactModel;
import net.canadensys.processing.ItemWriterIF;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Item writer for ResourceContactModel using Hibernate
 * @author canadensys
 *
 */
public class ResourceContactHibernateWriter implements ItemWriterIF<ResourceContactModel>{

	@Autowired
	@Qualifier(value="bufferSessionFactory")
	private SessionFactory sessionFactory;
	
	private Session session;
	
	@Override
	public void openWriter() {
		session = sessionFactory.openSession();
	}

	@Override
	public void closeWriter() {
		session.close();
	}

	@Override
	public void write(List<? extends ResourceContactModel> elementList) {
		session.beginTransaction();
		for(ResourceContactModel resourceContactModel: elementList){
			session.save(resourceContactModel);
		}
		session.getTransaction().commit();
	}
	
	@Override
	public void write(ResourceContactModel resourceContactModel) {
		session.beginTransaction();
		session.save(resourceContactModel);
		session.getTransaction().commit();
	}
	
	public void setSessionFactory(SessionFactory sessionFactory){
		this.sessionFactory = sessionFactory;
	}
}
