package net.canadensys.processing.occurrence.writer;

import java.util.List;

import net.canadensys.dataportal.occurrence.model.OccurrenceModel;
import net.canadensys.processing.ItemWriterIF;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Item writer for OccurrenceModel using Hibernate
 * @author canadensys
 *
 */
public class OccurrenceHibernateWriter implements ItemWriterIF<OccurrenceModel> {
	
	@Autowired
	@Qualifier(value="bufferSessionFactory")
	private SessionFactory sessionFactory;
	
	private StatelessSession session;
	
	@Override
	public void openWriter() {
	    session = sessionFactory.openStatelessSession();
	}

	@Override
	public void closeWriter() {
		session.close();
	}

	@Override
	public void write(List<? extends OccurrenceModel> elementList) {
		Transaction tx = session.beginTransaction();
		for(OccurrenceModel currOccurrence: elementList){
			session.insert(currOccurrence);
		}
		tx.commit();
	}
	
	@Override
	public void write(OccurrenceModel occModel) {
		Session currSession = sessionFactory.getCurrentSession();
		currSession.beginTransaction();
		currSession.save(occModel);
		currSession.getTransaction().commit();
	}
	
	public void setSessionFactory(SessionFactory sessionFactory){
		this.sessionFactory = sessionFactory;
	}
}
