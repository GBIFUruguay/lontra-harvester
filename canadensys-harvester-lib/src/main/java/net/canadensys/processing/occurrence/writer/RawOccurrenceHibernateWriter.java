package net.canadensys.processing.occurrence.writer;

import java.util.List;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.ItemWriterIF;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Item writer for OccurrenceRawModel using Hibernate
 * @author canadensys
 *
 */
public class RawOccurrenceHibernateWriter implements ItemWriterIF<OccurrenceRawModel>{

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
	public void write(List<? extends OccurrenceRawModel> elementList) {
		Transaction tx = session.beginTransaction();
		for(OccurrenceRawModel currRawOccurrence: elementList){
			session.insert(currRawOccurrence);
		}
		tx.commit();
	}

	@Override
	public void write(OccurrenceRawModel rawModel) {
		Session currSession = sessionFactory.getCurrentSession();
		currSession.beginTransaction();
		currSession.save(rawModel);
		currSession.getTransaction().commit();
	}

	public void setSessionFactory(SessionFactory sessionFactory){
		this.sessionFactory = sessionFactory;
	}

}
