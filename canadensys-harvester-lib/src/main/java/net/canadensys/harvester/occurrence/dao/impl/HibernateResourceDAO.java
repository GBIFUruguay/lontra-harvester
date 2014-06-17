package net.canadensys.harvester.occurrence.dao.impl;

import java.util.List;

import net.canadensys.harvester.occurrence.dao.ResourceDAO;
import net.canadensys.harvester.occurrence.model.ResourceModel;

import org.hibernate.Criteria;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public class HibernateResourceDAO implements ResourceDAO{
	
	@Autowired
	@Qualifier(value="publicSessionFactory")
	private SessionFactory sessionFactory;
	
	public List<ResourceModel> loadResources(){
		Criteria searchCriteria = sessionFactory.getCurrentSession().createCriteria(ResourceModel.class);
		return searchCriteria.list();
	}
}
