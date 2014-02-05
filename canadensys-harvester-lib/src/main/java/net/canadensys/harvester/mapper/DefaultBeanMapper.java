package net.canadensys.harvester.mapper;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import net.canadensys.harvester.ItemMapperIF;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.log4j.Logger;

/**
 * Blindly map a set of properties to an object of type defined by the generic.
 * @author canadensys
 *
 * @param <T>
 */
public class DefaultBeanMapper<T> implements ItemMapperIF<T> {
	private static final Logger LOGGER = Logger.getLogger(DefaultBeanMapper.class);
	
	private Class<T> classOfT;
	
	/**
	 * This empty constructor is only to respect Java bean convention.
	 * The other constructor should be used.
	 */
	public DefaultBeanMapper(){}
	
	public DefaultBeanMapper(Class<T> classOfT){
		this.classOfT = classOfT;
	}
	
	@Override
	public T mapElement(Map<String,Object> properties) {
		T object = null;
		try {
			object = classOfT.newInstance();
			BeanUtils.populate(object, properties);
		} catch (IllegalAccessException e) {
			LOGGER.fatal("Can not map properties to object", e);
		} catch (InvocationTargetException e) {
			LOGGER.fatal("Can not map properties to object", e);
		} catch (InstantiationException e) {
			LOGGER.fatal("Can not map properties to object", e);
		}
		return object;
	}
	
	public void setClassOfT(Class<T> classOfT) {
		this.classOfT = classOfT;
	}
}
