package net.canadensys.processing.occurrence.mock.mapper;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import net.canadensys.processing.ItemMapperIF;
import net.canadensys.processing.occurrence.mock.MockHabitObject;

import org.apache.commons.beanutils.BeanUtils;

/**
 * Mock mapper to create a MockHabitObject from properties
 * @author canadensys
 *
 */
public class MockHabitMapper implements ItemMapperIF<MockHabitObject> {
	@Override
	public MockHabitObject mapElement(Map<String,Object> properties) {
		MockHabitObject habitObject = new MockHabitObject();
		try {
			BeanUtils.populate(habitObject, properties);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return habitObject;
	}
}