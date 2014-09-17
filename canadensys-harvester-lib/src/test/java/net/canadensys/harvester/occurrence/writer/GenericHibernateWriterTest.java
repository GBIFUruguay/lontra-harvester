package net.canadensys.harvester.occurrence.writer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Set;

import javax.sql.DataSource;

import net.canadensys.dataportal.occurrence.model.ResourceContactModel;
import net.canadensys.dataportal.occurrence.model.ResourceInformationModel;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.config.ProcessingConfigTest;
import net.canadensys.harvester.exception.WriterException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ProcessingConfigTest.class, loader = AnnotationConfigContextLoader.class)
public class GenericHibernateWriterTest {

	@Autowired
	private ItemWriterIF<ResourceInformationModel> genericResourceInformationWriter;
	private JdbcTemplate jdbcTemplate;

	@Autowired
	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Test
	public void testInsertInformationAndContact() {
		ResourceInformationModel testInformation = new ResourceInformationModel();
		testInformation.set_abstract("Test abstract information");
		testInformation
				.setCitation("please use this format to cite this dataset");

		/** Add Contact:
		Set<ResourceContactModel> contacts = new HashSet<ResourceContactModel>();
		ResourceContactModel testContact = new ResourceContactModel();
		testContact.setName("Test Contact");
		testContact.setAddress("Fools street, 0");
		contacts.add(testContact);
		testInformation.setContacts(contacts);
		*/
		try {
			genericResourceInformationWriter.write(testInformation);
		} catch (WriterException e) {
			fail();
		}
		
		Number informationId = jdbcTemplate
				.queryForObject(
						"SELECT auto_id FROM buffer.resource_information WHERE _abstract ='Test abstract information'",
						Number.class);
		assertNotNull(informationId);

		/** Contact assert:
		Number contactId = jdbcTemplate
				.queryForObject(
						"SELECT auto_id FROM buffer.resource_contact WHERE name ='Test Contact'",
						Number.class);
		assertNotNull(contactId);
		*/
	}

}
