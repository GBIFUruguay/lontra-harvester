package net.canadensys.harvester.occurrence.writer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import javax.sql.DataSource;

import net.canadensys.dataportal.occurrence.model.ResourceMetadataModel;
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
	private ItemWriterIF<ResourceMetadataModel> genericResourceInformationWriter;
	private JdbcTemplate jdbcTemplate;

	@Autowired
	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Test
	public void testInsertInformationAndContact() {
		ResourceMetadataModel testMetadata = new ResourceMetadataModel();
		testMetadata.setDwca_resource_id(new Integer(1));
		testMetadata.set_abstract("Test abstract information");
		testMetadata.setCitation("please use this format to cite this dataset");

		/**
		 * Add Contact:
		 * Set<ResourceContactModel> contacts = new HashSet<ResourceContactModel>();
		 * ResourceContactModel testContact = new ResourceContactModel();
		 * testContact.setName("Test Contact");
		 * testContact.setAddress("Fools street, 0");
		 * contacts.add(testContact);
		 * testInformation.setContacts(contacts);
		 */
		try {
			genericResourceInformationWriter.write(testMetadata);
		}
		catch (WriterException e) {
			fail();
		}

		Number informationId = jdbcTemplate.queryForObject(
				"SELECT dwca_resource_id FROM buffer.resource_metadata WHERE _abstract ='Test abstract information'", Number.class);
		assertNotNull(informationId);

		/**
		 * Contact assert:
		 * Number contactId = jdbcTemplate
		 * .queryForObject(
		 * "SELECT auto_id FROM buffer.resource_contact WHERE name ='Test Contact'",
		 * Number.class);
		 * assertNotNull(contactId);
		 */
	}

}
