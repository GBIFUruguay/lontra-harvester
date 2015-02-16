package net.canadensys.harvester.config;

import java.util.Properties;

import javax.sql.DataSource;

import net.canadensys.databaseutils.model.DBMetadata;
import net.canadensys.dataportal.occurrence.model.ContactModel;
import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.dataportal.occurrence.model.ImportLogModel;
import net.canadensys.dataportal.occurrence.model.OccurrenceExtensionModel;
import net.canadensys.dataportal.occurrence.model.OccurrenceModel;
import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.dataportal.occurrence.model.PublisherModel;
import net.canadensys.dataportal.occurrence.model.ResourceMetadataModel;
import net.canadensys.harvester.ItemMapperIF;
import net.canadensys.harvester.ItemProcessorIF;
import net.canadensys.harvester.ItemReaderIF;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.LongRunningTaskIF;
import net.canadensys.harvester.StepIF;
import net.canadensys.harvester.jms.JMSWriter;
import net.canadensys.harvester.jms.control.JMSControlProducer;
import net.canadensys.harvester.occurrence.job.ComputeUniqueValueJob;
import net.canadensys.harvester.occurrence.job.ImportDwcaJob;
import net.canadensys.harvester.occurrence.job.MoveToPublicSchemaJob;
import net.canadensys.harvester.occurrence.mapper.OccurrenceExtensionMapper;
import net.canadensys.harvester.occurrence.mock.MockComputeGISDataTask;
import net.canadensys.harvester.occurrence.processor.DwcaExtensionLineProcessor;
import net.canadensys.harvester.occurrence.processor.DwcaLineProcessor;
import net.canadensys.harvester.occurrence.processor.OccurrenceProcessor;
import net.canadensys.harvester.occurrence.processor.ResourceInformationProcessor;
import net.canadensys.harvester.occurrence.reader.DwcaEmlReader;
import net.canadensys.harvester.occurrence.reader.DwcaExtensionInfoReader;
import net.canadensys.harvester.occurrence.reader.DwcaExtensionReader;
import net.canadensys.harvester.occurrence.reader.DwcaItemReader;
import net.canadensys.harvester.occurrence.step.HandleDwcaExtensionsStep;
import net.canadensys.harvester.occurrence.step.InsertResourceInformationStep;
import net.canadensys.harvester.occurrence.step.StreamEmlContentStep;
import net.canadensys.harvester.occurrence.step.SynchronousProcessEmlContentStep;
import net.canadensys.harvester.occurrence.step.SynchronousProcessOccurrenceExtensionStep;
import net.canadensys.harvester.occurrence.step.async.ProcessInsertOccurrenceStep;
import net.canadensys.harvester.occurrence.step.stream.StreamDwcContentStep;
import net.canadensys.harvester.occurrence.step.stream.StreamDwcExtensionContentStep;
import net.canadensys.harvester.occurrence.task.CheckHarvestingCompletenessTask;
import net.canadensys.harvester.occurrence.task.ComputeUniqueValueTask;
import net.canadensys.harvester.occurrence.task.GetResourceInfoTask;
import net.canadensys.harvester.occurrence.task.PostProcessOccurrenceTask;
import net.canadensys.harvester.occurrence.task.PrepareDwcaTask;
import net.canadensys.harvester.occurrence.task.RecordImportTask;
import net.canadensys.harvester.occurrence.task.ReplaceOldOccurrenceTask;
import net.canadensys.harvester.occurrence.writer.OccurrenceHibernateWriter;
import net.canadensys.harvester.occurrence.writer.RawOccurrenceHibernateWriter;
import net.canadensys.harvester.occurrence.writer.ResourceMetadataHibernateWriter;
import net.canadensys.harvester.task.ValidateSchemaVersion;
import net.canadensys.harvester.writer.GenericHibernateWriter;

import org.gbif.metadata.eml.Eml;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
import org.springframework.orm.hibernate4.LocalSessionFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableTransactionManagement
@ImportResource("classpath:taskDefinitions.xml")
public class ProcessingConfigTest {

	@Bean
	public static PropertyPlaceholderConfigurer properties() {
		PropertyPlaceholderConfigurer ppc = new PropertyPlaceholderConfigurer();
		ClassPathResource[] resources = new ClassPathResource[] { new ClassPathResource("test-harvester-config.properties") };
		ppc.setLocations(resources);
		return ppc;
	}

	@Value("${database.url}")
	private String dbUrl;

	@Value("${database.driver}")
	private String dbDriverClassName;

	@Value("${database.select_column_names}")
	private String selectColumnNamesSQL;

	@Value("${hibernate.dialect}")
	private String hibernateDialect;

	@Value("${hibernate.show_sql}")
	private String hibernateShowSql;

	@Value("${hibernate.buffer_schema}")
	private String hibernateBufferSchema;

	@Value("${occurrence.idGenerationSQL}")
	private String idGenerationSQL;

	@Value("${occurrence.extension.idGenerationSQL}")
	private String extIdGenerationSQL;

	@Value("${jms.broker_url}")
	private String jmsBrokerUrl;

	@Bean(name = "datasource")
	public DataSource dataSource() {
		return new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2).addScript("classpath:h2/h2setup.sql")
				// those 2 scripts are loaded from liger-data-access
				.addScript("/script/occurrence/create_occurrence_tables.sql")
				.addScript("/script/occurrence/create_occurrence_tables_buffer_schema.sql").build();
	}

	@Bean(name = "bufferSessionFactory")
	public LocalSessionFactoryBean bufferSessionFactory() {
		LocalSessionFactoryBean sb = new LocalSessionFactoryBean();
		sb.setDataSource(dataSource());
		sb.setAnnotatedClasses(new Class[] { OccurrenceRawModel.class, OccurrenceModel.class, ImportLogModel.class, ContactModel.class,
				ResourceMetadataModel.class, OccurrenceExtensionModel.class, DwcaResourceModel.class, PublisherModel.class });
		Properties hibernateProperties = new Properties();
		hibernateProperties.setProperty("hibernate.dialect", hibernateDialect);
		hibernateProperties.setProperty("hibernate.show_sql", hibernateShowSql);
		hibernateProperties.setProperty("hibernate.default_schema", hibernateBufferSchema);
		hibernateProperties.setProperty("javax.persistence.validation.mode", "none");
		sb.setHibernateProperties(hibernateProperties);
		return sb;
	}

	@Bean(name = "publicSessionFactory")
	public LocalSessionFactoryBean publicSessionFactory() {
		LocalSessionFactoryBean sb = new LocalSessionFactoryBean();
		sb.setDataSource(dataSource());
		sb.setAnnotatedClasses(new Class[] { OccurrenceRawModel.class, OccurrenceModel.class, ImportLogModel.class, ContactModel.class,
				ResourceMetadataModel.class, OccurrenceExtensionModel.class, DwcaResourceModel.class, PublisherModel.class, DBMetadata.class });

		Properties hibernateProperties = new Properties();
		hibernateProperties.setProperty("hibernate.dialect", hibernateDialect);
		hibernateProperties.setProperty("hibernate.show_sql", hibernateShowSql);
		hibernateProperties.setProperty("javax.persistence.validation.mode", "none");
		sb.setHibernateProperties(hibernateProperties);
		return sb;
	}

	@Bean(name = "bufferTransactionManager")
	public HibernateTransactionManager hibernateTransactionManager() {
		HibernateTransactionManager htmgr = new HibernateTransactionManager();
		htmgr.setSessionFactory(bufferSessionFactory().getObject());
		return htmgr;
	}

	@Bean(name = "publicTransactionManager")
	public HibernateTransactionManager publicHibernateTransactionManager() {
		HibernateTransactionManager htmgr = new HibernateTransactionManager();
		htmgr.setSessionFactory(publicSessionFactory().getObject());
		return htmgr;
	}

	@Bean
	public NamedParameterJdbcTemplate namedParameterJdbcTemplate() {
		return new NamedParameterJdbcTemplate(dataSource());
	}

	@Bean
	public DatabaseConfig databaseConfig() {
		DatabaseConfig databaseConfig = new DatabaseConfig();
		databaseConfig.setSelectColumnNamesSQL(selectColumnNamesSQL);
		return databaseConfig;
	}

	// ---JOB---
	@Bean
	@Scope("prototype")
	public ImportDwcaJob importDwcaJob() {
		return new ImportDwcaJob();
	}

	@Bean
	public MoveToPublicSchemaJob moveToPublicSchemaJob() {
		return new MoveToPublicSchemaJob();
	}

	@Bean
	public ComputeUniqueValueJob computeUniqueValueJob() {
		return new ComputeUniqueValueJob();
	}

	// ---STEP---
	@Bean(name = "streamEmlContentStep")
	public StepIF streamEmlContentStep() {
		return new StreamEmlContentStep();
	}

	@Bean(name = "streamDwcContentStep")
	public StepIF streamDwcContentStep() {
		return new StreamDwcContentStep();
	}

	@Bean(name = "processInsertOccurrenceStep")
	public StepIF processInsertOccurrenceStep() {
		return new ProcessInsertOccurrenceStep();
	}

	@Bean(name = "insertResourceInformationStep")
	public StepIF insertResourceInformationStep() {
		return new InsertResourceInformationStep();
	}

	@Bean
	@Scope("prototype")
	public StepIF handleDwcaExtensionsStep() {
		return new HandleDwcaExtensionsStep();
	}

	@Bean
	@Scope("prototype")
	public StepIF streamDwcExtensionContentStep() {
		return new StreamDwcExtensionContentStep();
	}

	@Bean(name = "synchronousProcessOccurrenceExtensionStep")
	public StepIF synchronousProcessOccurrenceExtensionStep() {
		return new SynchronousProcessOccurrenceExtensionStep();
	}

	@Bean(name = "synchronousProcessEmlContentStep")
	public StepIF synchronousProcessEmlContentStep() {
		return new SynchronousProcessEmlContentStep();
	}

	// ---TASK wiring---
	@Bean
	public ItemTaskIF prepareDwcaTask() {
		return new PrepareDwcaTask();
	}

	@Bean
	public ItemTaskIF computeGISDataTask() {
		return new MockComputeGISDataTask();
	}

	@Bean
	public LongRunningTaskIF checkProcessingCompletenessTask() {
		return new CheckHarvestingCompletenessTask();
	}

	@Bean
	public ItemTaskIF validateSchemaVersion() {
		return new ValidateSchemaVersion();
	}

	@Bean
	public ItemTaskIF getResourceInfoTask() {
		return new GetResourceInfoTask();
	}

	@Bean
	public ItemTaskIF computeUniqueValueTask() {
		return new ComputeUniqueValueTask();
	}

	@Bean
	public ItemTaskIF replaceOldOccurrenceTask() {
		return new ReplaceOldOccurrenceTask();
	}

	@Bean
	public ItemTaskIF recordImportTask() {
		return new RecordImportTask();
	}

	@Bean
	public ItemTaskIF postProcessOccurrenceTask() {
		return new PostProcessOccurrenceTask();
	}

	// ---PROCESSOR wiring---
	@Bean(name = "lineProcessor")
	public ItemProcessorIF<OccurrenceRawModel, OccurrenceRawModel> lineProcessor() {
		DwcaLineProcessor dwcaLineProcessor = new DwcaLineProcessor();
		dwcaLineProcessor.setIdGenerationSQL(idGenerationSQL);
		return dwcaLineProcessor;
	}

	@Bean(name = "extLineProcessor")
	public ItemProcessorIF<OccurrenceExtensionModel, OccurrenceExtensionModel> extLineProcessor() {
		DwcaExtensionLineProcessor dwcaLineProcessor = new DwcaExtensionLineProcessor();
		dwcaLineProcessor.setIdGenerationSQL(extIdGenerationSQL);
		return dwcaLineProcessor;
	}

	@Bean(name = "occurrenceProcessor")
	public ItemProcessorIF<OccurrenceRawModel, OccurrenceModel> occurrenceProcessor() {
		return new OccurrenceProcessor();
	}

	@Bean(name = "resourceInformationProcessor")
	public ItemProcessorIF<Eml, ResourceMetadataModel> resourceInformationProcessor() {
		return new ResourceInformationProcessor();
	}

	// ---READER wiring---
	@Bean
	@Scope("prototype")
	public ItemReaderIF<OccurrenceRawModel> dwcItemReader() {
		return new DwcaItemReader();
	}

	@Bean
	public ItemReaderIF<Eml> dwcaEmlReader() {
		return new DwcaEmlReader();
	}

	@Bean
	public ItemReaderIF<String> dwcaExtensionInfoReader() {
		return new DwcaExtensionInfoReader();
	}

	// --- MAPPER ---
	@Bean(name = "occurrenceExtensionMapper")
	public ItemMapperIF<OccurrenceExtensionModel> occurrenceExtensionMapper() {
		return new OccurrenceExtensionMapper();
	}

	/**
	 * Always return a new instance.
	 * 
	 * @return
	 */
	@Bean
	@Scope("prototype")
	public ItemReaderIF<OccurrenceExtensionModel> dwcaOccurrenceExtensionReader() {
		DwcaExtensionReader<OccurrenceExtensionModel> dwcaExtReader = new DwcaExtensionReader<OccurrenceExtensionModel>();
		dwcaExtReader.setMapper(occurrenceExtensionMapper());
		return dwcaExtReader;
	}

	// ---WRITER wiring---
	@Bean(name = "rawOccurrenceWriter")
	public ItemWriterIF<OccurrenceRawModel> rawOccurrenceWriter() {
		return new RawOccurrenceHibernateWriter();
	}

	@Bean(name = "occurrenceWriter")
	public ItemWriterIF<OccurrenceModel> occurrenceWriter() {
		return new OccurrenceHibernateWriter();
	}

	@Bean(name = "resourceInformationWriter")
	public ItemWriterIF<ResourceMetadataModel> resourceInformationHibernateWriter() {
		return new ResourceMetadataHibernateWriter();
	}

	@Bean(name = "occurrenceExtensionWriter")
	public ItemWriterIF<OccurrenceExtensionModel> occurrenceExtensionWriter() {
		return new GenericHibernateWriter<OccurrenceExtensionModel>();
	}

	/**
	 * Always return a new instance. We do not want to share JMS Writer
	 * instance.
	 * 
	 * @return
	 */
	@Bean
	@Scope("prototype")
	public JMSWriter jmsWriter() {
		return new JMSWriter(jmsBrokerUrl);
	}

	@Bean
	public JMSControlProducer errorReporter() {
		return new JMSControlProducer(jmsBrokerUrl);
	}

}
