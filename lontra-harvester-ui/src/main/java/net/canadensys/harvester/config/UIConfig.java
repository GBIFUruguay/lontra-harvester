package net.canadensys.harvester.config;

import java.util.Properties;

import javax.sql.DataSource;

import org.gbif.dwc.terms.Term;
import org.gbif.metadata.eml.Eml;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
import org.springframework.orm.hibernate4.LocalSessionFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import net.canadensys.dataportal.occurrence.dao.DwcaResourceDAO;
import net.canadensys.dataportal.occurrence.dao.ImportLogDAO;
import net.canadensys.dataportal.occurrence.dao.PublisherDAO;
import net.canadensys.dataportal.occurrence.dao.impl.HibernateDwcaResourceDAO;
import net.canadensys.dataportal.occurrence.dao.impl.HibernateImportLogDAO;
import net.canadensys.dataportal.occurrence.dao.impl.HibernatePublisherDAO;
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
import net.canadensys.harvester.config.harvester.HarvesterConfig;
import net.canadensys.harvester.config.harvester.HarvesterConfigIF;
import net.canadensys.harvester.jms.JMSConsumer;
import net.canadensys.harvester.jms.JMSWriter;
import net.canadensys.harvester.jms.control.JMSControlConsumer;
import net.canadensys.harvester.jms.control.JMSControlProducer;
import net.canadensys.harvester.main.JobInitiatorMain;
import net.canadensys.harvester.occurrence.controller.NodeStatusController;
import net.canadensys.harvester.occurrence.controller.StepController;
import net.canadensys.harvester.occurrence.controller.StepControllerIF;
import net.canadensys.harvester.occurrence.dao.IPTFeedDAO;
import net.canadensys.harvester.occurrence.dao.impl.RSSIPTFeedDAO;
import net.canadensys.harvester.occurrence.job.ComputeUniqueValueJob;
import net.canadensys.harvester.occurrence.job.ImportDwcaJob;
import net.canadensys.harvester.occurrence.job.MoveToPublicSchemaJob;
import net.canadensys.harvester.occurrence.job.PublisherNameUpdateJob;
import net.canadensys.harvester.occurrence.job.RemoveDwcaResourceJob;
import net.canadensys.harvester.occurrence.mapper.OccurrenceExtensionMapper;
import net.canadensys.harvester.occurrence.processor.DwcaExtensionLineProcessor;
import net.canadensys.harvester.occurrence.processor.DwcaLineProcessor;
import net.canadensys.harvester.occurrence.processor.OccurrenceProcessor;
import net.canadensys.harvester.occurrence.processor.ResourceMetadataProcessor;
import net.canadensys.harvester.occurrence.reader.DwcaEmlReader;
import net.canadensys.harvester.occurrence.reader.DwcaExtensionInfoReader;
import net.canadensys.harvester.occurrence.reader.DwcaExtensionReader;
import net.canadensys.harvester.occurrence.reader.DwcaItemReader;
import net.canadensys.harvester.occurrence.status.ResourceStatusCheckerIF;
import net.canadensys.harvester.occurrence.status.impl.DefaultResourceStatusChecker;
import net.canadensys.harvester.occurrence.step.HandleDwcaExtensionsStep;
import net.canadensys.harvester.occurrence.step.StreamEmlContentStep;
import net.canadensys.harvester.occurrence.step.stream.StreamDwcContentStep;
import net.canadensys.harvester.occurrence.step.stream.StreamDwcExtensionContentStep;
import net.canadensys.harvester.occurrence.task.CheckHarvestingCompletenessTask;
import net.canadensys.harvester.occurrence.task.ComputeGISDataTask;
import net.canadensys.harvester.occurrence.task.ComputeUniqueValueTask;
import net.canadensys.harvester.occurrence.task.GetResourceInfoTask;
import net.canadensys.harvester.occurrence.task.PostProcessOccurrenceTask;
import net.canadensys.harvester.occurrence.task.PrepareDwcaTask;
import net.canadensys.harvester.occurrence.task.PublisherNameUpdateTask;
import net.canadensys.harvester.occurrence.task.RecordImportTask;
import net.canadensys.harvester.occurrence.task.RemoveDwcaResourceTask;
import net.canadensys.harvester.occurrence.task.ReplaceOldOccurrenceTask;
import net.canadensys.harvester.occurrence.view.OccurrenceHarvesterMainView;
import net.canadensys.harvester.occurrence.view.model.HarvesterViewModel;
import net.canadensys.harvester.occurrence.writer.OccurrenceHibernateWriter;
import net.canadensys.harvester.occurrence.writer.RawOccurrenceHibernateWriter;
import net.canadensys.harvester.occurrence.writer.ResourceMetadataHibernateWriter;

/**
 * Configuration class using Spring annotations.
 * All the beans that could be changed based on configuration or could be mock are created from here.
 *
 * @author canadensys
 *
 */
@Configuration
@EnableTransactionManagement
@ImportResource("classpath:taskDefinitions.xml")
public class UIConfig {

	@Bean
	public static PropertyPlaceholderConfigurer properties() {
		PropertyPlaceholderConfigurer ppc = new PropertyPlaceholderConfigurer();
		ppc.setLocation(new FileSystemResource("config/harvester-config.properties"));
		return ppc;
	}

	@Value("${harvester.library.version:?}")
	private String currentVersion;

	@Value("${database.url}")
	private String dbUrl;
	@Value("${database.driver}")
	private String dbDriverClassName;
	@Value("${database.username}")
	private String username;
	@Value("${database.password}")
	private String password;
	@Value("${database.select_column_names}")
	private String selectColumnNamesSQL;

	@Value("${hibernate.dialect}")
	private String hibernateDialect;
	@Value("${hibernate.show_sql}")
	private String hibernateShowSql;
	@Value("${hibernate.buffer_schema}")
	private String hibernateBufferSchema;
	@Value("${hibernate.jdbc.fetch_size}")
	private String hibernateJDBCFetchSize;

	@Value("${jms.broker_url}")
	private String jmsBrokerUrl;

	@Value("${occurrence.idGenerationSQL}")
	private String idGenerationSQL;

	@Value("${occurrence.extension.idGenerationSQL:}")
	private String extIdGenerationSQL;

	// optional
	@Value("${ipt.rss:}")
	private String iptRssAddress;

	// --- Main ---
	@Bean
	public JobInitiatorMain jobInitiatorMain() {
		return new JobInitiatorMain();
	}

	@Bean(name = "datasource")
	public DataSource dataSource() {
		DriverManagerDataSource ds = new DriverManagerDataSource();
		ds.setDriverClassName(dbDriverClassName);
		ds.setUrl(dbUrl);
		ds.setUsername(username);
		ds.setPassword(password);
		return ds;
	}

	@Bean(name = "bufferSessionFactory")
	public LocalSessionFactoryBean bufferSessionFactory() {
		LocalSessionFactoryBean sb = new LocalSessionFactoryBean();
		sb.setDataSource(dataSource());
		sb.setAnnotatedClasses(new Class[] { OccurrenceRawModel.class,
				OccurrenceModel.class, ImportLogModel.class, ContactModel.class, ResourceMetadataModel.class });

		Properties hibernateProperties = new Properties();
		hibernateProperties.setProperty("hibernate.dialect", hibernateDialect);
		hibernateProperties.setProperty("hibernate.show_sql", hibernateShowSql);
		hibernateProperties.setProperty("hibernate.default_schema", hibernateBufferSchema);
		hibernateProperties.setProperty("hibernate.jdbc.fetch_size", hibernateJDBCFetchSize);
		hibernateProperties.setProperty("javax.persistence.validation.mode", "none");
		sb.setHibernateProperties(hibernateProperties);
		return sb;
	}

	@Bean(name = { "publicSessionFactory", "sessionFactory" })
	public LocalSessionFactoryBean publicSessionFactory() {
		LocalSessionFactoryBean sb = new LocalSessionFactoryBean();
		sb.setDataSource(dataSource());
		sb.setAnnotatedClasses(new Class[] {
				OccurrenceRawModel.class, OccurrenceModel.class,
				ImportLogModel.class, ContactModel.class, ResourceMetadataModel.class,
				DwcaResourceModel.class, PublisherModel.class });

		Properties hibernateProperties = new Properties();
		hibernateProperties.setProperty("hibernate.dialect", hibernateDialect);
		hibernateProperties.setProperty("hibernate.show_sql", hibernateShowSql);
		hibernateProperties.setProperty("hibernate.jdbc.fetch_size", hibernateJDBCFetchSize);
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

	// --- Controllers ---
	@Bean
	public StepControllerIF stepController() {
		return new StepController();
	}

	@Bean
	public NodeStatusController nodeStatusController() {
		return new NodeStatusController();
	}

	// --- VIEW ---
	@Bean
	public OccurrenceHarvesterMainView occurrenceHarvesterMainView() {
		return new OccurrenceHarvesterMainView();
	}

	// --- VIEW MODEL ---
	@Bean
	public HarvesterViewModel harvesterViewModel() {
		HarvesterViewModel hvm = new HarvesterViewModel();
		hvm.setDatabaseLocation(dbUrl);
		return hvm;
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
	
	@Bean
	public RemoveDwcaResourceJob removeDwcaResourceJob() {
		return new RemoveDwcaResourceJob();
	}

	@Bean PublisherNameUpdateJob publisherNameUpdateJob() {
		return new PublisherNameUpdateJob();
	}
	
	// ---STEP---
	@Bean(name = "streamEmlContentStep")
	public StepIF streamEmlContentStep() {
		return new StreamEmlContentStep();
	}

	@Bean(name = "streamDwcContentStep")
	public StepIF StreamDwcContentStep() {
		return new StreamDwcContentStep();
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

	// ---TASK wiring---
	@Bean
	public ItemTaskIF prepareDwcaTask() {
		return new PrepareDwcaTask();
	}

	@Bean
	public ItemTaskIF computeGISDataTask() {
		return new ComputeGISDataTask();
	}

	@Bean
	@Scope("prototype")
	public LongRunningTaskIF checkProcessingCompletenessTask() {
		return new CheckHarvestingCompletenessTask();
	}

	@Bean
	public ItemTaskIF getResourceInfoTask() {
		return new GetResourceInfoTask();
	}

	@Bean
	public ItemTaskIF replaceOldOccurrenceTask() {
		return new ReplaceOldOccurrenceTask();
	}
	
	@Bean
	public ItemTaskIF removeDwcaResourceTask() {
		return new RemoveDwcaResourceTask();
	}
	
	@Bean ItemTaskIF publisherNameUpdateTask() {
		return new PublisherNameUpdateTask();
	}

	@Bean
	public ItemTaskIF recordImportTask() {
		return new RecordImportTask();
	}

	@Bean
	public ItemTaskIF computeUniqueValueTask() {
		return new ComputeUniqueValueTask();
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
		return new ResourceMetadataProcessor();
	}

	// ---READER wiring---
	@Bean
	@Scope("prototype")
	public ItemReaderIF<OccurrenceRawModel> dwcItemReader() {
		return new DwcaItemReader();
	}

	@Bean
	@Scope("prototype")
	public ItemReaderIF<Eml> dwcaEmlReader() {
		return new DwcaEmlReader();
	}

	@Bean
	@Scope("prototype")
	public ItemReaderIF<Term> dwcaExtensionInfoReader() {
		return new DwcaExtensionInfoReader();
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

	// ---MAPPER---
	@Bean(name = "occurrenceExtensionMapper")
	public ItemMapperIF<OccurrenceExtensionModel> occurrenceExtensionMapper() {
		return new OccurrenceExtensionMapper();
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

	// ---Config---
	@Bean
	public HarvesterConfigIF harvesterConfig() {
		HarvesterConfig hc = new HarvesterConfig();
		hc.setIptRssAddress(iptRssAddress);
		return hc;
	}

	// ---DAO---
	@Bean
	public IPTFeedDAO iptFeedDAO() {
		return new RSSIPTFeedDAO();
	}

	@Bean
	public DwcaResourceDAO resourceDAO() {
		return new HibernateDwcaResourceDAO();
	}

	@Bean
	public PublisherDAO publisherDAO() {
		return new HibernatePublisherDAO();
	}

	@Bean
	public ImportLogDAO importLogDAO() {
		return new HibernateImportLogDAO();
	}

	@Bean
	public ResourceStatusCheckerIF resourceStatusNotifierIF() {
		return new DefaultResourceStatusChecker();
	}

	/**
	 * Always return a new instance. We do not want to share JMS Writer instance.
	 *
	 * @return
	 */
	@Bean
	@Scope("prototype")
	public JMSWriter jmsWriter() {
		return new JMSWriter(jmsBrokerUrl);
	}

	@Bean(name = "jmsConsumer")
	public JMSConsumer jmsConsumer() {
		return null;
	}

	@Bean(destroyMethod = "close")
	public JMSControlProducer controlMessageProducer() {
		return new JMSControlProducer(jmsBrokerUrl);
	}

	@Bean(destroyMethod = "close")
	public JMSControlConsumer errorReceiver() {
		return new JMSControlConsumer(jmsBrokerUrl);
	}

	@Bean(name = "currentVersion")
	public String currentVersion() {
		return currentVersion;
	}
}
