package net.canadensys.harvester.config;

import java.util.Properties;

import javax.sql.DataSource;

import net.canadensys.dataportal.occurrence.dao.ImportLogDAO;
import net.canadensys.dataportal.occurrence.dao.ResourceDAO;
import net.canadensys.dataportal.occurrence.dao.impl.HibernateImportLogDAO;
import net.canadensys.dataportal.occurrence.dao.impl.HibernateResourceDAO;
import net.canadensys.dataportal.occurrence.model.ImportLogModel;
import net.canadensys.dataportal.occurrence.model.OccurrenceModel;
import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.dataportal.occurrence.model.ResourceInformationModel;
import net.canadensys.dataportal.occurrence.model.ResourceModel;
import net.canadensys.harvester.ItemProcessorIF;
import net.canadensys.harvester.ItemReaderIF;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.LongRunningTaskIF;
import net.canadensys.harvester.ProcessingStepIF;
import net.canadensys.harvester.config.harvester.HarvesterConfig;
import net.canadensys.harvester.config.harvester.HarvesterConfigIF;
import net.canadensys.harvester.jms.JMSConsumer;
import net.canadensys.harvester.jms.JMSWriter;
import net.canadensys.harvester.jms.control.JMSControlConsumer;
import net.canadensys.harvester.jms.control.JMSControlProducer;
import net.canadensys.harvester.occurrence.dao.IPTFeedDAO;
import net.canadensys.harvester.occurrence.dao.impl.HibernateIPTFeedDAO;
import net.canadensys.harvester.occurrence.job.ComputeUniqueValueJob;
import net.canadensys.harvester.occurrence.job.ImportDwcaJob;
import net.canadensys.harvester.occurrence.job.MoveToPublicSchemaJob;
import net.canadensys.harvester.occurrence.notification.ResourceStatusNotifierIF;
import net.canadensys.harvester.occurrence.notification.impl.DefaultResourceStatusNotifier;
import net.canadensys.harvester.occurrence.processor.DwcaLineProcessor;
import net.canadensys.harvester.occurrence.processor.OccurrenceProcessor;
import net.canadensys.harvester.occurrence.processor.ResourceInformationProcessor;
import net.canadensys.harvester.occurrence.reader.DwcaEmlReader;
import net.canadensys.harvester.occurrence.reader.DwcaItemReader;
import net.canadensys.harvester.occurrence.step.InsertResourceInformationStep;
import net.canadensys.harvester.occurrence.step.StreamEmlContentStep;
import net.canadensys.harvester.occurrence.step.async.ProcessInsertOccurrenceStep;
import net.canadensys.harvester.occurrence.step.stream.StreamDwcContentStep;
import net.canadensys.harvester.occurrence.task.CheckHarvestingCompletenessTask;
import net.canadensys.harvester.occurrence.task.CleanBufferTableTask;
import net.canadensys.harvester.occurrence.task.ComputeGISDataTask;
import net.canadensys.harvester.occurrence.task.ComputeUniqueValueTask;
import net.canadensys.harvester.occurrence.task.GetResourceInfoTask;
import net.canadensys.harvester.occurrence.task.PrepareDwcaTask;
import net.canadensys.harvester.occurrence.task.RecordImportTask;
import net.canadensys.harvester.occurrence.task.ReplaceOldOccurrenceTask;
import net.canadensys.harvester.occurrence.view.model.HarvesterViewModel;
import net.canadensys.harvester.occurrence.writer.OccurrenceHibernateWriter;
import net.canadensys.harvester.occurrence.writer.RawOccurrenceHibernateWriter;
import net.canadensys.harvester.occurrence.writer.ResourceInformationHibernateWriter;

import org.gbif.metadata.eml.Eml;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
import org.springframework.orm.hibernate4.LocalSessionFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * Configuration class using Spring annotations. All the beans that could be
 * changed based on configuration or could be mock are created from here.
 * 
 * @author canadensys
 * 
 */
@Configuration
@ComponentScan(basePackages = "net.canadensys.harvester", excludeFilters = { @Filter(type = FilterType.CUSTOM, value = { ExcludeTestClassesTypeFilter.class }) })
@EnableTransactionManagement
public class ProcessingConfig {

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

	// optional
	@Value("${ipt.rss:}")
	private String iptRssAddress;
	@Value("${harvester.import.allow_localfile:false}")
	private Boolean allowLocalFileImport;

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
		sb.setAnnotatedClasses(new Class[] { OccurrenceRawModel.class, OccurrenceModel.class, ImportLogModel.class, ResourceModel.class,
				ResourceInformationModel.class });

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
		sb.setAnnotatedClasses(new Class[] { OccurrenceRawModel.class, OccurrenceModel.class, ImportLogModel.class, ResourceModel.class,
				ResourceInformationModel.class });

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

	// ---VIEW MODEL---
	@Bean
	public HarvesterViewModel harvesterViewModel() {
		HarvesterViewModel hvm = new HarvesterViewModel();
		hvm.setDatabaseLocation(dbUrl);
		return hvm;
	}

	// ---JOB---
	@Bean
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
	public ProcessingStepIF streamEmlContentStep() {
		return new StreamEmlContentStep();
	}

	@Bean(name = "streamDwcContentStep")
	public ProcessingStepIF StreamDwcContentStep() {
		return new StreamDwcContentStep();
	}

	@Bean(name = "processInsertOccurrenceStep")
	public ProcessingStepIF processInsertOccurrenceStep() {
		return new ProcessInsertOccurrenceStep();
	}

	@Bean(name = "insertResourceInformationStep")
	public ProcessingStepIF insertResourceInformationStep() {
		return new InsertResourceInformationStep();
	}

	@Bean(name = "processOccurrenceStatisticsStep")
	public ProcessingStepIF processOccurrenceStatisticsStep() {
		return null;
	}

	// ---TASK wiring---
	@Bean
	public ItemTaskIF prepareDwcaTask() {
		PrepareDwcaTask pdwca = new PrepareDwcaTask();
		pdwca.setAllowDatasetShortnameExtraction(allowLocalFileImport);
		return pdwca;
	}

	@Bean
	public ItemTaskIF cleanBufferTableTask() {
		return new CleanBufferTableTask();
	}

	@Bean
	public ItemTaskIF computeGISDataTask() {
		return new ComputeGISDataTask();
	}

	@Bean
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
	public ItemTaskIF recordImportTask() {
		return new RecordImportTask();
	}

	@Bean
	public ItemTaskIF computeUniqueValueTask() {
		return new ComputeUniqueValueTask();
	}

	// ---PROCESSOR wiring---
	@Bean(name = "lineProcessor")
	public ItemProcessorIF<OccurrenceRawModel, OccurrenceRawModel> lineProcessor() {
		DwcaLineProcessor dwcaLineProcessor = new DwcaLineProcessor();
		dwcaLineProcessor.setIdGenerationSQL(idGenerationSQL);
		return dwcaLineProcessor;
	}

	@Bean(name = "occurrenceProcessor")
	public ItemProcessorIF<OccurrenceRawModel, OccurrenceModel> occurrenceProcessor() {
		return new OccurrenceProcessor();
	}

	@Bean(name = "resourceInformationProcessor")
	public ItemProcessorIF<Eml, ResourceInformationModel> resourceContactProcessor() {
		return new ResourceInformationProcessor();
	}

	// ---READER wiring---
	@Bean
	public ItemReaderIF<OccurrenceRawModel> dwcItemReader() {
		return new DwcaItemReader();
	}

	@Bean
	public ItemReaderIF<Eml> dwcaEmlReader() {
		return new DwcaEmlReader();
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
	public ItemWriterIF<ResourceInformationModel> resourceInformationHibernateWriter() {
		return new ResourceInformationHibernateWriter();
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
		return new HibernateIPTFeedDAO();
	}

	@Bean
	public ResourceDAO resourceDAO() {
		return new HibernateResourceDAO();
	}

	@Bean
	public ImportLogDAO importLogDAO() {
		return new HibernateImportLogDAO();
	}

	@Bean
	public ResourceStatusNotifierIF resourceStatusNotifierIF() {
		return new DefaultResourceStatusNotifier();
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
