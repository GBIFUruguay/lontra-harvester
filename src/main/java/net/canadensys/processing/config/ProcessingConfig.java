package net.canadensys.processing.config;

import java.util.Properties;

import javax.sql.DataSource;

import net.canadensys.dataportal.occurrence.model.OccurrenceModel;
import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.dataportal.occurrence.model.ResourceContactModel;
import net.canadensys.processing.ExcludeTestClassesTypeFilter;
import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.ItemReaderIF;
import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.ItemWriterIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.jms.JMSConsumer;
import net.canadensys.processing.jms.JMSWriter;
import net.canadensys.processing.occurrence.job.ComputeStatisticsJob;
import net.canadensys.processing.occurrence.job.ComputeUniqueValueJob;
import net.canadensys.processing.occurrence.job.FindUsedDwcaTermJob;
import net.canadensys.processing.occurrence.job.ImportDwcaJob;
import net.canadensys.processing.occurrence.job.MoveToPublicSchemaJob;
import net.canadensys.processing.occurrence.job.UpdateResourceContactJob;
import net.canadensys.processing.occurrence.model.ImportLogModel;
import net.canadensys.processing.occurrence.model.OccurrenceQualityReportElement;
import net.canadensys.processing.occurrence.model.ResourceModel;
import net.canadensys.processing.occurrence.processor.DwcaLineProcessor;
import net.canadensys.processing.occurrence.processor.OccurrenceProcessor;
import net.canadensys.processing.occurrence.processor.ResourceContactProcessor;
import net.canadensys.processing.occurrence.reader.DwcaEmlReader;
import net.canadensys.processing.occurrence.reader.DwcaItemReader;
import net.canadensys.processing.occurrence.reader.RawOccurrenceHibernateReader;
import net.canadensys.processing.occurrence.step.InsertRawOccurrenceStep;
import net.canadensys.processing.occurrence.step.InsertResourceContactStep;
import net.canadensys.processing.occurrence.step.ProcessInsertOccurrenceStep;
import net.canadensys.processing.occurrence.step.StreamDwcaContentStep;
import net.canadensys.processing.occurrence.step.StreamEmlContentStep;
import net.canadensys.processing.occurrence.step.StreamOccurrenceForStatsStep;
import net.canadensys.processing.occurrence.step.UpdateResourceContactStep;
import net.canadensys.processing.occurrence.task.CheckProcessingCompletenessTask;
import net.canadensys.processing.occurrence.task.CleanBufferTableTask;
import net.canadensys.processing.occurrence.task.ComputeGISDataTask;
import net.canadensys.processing.occurrence.task.ComputeUniqueValueTask;
import net.canadensys.processing.occurrence.task.FindUsedDwcaTermTask;
import net.canadensys.processing.occurrence.task.GetResourceInfoTask;
import net.canadensys.processing.occurrence.task.PrepareDwcaTask;
import net.canadensys.processing.occurrence.task.RecordImportTask;
import net.canadensys.processing.occurrence.task.ReplaceOldOccurrenceTask;
import net.canadensys.processing.occurrence.view.HarvesterViewModel;
import net.canadensys.processing.occurrence.writer.OccurrenceHibernateWriter;
import net.canadensys.processing.occurrence.writer.RawOccurrenceHibernateWriter;
import net.canadensys.processing.occurrence.writer.ResourceContactHibernateWriter;

import org.gbif.metadata.eml.Eml;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
import org.springframework.orm.hibernate4.LocalSessionFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * Configuration class using Spring annotations.
 * All the beans that could be changed based on configuration or could be mock are created from here.
 * @author canadensys
 *
 */
@Configuration
@ComponentScan(basePackages ="net.canadensys.processing",
	excludeFilters = { @Filter(type = FilterType.CUSTOM, value = { ExcludeTestClassesTypeFilter.class }),
	@Filter(type = FilterType.ASSIGNABLE_TYPE, value = { ProcessingNodeConfig.class })})
@EnableTransactionManagement
public class ProcessingConfig {
	
    @Bean
    public static PropertyPlaceholderConfigurer properties(){
    	PropertyPlaceholderConfigurer ppc = new PropertyPlaceholderConfigurer();
    	ClassPathResource[] resources = new ClassPathResource[]
    			{ new ClassPathResource( "harvester-config.properties" ) };
    	ppc.setLocations( resources );
    	return ppc;
    }
    
    @Value("${database.url}")
    private String dbUrl;
    @Value( "${database.driver}" )
    private String dbDriverClassName;
    @Value( "${database.username}" )
    private String username;
    @Value( "${database.password}" )
    private String password;
    
    @Value( "${hibernate.dialect}" )
    private String hibernateDialect;
    @Value( "${hibernate.show_sql}" )
    private String hibernateShowSql;
    @Value( "${hibernate.buffer_schema}" )
    private String hibernateBufferSchema;
    @Value( "${hibernate.jdbc.fetch_size}" )
    private String hibernateJDBCFetchSize;
    
    @Value("${jms.broker_url}")
    private String jmsBrokerUrl;
    
    @Value("${occurrence.idGenerationSQL}")
    private String idGenerationSQL;
    
    @Bean(name="datasource")
    public DataSource dataSource() {
    	DriverManagerDataSource ds = new DriverManagerDataSource();
    	ds.setDriverClassName(dbDriverClassName);
    	ds.setUrl(dbUrl);
    	ds.setUsername(username);
    	ds.setPassword(password);
    	return ds;
    }
    
    @Bean(name="bufferSessionFactory")
    public LocalSessionFactoryBean bufferSessionFactory() {
    	LocalSessionFactoryBean sb = new LocalSessionFactoryBean(); 
    	sb.setDataSource(dataSource()); 
    	sb.setAnnotatedClasses(new Class[]{OccurrenceRawModel.class,
    			OccurrenceModel.class,ImportLogModel.class,ResourceContactModel.class});

		Properties hibernateProperties = new Properties();
		hibernateProperties.setProperty("hibernate.dialect", hibernateDialect);
		hibernateProperties.setProperty("hibernate.show_sql", hibernateShowSql);
		hibernateProperties.setProperty("hibernate.default_schema", hibernateBufferSchema);
		hibernateProperties.setProperty("hibernate.jdbc.fetch_size", hibernateJDBCFetchSize);
		hibernateProperties.setProperty("javax.persistence.validation.mode", "none");
    	sb.setHibernateProperties(hibernateProperties);
    	return sb;
    }
    
    @Bean(name="publicSessionFactory")
    public LocalSessionFactoryBean publicSessionFactory() {
    	LocalSessionFactoryBean sb = new LocalSessionFactoryBean(); 
    	sb.setDataSource(dataSource()); 
    	sb.setAnnotatedClasses(new Class[]{
    			OccurrenceRawModel.class,OccurrenceModel.class,
    			ImportLogModel.class, ResourceModel.class, });

		Properties hibernateProperties = new Properties();
		hibernateProperties.setProperty("hibernate.dialect", hibernateDialect);
		hibernateProperties.setProperty("hibernate.show_sql", hibernateShowSql);
		hibernateProperties.setProperty("hibernate.jdbc.fetch_size", hibernateJDBCFetchSize);
		hibernateProperties.setProperty("javax.persistence.validation.mode", "none");
    	sb.setHibernateProperties(hibernateProperties);
    	return sb;
    }
    
    @Bean(name="bufferTransactionManager")
    public HibernateTransactionManager hibernateTransactionManager(){
    	HibernateTransactionManager htmgr = new HibernateTransactionManager();
		htmgr.setSessionFactory(bufferSessionFactory().getObject());
    	return htmgr;
    }
    
    @Bean(name="publicTransactionManager")
    public HibernateTransactionManager publicHibernateTransactionManager(){
    	HibernateTransactionManager htmgr = new HibernateTransactionManager();
		htmgr.setSessionFactory(publicSessionFactory().getObject());
    	return htmgr;
    }
    
    //---VIEW MODEL---
	@Bean
	public HarvesterViewModel harvesterViewModel(){
		HarvesterViewModel hvm = new HarvesterViewModel();
		hvm.setDatabaseLocation(dbUrl);
		return hvm;
	}
	
    //---JOB---
	@Bean
	public ImportDwcaJob importDwcaJob(){
		return new ImportDwcaJob();
	}
	@Bean
	public MoveToPublicSchemaJob moveToPublicSchemaJob(){
		return new MoveToPublicSchemaJob();
	}
	@Bean
	public ComputeUniqueValueJob computeUniqueValueJob(){
		return new ComputeUniqueValueJob();
	}
	@Bean
	public UpdateResourceContactJob updateResourceContactJob(){
		return new UpdateResourceContactJob();
	}
	@Bean
	public ComputeStatisticsJob computeStatisticsJob(){
		return new ComputeStatisticsJob();
	}
	@Bean
	public FindUsedDwcaTermJob findUsedDwcaTermJob(){
		return new FindUsedDwcaTermJob();
	}
	
	//---STEP---
	@Bean(name="streamEmlContentStep")
	public ProcessingStepIF streamEmlContentStep(){
		return new StreamEmlContentStep();
	}
	
	@Bean(name="streamDwcaContentStep")
	public ProcessingStepIF StreamDwcaContentStep(){
		return new StreamDwcaContentStep();
	}
	
	@Bean(name="insertRawOccurrenceStep")
	public ProcessingStepIF insertRawOccurrenceStep(){
		return new InsertRawOccurrenceStep();
	}
	
	@Bean(name="processInsertOccurrenceStep")
	public ProcessingStepIF processInsertOccurrenceStep(){
		return new ProcessInsertOccurrenceStep();
	}
	
	@Bean(name="insertResourceContactStep")
	public ProcessingStepIF insertResourceContactStep(){
		return new InsertResourceContactStep();
	}
	
	@Bean(name="updateResourceContactStep")
	public ProcessingStepIF updateResourceContactStep(){
		return new UpdateResourceContactStep();
	}
	
	@Bean(name="processOccurrenceStatisticsStep")
	public ProcessingStepIF processOccurrenceStatisticsStep(){
		return null;
	}
	
	@Bean(name="streamOccurrenceForStatsStep")
	public ProcessingStepIF streamOccurrenceForStatsStep(){
		return new StreamOccurrenceForStatsStep();
	}
	
	//---TASK wiring---
	
	@Bean
	public ItemTaskIF prepareDwcaTask(){
		return new PrepareDwcaTask();
	}
	
	@Bean
	public ItemTaskIF cleanBufferTableTask(){
		return new CleanBufferTableTask();
	}
	
	@Bean
	public ItemTaskIF computeGISDataTask(){
		return new ComputeGISDataTask();
	}
	
	@Bean
	public ItemTaskIF checkProcessingCompletenessTask(){
		return new CheckProcessingCompletenessTask();
	}
	
	@Bean
	public ItemTaskIF getResourceInfoTask(){
		return new GetResourceInfoTask();
	}
	
	@Bean
	public ItemTaskIF replaceOldOccurrenceTask(){
		return new ReplaceOldOccurrenceTask();
	}
	
	@Bean
	public ItemTaskIF recordImportTask(){
		return new RecordImportTask();
	}
	
	@Bean
	public ItemTaskIF computeUniqueValueTask(){
		return new ComputeUniqueValueTask();
	}
	
	@Bean
	public ItemTaskIF findUsedDwcaTermTask(){
		return new FindUsedDwcaTermTask();
	}
	
	
	//---PROCESSOR wiring---
	@Bean(name="lineProcessor")
	public ItemProcessorIF<OccurrenceRawModel, OccurrenceRawModel> lineProcessor(){
		DwcaLineProcessor dwcaLineProcessor = new DwcaLineProcessor();
		dwcaLineProcessor.setIdGenerationSQL(idGenerationSQL);
		return dwcaLineProcessor;
	}
	
	@Bean(name="occurrenceProcessor")
	public ItemProcessorIF<OccurrenceRawModel, OccurrenceModel> occurrenceProcessor(){
		return new OccurrenceProcessor();
	}
	
	@Bean(name="resourceContactProcessor")
	public ItemProcessorIF<Eml, ResourceContactModel> resourceContactProcessor(){
		return new ResourceContactProcessor();
	}
	//should only be used by nodes
	@Bean(name="occurrenceQualityProcessor")
	public ItemProcessorIF<OccurrenceRawModel,OccurrenceQualityReportElement> occurrenceQualityProcessor(){
		return null;
	}
	
	//---READER wiring---
	@Bean
	public ItemReaderIF<OccurrenceRawModel> dwcaItemReader(){
		return new DwcaItemReader();
	}
	
	@Bean
	public ItemReaderIF<Eml> dwcaEmlReader(){
		return new DwcaEmlReader();
	}
	
	@Bean
	public ItemReaderIF<OccurrenceRawModel> rawOccurrenceHibernateReader(){
		return new RawOccurrenceHibernateReader();
	}
	
	//---WRITER wiring---
	@Bean(name="rawOccurrenceWriter")
	public ItemWriterIF<OccurrenceRawModel> rawOccurrenceWriter(){
		return new RawOccurrenceHibernateWriter();
	}
	
	@Bean(name="occurrenceWriter")
	public ItemWriterIF<OccurrenceModel> occurrenceWriter(){
		return new OccurrenceHibernateWriter();
	}
	
	@Bean(name="resourceContactWriter")
	public ItemWriterIF<ResourceContactModel> resourceContactHibernateWriter(){
		return new ResourceContactHibernateWriter();
	}
	
	/**
	 * Always return a new instance. We do not want to share JMS Writer instance.
	 * @return
	 */
	@Bean
	@Scope("prototype")
	public JMSWriter jmsWriter(){
		return new JMSWriter(jmsBrokerUrl);
	}
	
	@Bean(name="jmsConsumer")
	public JMSConsumer jmsConsumer(){
		return null;
	}
}
