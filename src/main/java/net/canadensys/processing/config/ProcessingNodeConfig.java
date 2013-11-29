package net.canadensys.processing.config;

import java.beans.PropertyVetoException;
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
import net.canadensys.processing.occurrence.processor.OccurrenceQualityProcessor;
import net.canadensys.processing.occurrence.processor.ResourceContactProcessor;
import net.canadensys.processing.occurrence.reader.DwcaItemReader;
import net.canadensys.processing.occurrence.step.InsertRawOccurrenceStep;
import net.canadensys.processing.occurrence.step.InsertResourceContactStep;
import net.canadensys.processing.occurrence.step.ProcessInsertOccurrenceStep;
import net.canadensys.processing.occurrence.step.ProcessOccurrenceStatisticsStep;
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
import org.springframework.core.io.ClassPathResource;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
import org.springframework.orm.hibernate4.LocalSessionFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * Configuration class using Spring annotations.
 * All the beans are created from here.
 * @author canadensys
 *
 */
@Configuration
@ComponentScan(basePackages ="net.canadensys.processing",
	excludeFilters = { @Filter(type = FilterType.CUSTOM, value = { ExcludeTestClassesTypeFilter.class }),
		@Filter(type = FilterType.ASSIGNABLE_TYPE, value = { ProcessingConfig.class })})
@EnableTransactionManagement
public class ProcessingNodeConfig {
	
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
    
    @Value("${jms.broker_url}")
    private String jmsBrokerUrl;
    
    @Bean(name="datasource")
    public DataSource dataSource() {    		
    	ComboPooledDataSource ds = new ComboPooledDataSource();
    	try {
			ds.setDriverClass(dbDriverClassName);
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
    	ds.setJdbcUrl(dbUrl);
    	ds.setUser(username);
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
		hibernateProperties.setProperty("hibernate.connection.autocommit","false");
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
    			ImportLogModel.class, ResourceModel.class});

		Properties hibernateProperties = new Properties();
		hibernateProperties.setProperty("hibernate.dialect", hibernateDialect);
		hibernateProperties.setProperty("hibernate.show_sql", hibernateShowSql);
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
		return null;
	}
	
    //---JOB---
	@Bean
	public ImportDwcaJob importDwcaJob(){
		return new ImportDwcaJob();
	}
	@Bean
	public MoveToPublicSchemaJob moveToPublicSchemaJob(){
		return null;
	}
	@Bean
	public ComputeUniqueValueJob computeUniqueValueJob(){
		return null;
	}
	@Bean
	public UpdateResourceContactJob updateResourceContactJob(){
		return null;
	}
	@Bean
	public ComputeStatisticsJob computeStatisticsJob(){
		return null;
	}
	@Bean
	public FindUsedDwcaTermJob findUsedDwcaTermJob(){
		return null;
	}
	
	//---STEP---
	@Bean(name="streamEmlContentStep")
	public ProcessingStepIF streamEmlContentStep(){
		return null;
	}
	@Bean(name="streamDwcaContentStep")
	public ProcessingStepIF streamDwcaContentStep(){
		return null;
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
		return null;
	}
	
	@Bean(name="processOccurrenceStatisticsStep")
	public ProcessingStepIF processOccurrenceStatisticsStep(){
		return new ProcessOccurrenceStatisticsStep();
	}
	
	@Bean(name="streamOccurrenceForStatsStep")
	public ProcessingStepIF streamOccurrenceForStatsStep(){
		return null;
	}
	
	//---Unused TASK in processing node---
	@Bean
	public ItemTaskIF prepareDwcaTask(){
		return null;
	}
	@Bean
	public ItemTaskIF cleanBufferTableTask(){
		return null;
	}
	@Bean
	public ItemTaskIF computeGISDataTask(){
		return null;
	}
	@Bean
	public ItemTaskIF checkProcessingCompletenessTask(){
		return null;
	}
	@Bean
	public ItemTaskIF getResourceInfoTask(){
		return null;
	}
	@Bean
	public ItemTaskIF findUsedDwcaTermTask(){
		return null;
	}
	
	//---PROCESSOR wiring---
	@Bean(name="lineProcessor")
	public ItemProcessorIF<OccurrenceRawModel, OccurrenceRawModel> lineProcessor(){
		return new DwcaLineProcessor();
	}
	
	@Bean(name="occurrenceProcessor")
	public ItemProcessorIF<OccurrenceRawModel, OccurrenceModel> occurrenceProcessor(){
		return new OccurrenceProcessor();
	}
	
	@Bean(name="resourceContactProcessor")
	public ItemProcessorIF<Eml, ResourceContactModel> resourceContactProcessor(){
		return new ResourceContactProcessor();
	}
	
	@Bean(name="occurrenceQualityProcessor")
	public ItemProcessorIF<OccurrenceRawModel,OccurrenceQualityReportElement> occurrenceQualityProcessor(){
		return new OccurrenceQualityProcessor();
	}
	
	//---READER wiring---
	@Bean
	public ItemReaderIF<OccurrenceRawModel> dwcaItemReader(){
		return new DwcaItemReader();
	}
	@Bean
	public ItemReaderIF<Eml> dwcaEmlReader(){
		return null;
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
	 * node should not use this
	 * @return
	 */
	@Bean
	public JMSWriter jmsWriter(){
		return null;
	}
	
	@Bean(name="jmsConsumer")
	public JMSConsumer jmsConsumer(){
		return new JMSConsumer(jmsBrokerUrl);
	}
	
}
