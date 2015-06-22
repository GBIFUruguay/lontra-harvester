package net.canadensys.harvester.config;

import javax.sql.DataSource;

import net.canadensys.harvester.main.MigrationMain;
import net.canadensys.harvester.migration.LontraMigrator;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Minimal CLI configuration is used to perform check and update on the harvester database.
 * 
 * @author cgendreau
 * 
 */
public class CLIMinimalConfig {

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

	@Bean
	public static PropertyPlaceholderConfigurer properties() {
		PropertyPlaceholderConfigurer ppc = new PropertyPlaceholderConfigurer();
		ppc.setLocation(new FileSystemResource("config/harvester-config.properties"));
		return ppc;
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

	@Bean
	public JdbcTemplate jdbcTemplate() {
		return new JdbcTemplate(dataSource());
	}

	@Bean
	public MigrationMain diagnosisMain() {
		return new MigrationMain();
	}

	@Bean
	public LontraMigrator lontraMigrator() {
		return new LontraMigrator();
	}

}
