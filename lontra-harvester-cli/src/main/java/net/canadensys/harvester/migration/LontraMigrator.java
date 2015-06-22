package net.canadensys.harvester.migration;

import java.sql.SQLException;
import java.util.List;

import liquibase.changelog.ChangeSet;
import liquibase.exception.LiquibaseException;
import net.canadensys.dataportal.occurrence.migration.LiquibaseHelper;
import net.canadensys.harvester.config.CLIMinimalConfig;

import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.Lists;

/**
 * Class responsible for migration of Lontra underlying database.
 * 
 * @author cgendreau
 *
 */
public class LontraMigrator {

	@Autowired
	private CLIMinimalConfig config;

	public void migrate() {
		try {
			LiquibaseHelper.migrate(config.dataSource().getConnection());
		}
		catch (LiquibaseException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Get the list of change sets that as not been applied to the database.
	 * 
	 * @return
	 */
	public List<String> getChangeSetList() {
		List<String> changeSetList = Lists.newArrayList();
		try {
			List<ChangeSet> changeSets = LiquibaseHelper.listUnrunPublicChangeSets(config.dataSource().getConnection());
			for (ChangeSet cs : changeSets) {
				changeSetList.add("[" + cs.getId() + "]=>" + cs.getComments());
			}
		}
		catch (LiquibaseException | SQLException e) {
			e.printStackTrace();
		}
		return changeSetList;
	}

}
