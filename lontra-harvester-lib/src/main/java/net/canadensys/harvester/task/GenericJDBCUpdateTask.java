package net.canadensys.harvester.task;

import java.util.List;
import java.util.Map;

import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.exception.TaskExecutionException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Generic JDBC task that can run multiple SQL updates using NamedParameter extracted from
 * shartedParameters.
 * Transaction management is the responsibility of the caller.
 * 
 * @author cgendreau
 * 
 */
public class GenericJDBCUpdateTask implements ItemTaskIF {

	@Autowired
	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	private String title;
	private List<String> sqlStatements;

	@Override
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public void setSqlStatements(List<String> sqlStatements) {
		this.sqlStatements = sqlStatements;
	}

	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters) throws TaskExecutionException {

		// expose all SharedParameters to namedParameterJdbcTemplate
		MapSqlParameterSource namedParameters = new MapSqlParameterSource();
		for (SharedParameterEnum currSharedParam : sharedParameters.keySet()) {
			namedParameters.addValue(currSharedParam.toString().toLowerCase(), sharedParameters.get(currSharedParam));
		}

		try {
			for (String currSQL : sqlStatements) {
				namedParameterJdbcTemplate.update(currSQL, namedParameters);
			}
		}
		catch (DataAccessException daEx) {
			throw new TaskExecutionException("Can't execute sqlStatement", daEx);
		}
	}

}
