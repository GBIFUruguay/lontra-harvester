package net.canadensys.harvester.config;

/**
 * Configuration object holding database specific configuration.
 * 
 * @author cgendreau
 * 
 */
public class DatabaseConfig {

	private String selectColumnNamesSQL;

	/**
	 * Returns the SQL query to use to get the list of columns of a table inside a specific schema.
	 * Query parameter 0:schema name, 1:table name
	 * 
	 * @return
	 */
	public String getSelectColumnNamesSQL() {
		return selectColumnNamesSQL;
	}

	public void setSelectColumnNamesSQL(String selectColumnNamesSQL) {
		this.selectColumnNamesSQL = selectColumnNamesSQL;
	}

}
