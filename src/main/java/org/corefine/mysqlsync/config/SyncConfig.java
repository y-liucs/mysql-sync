package org.corefine.mysqlsync.config;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

@Component
public class SyncConfig implements InitializingBean {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private List<DatabaseConfig> dbs;

	public List<DatabaseConfig> getDbs() {
		return dbs;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		ResourceLoader resourceLoader = new DefaultResourceLoader();
		Resource resource = resourceLoader.getResource("sync.js");
		JSONArray jArray = new JSONArray(new JSONTokener(resource.getInputStream()));
		dbs = new ArrayList<>(jArray.length());
		for (int i = 0; i < jArray.length(); i++) {
			JSONObject jDb = jArray.getJSONObject(i);
			DatabaseConfig db = new DatabaseConfig();
			dbs.add(db);
			db.setDbName(jDb.getString("dbName"));
			JSONArray jTables = jDb.getJSONArray("tables");
			List<TableConfig> tables = new ArrayList<>(jTables.length());
			db.setTables(tables);
			for (int j = 0; j < jTables.length(); j++) {
				JSONObject jTable = jTables.getJSONObject(j);
				TableConfig table = new TableConfig();
				tables.add(table);
				table.setTableName(jTable.getString("tableName"));
				table.setUpdate(jTable.getBoolean("update"));
				List<String> ignores;
				if (jTable.has("ignores")) {
					JSONArray jIgnores = jTable.getJSONArray("ignores");
					ignores = new ArrayList<>(jIgnores.length());
					for (int k = 0; k < jIgnores.length(); k++)
						ignores.add(jIgnores.getString(k));
				} else
					ignores = new ArrayList<>();
				table.setIgnores(ignores);
			}
		}
		logger.info("获取到同步配置：" + dbs);
	}
	
	public static class DatabaseConfig {
		private String dbName;
		private List<TableConfig> tables;
		public String getDbName() {
			return dbName;
		}
		public void setDbName(String dbName) {
			this.dbName = dbName;
		}
		public List<TableConfig> getTables() {
			return tables;
		}
		public void setTables(List<TableConfig> tables) {
			this.tables = tables;
		}
		@Override
		public String toString() {
			return "DatabaseConfig [dbName=" + dbName + ", tables=" + tables + "]";
		}
	}
	
	public static class TableConfig {
		private String tableName;
		private boolean update;
		private List<String> ignores;
		public String getTableName() {
			return tableName;
		}
		public void setTableName(String tableName) {
			this.tableName = tableName;
		}
		public boolean isUpdate() {
			return update;
		}
		public void setUpdate(boolean update) {
			this.update = update;
		}
		public List<String> getIgnores() {
			return ignores;
		}
		public void setIgnores(List<String> ignores) {
			this.ignores = ignores;
		}
		@Override
		public String toString() {
			return "TableConfig [tableName=" + tableName + ", update=" + update + ", ignores=" + ignores + "]";
		}
	}
}
