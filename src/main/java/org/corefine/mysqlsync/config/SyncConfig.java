package org.corefine.mysqlsync.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SyncConfig {
	@Value("${syncConfigPath}")
	private String syncConfigPath;

	
	public List<DatabaseSyncConfig> getDbs() {
		JSONArray jArray;
		try {
			jArray = new JSONArray(new JSONTokener(new FileInputStream(syncConfigPath)));
		} catch (JSONException | FileNotFoundException e) {
			throw new RuntimeException("读取配置异常", e);
		}
		int size = jArray.length();
		List<DatabaseSyncConfig> dbs = new ArrayList<>(size);
		
		for (int i = 0; i < size; i++) {
			JSONObject jDb = jArray.getJSONObject(i);
			dbs.add(new DatabaseSyncConfig(jDb));
		}
		return dbs;
	}
	
	
	public static class DatabaseSyncConfig{
		private String srcDBName;
		private String srcTableName;
		private String descDBName;
		private String descTableName;
		private boolean update;
		
		public DatabaseSyncConfig(){
			
		}
		
		public DatabaseSyncConfig(JSONObject json){
			String src = json.getString("src");
			String[] srcs = src.split("\\.");
			this.srcDBName = srcs[0];
			this.srcTableName = srcs[1];
			String desc = json.getString("desc");
			String[] descs = desc.split("\\.");
			this.descDBName = descs[0];
			this.descTableName = descs[1];
			this.update = json.getBoolean("update");
		}
		@Override
		public String toString() {
			return "DatabaseConfig [srcDBTableName=" + srcDBName+"." + srcTableName 
					+" TO descDBTableName=" + descDBName+"." + descTableName +"]";
		}

		public String getSrcDBName() {
			return srcDBName;
		}

		public String getSrcTableName() {
			return srcTableName;
		}

		public String getDescDBName() {
			return descDBName;
		}

		public String getDescTableName() {
			return descTableName;
		}

		public boolean isUpdate() {
			return update;
		}
	}
}
