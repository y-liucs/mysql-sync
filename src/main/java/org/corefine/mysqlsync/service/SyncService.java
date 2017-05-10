package org.corefine.mysqlsync.service;

import java.sql.Connection;
import java.util.List;

import org.corefine.mysqlsync.config.ColumnsConfig;
import org.corefine.mysqlsync.config.DescConfig;
import org.corefine.mysqlsync.config.SrcConfig;
import org.corefine.mysqlsync.config.SyncConfig;
import org.corefine.mysqlsync.config.SyncConfig.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class SyncService {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	@Autowired
	private SyncConfig syncConfig;
	@Autowired
	private SrcConfig srcConfig;
	@Autowired
	private DescConfig descConfig;
	@Autowired
	private ColumnsConfig columnsConfig;
	@Value("${oneCheckRows}")
	private Integer oneCheckRows;

	public void sync() {
		logger.info("开始数据同步...");
		List<DatabaseConfig> dbs = syncConfig.getDbs();
		for (DatabaseConfig db : dbs) {
			logger.info("开始同步" + db.getDbName() + "数据库...");
			syncDatabase(db);
			logger.info("完成同步" + db.getDbName() + "数据库!");
		}
		logger.info("完成数据同步!");
	}
	
	private void syncDatabase(DatabaseConfig db) {
		//1.创建连接
		//2.初始化对比表
		//3.开始同步
	}
	
	
	private class SyncConnection {
		Connection src;
		Connection desc;
	}
}
