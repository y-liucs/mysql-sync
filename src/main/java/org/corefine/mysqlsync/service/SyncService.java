package org.corefine.mysqlsync.service;

import org.corefine.mysqlsync.config.SyncConfig;
import org.corefine.mysqlsync.config.SyncConfig.DatabaseSyncConfig;
import org.corefine.mysqlsync.service.DBService.SyncConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

@Service
public class SyncService {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	@Autowired
	private InsertService insertService;
	@Autowired
	private UpdateService updateService;
	@Autowired
	private CreateService createService;
	@Autowired
	private SyncConfig syncConfig;
	@Autowired
	private DBService dbService;

	public void sync() {
		logger.info("开始数据同步...");
		List<DatabaseSyncConfig> dbs = syncConfig.getDbs();
		for (DatabaseSyncConfig db : dbs) {
			logger.info("开始同步" + db.toString() + "数据库...");
			try {
				syncDatabase(db);
			} catch (Exception e) {
				logger.error("数据同步异常：" +db.toString(), e);
			}
			logger.info("完成同步" + db.toString() + "数据库!");
		}
		logger.info("完成数据同步!");
	}
	
	
	
	private void syncDatabase(DatabaseSyncConfig db) {
		SyncConnection syncConnection = dbService.createConnection(db);
//		dbService.initSyncTable(syncConnection.desc);
		try {
			syncTable(syncConnection, db);
		} catch (Exception e) {
			logger.error("数据同步异常:" + db.toString(), e);
		} finally {
			try {
				syncConnection.src.close();
			} catch (SQLException e) {}
			try {
				syncConnection.desc.close();
			} catch (SQLException e) {}
		}
	}

	private void syncTable(SyncConnection syncConnection, DatabaseSyncConfig db) {
		createService.sync(syncConnection, db);
		insertService.sync(syncConnection, db);
		if (db.isUpdate()) {
			logger.debug(db.getSrcTableName() + "开始验证是否有更新...");
			updateService.sync(syncConnection, db);
			logger.debug(db.getDescTableName() + "完成验证是否有更新!");
		}
	}
}
