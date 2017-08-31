package org.corefine.mysqlsync.service;

import java.sql.SQLException;
import java.util.List;

import org.corefine.mysqlsync.config.SyncConfig;
import org.corefine.mysqlsync.config.SyncConfig.DatabaseConfig;
import org.corefine.mysqlsync.config.SyncConfig.TableConfig;
import org.corefine.mysqlsync.service.DBService.SyncConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
		List<DatabaseConfig> dbs = syncConfig.getDbs();
		for (DatabaseConfig db : dbs) {
			logger.info("开始同步" + db.getDbName() + "数据库...");
			try {
				syncDatabase(db);
			} catch (Exception e) {
				logger.error("数据同步异常：" + db.getDbName(), e);
			}
			logger.info("完成同步" + db.getDbName() + "数据库!");
		}
		logger.info("完成数据同步!");
	}

	private void syncDatabase(DatabaseConfig db) {
		SyncConnection syncConnection = dbService.createConnection(db);
		try {
			dbService.initSyncTable(syncConnection.desc);
			for (TableConfig table : db.getTables()) {
				logger.info("开始同步" + table.getTableName() + "表...");
				try {
					syncTable(syncConnection, table);
				} catch (RuntimeException e) {
					logger.error("同步表失败" + table.getTableName(), e);
				}
				logger.info("完成同步" + table.getTableName() + "表!");
			}
		} catch (Exception e) {
			logger.error("数据同步异常:" + db.getDbName(), e);
		} finally {
			try {
				syncConnection.src.close();
			} catch (SQLException e) {}
			try {
				syncConnection.desc.close();
			} catch (SQLException e) {}
		}
	}

	private void syncTable(SyncConnection syncConnection, TableConfig table) {
		createService.sync(syncConnection, table);
		insertService.sync(syncConnection, table);
		if (table.isUpdate()) {
			logger.debug(table.getTableName() + "开始验证是否有更新...");
			updateService.sync(syncConnection, table);
			logger.debug(table.getTableName() + "完成验证是否有更新!");
		}
	}
}
