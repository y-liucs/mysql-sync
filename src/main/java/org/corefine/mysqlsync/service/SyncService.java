package org.corefine.mysqlsync.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.corefine.mysqlsync.config.ColumnsConfig;
import org.corefine.mysqlsync.config.DbConfig;
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
		//1.创建连接
		SyncConnection syncConnection = createConnection(db);
		//2.初始化对比表

		//3.开始同步
	}

	private void initSyncTable(Connection conn) {
		
	}

	private SyncConnection createConnection(DatabaseConfig db) {
		SyncConnection sync = new SyncConnection();
		try {
			sync.src = createConnection(srcConfig, db.getDbName());
		} catch (SQLException e) {
			throw new RuntimeException("无法连接到源数据库:" + db + "," + srcConfig, e);
		}
		try {
			sync.desc = createConnection(descConfig, db.getDbName());
		} catch (SQLException e) {
			throw new RuntimeException("无法连接到目标数据库:" + db + "," + descConfig, e);
		}
		return sync;
	}

	private Connection createConnection(DbConfig config, String dbName) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("jdbc:mysql://");
		sb.append(config.getHost());
		sb.append(":");
		sb.append(config.getPort());
		sb.append("/");
		sb.append(dbName);
		sb.append("?useUnicode=true&characterEncoding=utf-8&useSSL=true");
		return DriverManager.getConnection(sb.toString(), config.getUsername(), config.getPassword());
	}

	private class SyncConnection {
		Connection src;
		Connection desc;
	}
}
