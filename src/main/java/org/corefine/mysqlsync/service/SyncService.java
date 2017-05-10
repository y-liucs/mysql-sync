package org.corefine.mysqlsync.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.corefine.mysqlsync.config.ColumnsConfig;
import org.corefine.mysqlsync.config.DbConfig;
import org.corefine.mysqlsync.config.DescConfig;
import org.corefine.mysqlsync.config.SrcConfig;
import org.corefine.mysqlsync.config.SyncConfig;
import org.corefine.mysqlsync.config.SyncConfig.DatabaseConfig;
import org.corefine.mysqlsync.config.SyncConfig.TableConfig;
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
	@Value("${oneQueryRows}")
	private Integer oneQueryRows;
	@Value("${oneInsertRows}")
	private Integer oneInsertRows;

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
		try {
			//2.初始化对比表
			initSyncTable(syncConnection.desc);
			//3.开始同步
			for (TableConfig table : db.getTables()) {
				logger.info("开始同步" + table.getTableName() + "表...");
				syncTable(syncConnection, table);
				logger.info("完成同步" + table.getTableName() + "表!");
			}
		} catch (Exception e) {
			logger.error("数据同步异常:" + db.getDbName(), e);
		} finally {
			//4.关闭连接
			try {
				syncConnection.src.close();
			} catch (SQLException e) {}
			try {
				syncConnection.desc.close();
			} catch (SQLException e) {}
		}
	}

	private void syncTable(SyncConnection syncConnection, TableConfig table) {
		//1.同步新增
		syncInsert(syncConnection, table.getTableName());
		//2.同步更新
		if (table.isUpdate())
			syncUpdate(syncConnection, table.getTableName());
	}

	private void syncUpdate(SyncConnection syncConnection, String tableName) {
		//TODO
		System.err.println("不支持此操作");
	}

	private void syncInsert(SyncConnection syncConnection, String tableName) {
		Object maxId = null;
		while (true) {
			//1.查询最大ID
			if (maxId == null) {
				maxId = querySimple(syncConnection.desc, "select `batch` from _sync_data where `key` = ?", tableName);
				if (maxId == null) {
					execute(syncConnection.desc, "insert into _sync_data(`key`, `batch`, `md5`) values(?, '0', '')", tableName);
				}
			}
			//2.查询数据
			String sql = "select * from " + tableName;
			if (maxId != null)
				sql += " where " + columnsConfig.getId() + " > ?";
			sql += " order by " + columnsConfig.getId() + " asc limit " + oneQueryRows;
			List<Map<String, Object>> dataList;
			if (maxId == null)
				dataList = query(syncConnection.src, sql);
			else
				dataList = query(syncConnection.src, sql, maxId);
				
			if (dataList.isEmpty())
				return;
			Map<String, Object> firstMap = dataList.get(0);
			//3.写入数据
			StringBuilder sb = new StringBuilder();
			sb.append("insert into ").append(tableName).append("(");
			for (String key : firstMap.keySet()) {
				sb.append("`").append(key).append("`").append(",");
			}
			sb.delete(sb.length() - 1, sb.length());
			sb.append(") values ");
			int index = 0;
			Object[] datas = new Object[dataList.size() * firstMap.size()];
			for (Map<String, Object> data : dataList) {
				sb.append("(");
				for (Object value : data.values()) {
					sb.append("?,");
					datas[index++] = value;
				}
				sb.delete(sb.length() - 1, sb.length());
				sb.append("),");
			}
			sb.delete(sb.length() - 1, sb.length());
			execute(syncConnection.desc, sb.toString(), datas);
			maxId = dataList.get(dataList.size() - 1).get(columnsConfig.getId());
			//4.更新maxId
			execute(syncConnection.desc, "update _sync_data set batch  = ? where `key` = ?", maxId, tableName);
			logger.debug(tableName + "新增" + dataList.size() + "条记录，当前主建：" + maxId);
		}
	}

	@SuppressWarnings("unchecked")
	private <T> T querySimple(Connection connection, String sql, Object...params) {
		Map<String, Object> map = queryOne(connection, sql, params);
		if (map == null)
			return null;
		else if (map.size() == 1)
			return (T) map.entrySet().iterator().next().getValue();
		else
			throw new RuntimeException("数据不唯一");
	}

	private Map<String, Object> queryOne(Connection connection, String sql, Object...params) {
		List<Map<String, Object>> list = query(connection, sql, params);
		if (list.isEmpty())
			return null;
		else if (list.size() == 1)
			return list.get(0);
		else
			throw new RuntimeException("数据不唯一");
	}

	private List<Map<String, Object>> query(Connection connection, String sql, Object...params) {
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(sql);
			for (int i = 0; i < params.length; i++)
				ps.setObject(i + 1, params[i]);
			ResultSet rs = ps.executeQuery();
			List<Map<String, Object>> resultList = new ArrayList<>();
			ResultSetMetaData rsd = rs.getMetaData();
			int count = rsd.getColumnCount();
			String[] names = new String[count];
			for (int i = 0; i < count; i++)
				names[i] = rsd.getColumnLabel(i + 1);
			Map<String, Object> data;
			while (rs.next()) {
				data = new LinkedHashMap<>(count);
				for (int i = 0; i < count; i++)
					data.put(names[i], rs.getObject(i + 1));
				resultList.add(data);
			}
			return resultList;
		} catch (SQLException e) {
			throw new RuntimeException("执行查询错误:" + sql, e);
		} finally {
			if (ps != null)
				try {
					ps.close();
				} catch (SQLException e) {}
		}
	}

	private boolean execute(Connection connection, String sql, Object...params) {
		PreparedStatement pt = null;
		try {
			pt = connection.prepareStatement(sql);
			for (int i = 0; i < params.length; i++)
				pt.setObject(i + 1, params[i]);
			return pt.execute();
		} catch (SQLException e) {
			throw new RuntimeException("执行sql失败:" + sql, e);
		} finally {
			try {
				pt.close();
			} catch (SQLException e) {}
		}
	}

	private void initSyncTable(Connection conn) {
		String checkTableScript = "SHOW TABLES LIKE '_sync_data'";
		String createTableScript = "CREATE TABLE `_sync_data`(`key` varchar(255) NOT NULL, "
				+ "`batch` varchar(255) DEFAULT NULL, `md5` varchar(255) NOT NULL, "
				+ "PRIMARY KEY (`key`),  UNIQUE KEY `key` (`key`,`batch`)) "
				+ "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
		Statement st = null;
		try {
			st = conn.createStatement();
			ResultSet rs = st.executeQuery(checkTableScript);
			if (!rs.next())
				st.execute(createTableScript);
		} catch (SQLException e) {
			throw new RuntimeException("初始化同步表失败", e);
		} finally {
			if (st != null)
				try {
					st.close();
				} catch (SQLException e) {}
		}
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
			try {
				sync.src.close();
			} catch (SQLException e1) {}
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
