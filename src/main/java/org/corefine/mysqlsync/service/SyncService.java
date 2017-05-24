package org.corefine.mysqlsync.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
	@Value("${oneQueryRows}")
	private Integer oneQueryRows;

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
		//1.同步结构
		syncCreate(syncConnection, table.getTableName());
		//2.同步新增
		syncInsert(syncConnection, table.getTableName());
		//3.同步更新
		if (table.isUpdate()) {
			logger.debug(table.getTableName() + "开始验证是否有更新...");
			syncUpdate(syncConnection, table.getTableName());
			logger.debug(table.getTableName() + "完成验证是否有更新!");
		}
	}

	private void syncCreate(SyncConnection syncConnection, String tableName) {
		String sql = "show create table " + tableName;
		String srcData = queryOne(syncConnection.src, sql).get("Create Table").toString();
		String srcMd5 = md5(srcData);
		String descData = "";
		try {
			queryOne(syncConnection.desc, sql).get("Create Table").toString();
		} catch (RuntimeException e) {
			//table is not exist
		}
		String descMd5 = md5(descData);
		if (!descMd5.equals(srcMd5)) {
			execute(syncConnection.desc, "drop table " + tableName);
			execute(syncConnection.desc, srcData);
			logger.info(tableName + "表结构错误，已经重建");
		}
	}

	private void syncUpdate(SyncConnection syncConnection, String tableName) {
		int checkRows = oneQueryRows * 8;
		String dataCheckSql = "select " + columnsConfig.getId() + " as 'ID', " + columnsConfig.getCheck() + " as 'CHECK' from "
				+ tableName + " where " + columnsConfig.getId() + " > ? and " + columnsConfig.getId()
				+ " <= ? order by " + columnsConfig.getId() + " asc";
		String dataMd5Sql = "select `md5` from _sync_data where `key` = ?";
		String insertMd5Sql = "insert into _sync_data(`md5`, `key`) values(?, ? )";
		String updateMd5Sql = "update _sync_data set `md5` = ? where `key` = ?";
		String maxIdSql = "select id from " + tableName + " order by id desc limit 1";
		Long maxId = querySimple(syncConnection.src, maxIdSql);
		long startId = 0, endId = checkRows + startId;
		while (true) {
			logger.debug(tableName + "验证记录是否被修改，当前ID：" + endId);
			//1.对比数据
			List<Map<String, Object>> dataList = query(syncConnection.src, dataCheckSql, startId, endId);
			if (dataList.isEmpty())
				continue;
			String key = '$' + tableName + '-' + checkRows + '-' + startId;
			String srcMd5 = md5(dataList);
			String descMd5 = querySimple(syncConnection.desc, dataMd5Sql, key);
			if (!srcMd5.equals(descMd5)) {
				//2.执行更新数据，缩小更新范围
				int step = oneQueryRows / 8, index = 0;
				for (long stepStartId = startId; stepStartId < endId;) {
					long stepEndId = stepStartId + step;
					int endIndex = index + step > dataList.size() ? dataList.size() : index + step;
					List<Map<String, Object>> baseSrcList = dataList.subList(index, endIndex); 
					List<Map<String, Object>> srcList = new ArrayList<>(baseSrcList.size());
					srcList.addAll(baseSrcList);
					for (int i = srcList.size() - 1; i >= 0; i--) {
						if (((Long)srcList.get(i).get("ID")) > stepEndId)
							srcList.remove(i);
						else
							break;
					}
					String stepKey = '#' + tableName + '-' + step + '-' + stepStartId;
					//2.1.md5 src
					String stepSrcMd5 = md5(srcList);
					String stepDescMd5 = querySimple(syncConnection.desc, dataMd5Sql, stepKey);
					if (!stepSrcMd5.equals(stepDescMd5)) {
						int updateCount = 0;
						//2.2.转换目录库的数据结构
						List<Map<String, Object>> descList = query(syncConnection.src, dataCheckSql, stepStartId, stepEndId);
						Map<Object, Object> descMap = new HashMap<>(descList.size());
						for (Map<String, Object> data : descList)
							descMap.put(data.get("ID"), data.get("CHECK"));
						//2.3.对比数据
						for (Map<String, Object> data : srcList) {
							Object id = data.get("ID");
							Object srcCheck = data.get("CHECK");
							Object descCheck = descMap.remove(id);
							if (srcCheck.equals(descCheck))
								continue;
							updateCount++;
							//2.4.更新或新增数据
							if (descCheck == null)
								syncUpdateInsertOneRow(syncConnection, tableName, (Long) id);
							else
								syncUpdateUpdateOneRow(syncConnection, tableName, (Long) id);
						}
						//2.5.删除数据
						updateCount += descMap.size();
						if (descMap.size() > 0)
							syncUpdateDeleteRows(syncConnection.desc, tableName, descMap);
						//2.6.写入md5
						if (stepDescMd5 == null)
							execute(syncConnection.desc, insertMd5Sql, stepSrcMd5, stepKey);
						else
							execute(syncConnection.desc, updateMd5Sql, stepSrcMd5, stepKey);
						logger.debug(tableName + "更新" + updateCount + "条记录，当前ID：" + stepStartId);
					}
					step = endId - stepStartId >= step ? step : (int) (endId - stepStartId);
					stepStartId += step;
					index = endIndex;
				}
			}
			//3.写入新的MD5
			if (descMd5 == null)
				execute(syncConnection.desc, insertMd5Sql, srcMd5, key);
			else
				execute(syncConnection.desc, updateMd5Sql, srcMd5, key);
			//4.结束前处理
			if (endId >= maxId)
				return;
			startId = endId;
			endId += checkRows;
		}
	}

	private void syncUpdateDeleteRows(Connection connection, String tableName, Map<Object, Object> descMap) {
		StringBuilder sb = new StringBuilder();
		sb.append("delete from ");
		sb.append(tableName);
		sb.append(" where `");
		sb.append(columnsConfig.getId());
		sb.append("` in (");
		Object[] ids = new Object[descMap.size()];
		int index = 0;
		for (Object id : descMap.keySet()) {
			ids[index++] = id;
			sb.append("?,");
		}
		sb.delete(sb.length() - 1, sb.length());
		sb.append(")");
		execute(connection, sb.toString(), ids);
	}

	private void syncUpdateInsertOneRow(SyncConnection syncConnection, String tableName, Long id) {
		String dataSql = "select * from " + tableName + " where `" + columnsConfig.getId() + "` = ?";
		Map<String, Object> data = queryOne(syncConnection.src, dataSql, id);
		if (data == null)
			return;
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(tableName).append("(");
		int index = 0;
		Object[] datas = new Object[data.size()];
		StringBuilder valuesSb = new StringBuilder();
		for (Entry<String, Object> entry : data.entrySet()) {
			sb.append("`").append(entry.getKey()).append("`").append(",");
			valuesSb.append("?,");
			datas[index++] = entry.getValue();
		}
		sb.delete(sb.length() - 1, sb.length());
		sb.append(") values (");
		sb.append(valuesSb, 0, valuesSb.length() - 1);
		sb.append(")");
		execute(syncConnection.desc, sb.toString(), datas);
	}

	private void syncUpdateUpdateOneRow(SyncConnection syncConnection, String tableName, Long id) {
		String dataSql = "select * from " + tableName + " where `" + columnsConfig.getId() + "` = ?";
		Map<String, Object> data = queryOne(syncConnection.src, dataSql, id);
		if (data == null)
			return;
		StringBuilder sb = new StringBuilder();
		sb.append("update ").append(tableName).append(" set ");
		Object[] datas = new Object[data.size()];
		int index = 0;
		for (Entry<String, Object> entry : data.entrySet()) {
			sb.append("`").append(entry.getKey()).append("` = ").append(" ?,");
			datas[index++] = entry.getValue();
		}
		sb.delete(sb.length() - 1, sb.length());
		sb.append(" where `");
		sb.append(columnsConfig.getId());
		sb.append("` = ");
		sb.append(id);
		execute(syncConnection.desc, sb.toString(), datas);
	}

	private void syncInsert(SyncConnection syncConnection, String tableName) {
		Long maxId = querySimple(syncConnection.desc, "select " + columnsConfig.getId()
		+ " from " + tableName + " order by " + columnsConfig.getId() + " desc limit 1");
		if (maxId == null)
			maxId = 0l;
		while (true) {
			//1.查询数据
			String sql = "select * from " + tableName;
			sql += " where " + columnsConfig.getId() + " > ?";
			sql += " order by " + columnsConfig.getId() + " asc limit " + oneQueryRows;
			List<Map<String, Object>> dataList = query(syncConnection.src, sql, maxId);
			if (dataList.isEmpty())
				return;
			Map<String, Object> firstMap = dataList.get(0);
			//2.写入数据
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
			maxId = (Long) dataList.get(dataList.size() - 1).get(columnsConfig.getId());
			logger.debug(tableName + "新增" + dataList.size() + "条记录，当前ID：" + maxId);
			if (dataList.size() < oneQueryRows)
				return;
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
				+ "`md5` varchar(255) NOT NULL, "
				+ "PRIMARY KEY (`key`)) "
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

	private String md5(List<Map<String, Object>> dataList) {
		StringBuilder sb = new StringBuilder();
		for (Map<String, Object> data : dataList)
			sb.append(data.get(data.get("ID"))).append(';').append(data.get("CHECK")).append('.');
		return md5(sb.toString());
	}

	private String md5(String data) {
		return MD5Util.getMD5(data);
	}

	private class SyncConnection {
		Connection src;
		Connection desc;
	}
}
