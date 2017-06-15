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

import org.corefine.mysqlsync.config.DbConfig;
import org.corefine.mysqlsync.config.DescConfig;
import org.corefine.mysqlsync.config.SrcConfig;
import org.corefine.mysqlsync.config.SyncConfig.DatabaseConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DBService {
	@Autowired
	private SrcConfig srcConfig;
	@Autowired
	private DescConfig descConfig;

	@SuppressWarnings("unchecked")
	public <T> T querySimple(Connection connection, String sql, Object... params) {
		Map<String, Object> map = queryOne(connection, sql, params);
		if (map == null)
			return null;
		else if (map.size() == 1)
			return (T) map.entrySet().iterator().next().getValue();
		else
			throw new RuntimeException("数据不唯一");
	}

	public Map<String, Object> queryOne(Connection connection, String sql, Object... params) {
		List<Map<String, Object>> list = query(connection, sql, params);
		if (list.isEmpty())
			return null;
		else if (list.size() == 1)
			return list.get(0);
		else
			throw new RuntimeException("数据不唯一");
	}

	public List<Map<String, Object>> query(Connection connection, String sql, Object... params) {
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
				} catch (SQLException e) {
				}
		}
	}
	public boolean execute(Connection connection, String sql, int length, Object... params) {
		PreparedStatement pt = null;
		try {
			pt = connection.prepareStatement(sql);
			for (int i = 0; i < length; i++)
				pt.setObject(i + 1, params[i]);
			return pt.execute();
		} catch (SQLException e) {
			throw new RuntimeException("执行sql失败:" + sql, e);
		} finally {
			try {
				pt.close();
			} catch (SQLException e) {
			}
		}
	}

	public boolean execute(Connection connection, String sql, Object... params) {
		return execute(connection, sql, params.length, params);
	}

	public void initSyncTable(Connection conn) {
		String checkTableScript = "SHOW TABLES LIKE '_sync_data'";
		String createTableScript = "CREATE TABLE `_sync_data`(`key` varchar(255) NOT NULL, "
				+ "`md5` varchar(255) NOT NULL, " + "PRIMARY KEY (`key`)) " + "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
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
				} catch (SQLException e) {
				}
		}
	}

	public SyncConnection createConnection(DatabaseConfig db) {
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
			} catch (SQLException e1) {
			}
			throw new RuntimeException("无法连接到目标数据库:" + db + "," + descConfig, e);
		}
		return sync;
	}

	public Connection createConnection(DbConfig config, String dbName) throws SQLException {
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
	
	public static class SyncConnection {
		Connection src;
		Connection desc;
	}
}
