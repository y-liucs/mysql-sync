package org.corefine.mysqlsync.service;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.corefine.mysqlsync.config.ColumnsConfig;
import org.corefine.mysqlsync.config.SyncConfig.TableConfig;
import org.corefine.mysqlsync.service.DBService.SyncConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class InsertService {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	@Autowired
	private DBService dbService;
	@Autowired
	private ColumnsConfig columnsConfig;
	@Value("${oneQueryRows}")
	private Integer oneQueryRows;
	
	public void sync(SyncConnection syncConnection, TableConfig table) {
		String tableName = table.getTableName();
		Long maxId = dbService.querySimple(syncConnection.desc, "select " + columnsConfig.getId()
		+ " from " + tableName + " order by " + columnsConfig.getId() + " desc limit 1");
		if (maxId == null)
			maxId = 0l;
		while (true) {
			//1.查询数据
			String sql = "select * from " + tableName;
			sql += " where " + columnsConfig.getId() + " > ?";
			sql += " order by " + columnsConfig.getId() + " asc limit " + oneQueryRows;
			List<Map<String, Object>> dataList = dbService.query(syncConnection.src, sql, maxId);
			if (dataList.isEmpty())
				return;
			Map<String, Object> firstMap = dataList.get(0);
			//2.写入数据
			StringBuilder sb = new StringBuilder();
			sb.append("insert into ").append(tableName).append("(");
			int size = 0;
			keyFlag: for (String key : firstMap.keySet()) {
				for (String ignore : table.getIgnores()) {
					if (ignore.equals(key))
						continue keyFlag;
				}
				sb.append("`").append(key).append("`").append(",");
				size++;
			}
			sb.delete(sb.length() - 1, sb.length());
			sb.append(") values ");
			int index = 0;
			Object[] datas = new Object[dataList.size() * size];
			for (Map<String, Object> data : dataList) {
				sb.append("(");
				keyFlag: for (Entry<String, Object> entry : data.entrySet()) {
					for (String ignore : table.getIgnores()) {
						if (ignore.equals(entry.getKey()))
							continue keyFlag;
					}
					sb.append("?,");
					datas[index++] = entry.getValue();
				}
				sb.delete(sb.length() - 1, sb.length());
				sb.append("),");
			}
			sb.delete(sb.length() - 1, sb.length());
			dbService.execute(syncConnection.desc, sb.toString(), datas);
			maxId = (Long) dataList.get(dataList.size() - 1).get(columnsConfig.getId());
			logger.debug(tableName + "新增" + dataList.size() + "条记录，当前ID：" + maxId);
			if (dataList.size() < oneQueryRows)
				return;
		}
	}
}
