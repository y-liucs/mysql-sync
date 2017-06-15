package org.corefine.mysqlsync.service;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
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
public class UpdateService {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	@Autowired
	private DBService dbService;
	@Autowired
	private Md5Service md5Service;
	@Autowired
	private ColumnsConfig columnsConfig;
	@Value("${oneQueryRows}")
	private Integer oneQueryRows;

	public void sync(SyncConnection syncConnection, TableConfig table) {
		String tableName = table.getTableName();
		int checkRows = oneQueryRows * 16;
		String dataCheckSql = "select " + columnsConfig.getId() + " as 'ID', " + columnsConfig.getCheck() + " as 'CHECK' from "
				+ tableName + " where " + columnsConfig.getId() + " > ? and " + columnsConfig.getId()
				+ " <= ? order by " + columnsConfig.getId() + " asc";
		String dataMd5Sql = "select `md5` from _sync_data where `key` = ?";
		String insertMd5Sql = "insert into _sync_data(`md5`, `key`) values(?, ? )";
		String updateMd5Sql = "update _sync_data set `md5` = ? where `key` = ?";
		String maxIdSql = "select id from " + tableName + " order by id desc limit 1";
		Long maxId = dbService.querySimple(syncConnection.src, maxIdSql);
		long startId = 0, endId = checkRows + startId;
		int emptyCount = 0;
		while (true) {
			logger.debug(tableName + "验证记录是否被修改，当前ID：" + endId);
			//1.对比数据
			List<Map<String, Object>> dataList = dbService.query(syncConnection.src, dataCheckSql, startId, endId);
			if (dataList.isEmpty() && emptyCount++ > 10) 
				break;
			else {
				emptyCount = 0;
				String key = '$' + tableName + '-' + checkRows + '-' + startId;
				String srcMd5 = md5Service.md5(dataList);
				String descMd5 = dbService.querySimple(syncConnection.desc, dataMd5Sql, key);
				if (!srcMd5.equals(descMd5)) {
					//2.执行更新数据，缩小更新范围
					int step = 1024, index = 0;
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
						String stepSrcMd5 = md5Service.md5(srcList);
						String stepDescMd5 = dbService.querySimple(syncConnection.desc, dataMd5Sql, stepKey);
						if (!stepSrcMd5.equals(stepDescMd5)) {
							int updateCount = 0;
							//2.2.转换目录库的数据结构
							List<Map<String, Object>> descList = dbService.query(syncConnection.desc, dataCheckSql, stepStartId, stepEndId);
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
									syncUpdateInsertOneRow(syncConnection, table, (Long) id);
								else
									syncUpdateUpdateOneRow(syncConnection, table, (Long) id);
							}
							//2.5.删除数据
							updateCount += descMap.size();
							if (descMap.size() > 0)
								syncUpdateDeleteRows(syncConnection.desc, tableName, descMap);
							//2.6.写入md5
							if (stepDescMd5 == null)
								dbService.execute(syncConnection.desc, insertMd5Sql, stepSrcMd5, stepKey);
							else
								dbService.execute(syncConnection.desc, updateMd5Sql, stepSrcMd5, stepKey);
							logger.debug(tableName + "更新" + updateCount + "条记录，当前ID：" + stepStartId);
						}
						step = endId - stepStartId >= step ? step : (int) (endId - stepStartId);
						stepStartId += step;
						index = endIndex;
					}
				}
				//3.写入新的MD5
				if (descMd5 == null)
					dbService.execute(syncConnection.desc, insertMd5Sql, srcMd5, key);
				else
					dbService.execute(syncConnection.desc, updateMd5Sql, srcMd5, key);
				//4.结束前处理
				if (endId >= maxId)
					return;
			}
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
		dbService.execute(connection, sb.toString(), ids);
	}

	private void syncUpdateInsertOneRow(SyncConnection syncConnection, TableConfig table, Long id) {
		String dataSql = "select * from " + table.getTableName() + " where `" + columnsConfig.getId() + "` = ?";
		Map<String, Object> data = dbService.queryOne(syncConnection.src, dataSql, id);
		if (data == null)
			return;
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(table.getTableName()).append("(");
		int index = 0;
		Object[] datas = new Object[data.size()];
		StringBuilder valuesSb = new StringBuilder();
		keyFlag: for (Entry<String, Object> entry : data.entrySet()) {
			for (String ignore : table.getIgnores()) {
				if (ignore.equals(entry.getKey()))
					continue keyFlag;
			}
			sb.append("`").append(entry.getKey()).append("`").append(",");
			valuesSb.append("?,");
			datas[index++] = entry.getValue();
		}
		sb.delete(sb.length() - 1, sb.length());
		sb.append(") values (");
		sb.append(valuesSb, 0, valuesSb.length() - 1);
		sb.append(")");
		dbService.execute(syncConnection.desc, sb.toString(), datas);
	}

	private void syncUpdateUpdateOneRow(SyncConnection syncConnection, TableConfig table, Long id) {
		String tableName = table.getTableName();
		String dataSql = "select * from " + tableName + " where `" + columnsConfig.getId() + "` = ?";
		Map<String, Object> data = dbService.queryOne(syncConnection.src, dataSql, id);
		if (data == null)
			return;
		StringBuilder sb = new StringBuilder();
		sb.append("update ").append(tableName).append(" set ");
		Object[] datas = new Object[data.size()];
		int index = 0;
		keyFlag: for (Entry<String, Object> entry : data.entrySet()) {
			for (String ignore : table.getIgnores()) {
				if (ignore.equals(entry.getKey()))
					continue keyFlag;
			}
			sb.append("`").append(entry.getKey()).append("` = ").append(" ?,");
			datas[index++] = entry.getValue();
		}
		sb.delete(sb.length() - 1, sb.length());
		sb.append(" where `");
		sb.append(columnsConfig.getId());
		sb.append("` = ");
		sb.append(id);
		dbService.execute(syncConnection.desc, sb.toString(), datas);
	}
}
