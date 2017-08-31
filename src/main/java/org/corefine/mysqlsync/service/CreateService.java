package org.corefine.mysqlsync.service;

import org.corefine.mysqlsync.config.SyncConfig.TableConfig;
import org.corefine.mysqlsync.service.DBService.SyncConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CreateService {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	@Autowired
	private DBService dbService;
	@Autowired
	private Md5Service md5Service;

	public void sync(SyncConnection syncConnection, TableConfig table) {
		String tableName = table.getTableName();
		String sql = "show create table " + tableName;
		String srcData = dbService.queryOne(syncConnection.src, sql).get("Create Table").toString();
		String srcMd5 = md5Service.md5(srcData);
		String descData = "";
		boolean isDelete;
		try {
			descData = dbService.queryOne(syncConnection.desc, sql).get("Create Table").toString();
			isDelete = true;
		} catch (RuntimeException e) {
			isDelete = false;
			//table is not exist
		}
		String descMd5 = md5Service.md5(descData);
		if (!descMd5.equals(srcMd5)) {
			if (isDelete)
				dbService.execute(syncConnection.desc, "drop table " + tableName);
			dbService.execute(syncConnection.desc, srcData);
			logger.info(tableName + "表结构错误，已经重建");
		}
	}
}
