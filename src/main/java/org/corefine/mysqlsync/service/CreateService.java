package org.corefine.mysqlsync.service;

import org.corefine.mysqlsync.config.SyncConfig.DatabaseSyncConfig;
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

	public void sync(SyncConnection syncConnection, DatabaseSyncConfig db) {
//		String sql = "show create table " + db.getSrcTableName();
//		String srcData = dbService.queryOne(syncConnection.src, sql).get("Create Table").toString();
//		String srcMd5 = md5Service.md5(srcData);
		String descData = "";
		boolean isDelete;
		try {
			String sql = "show create table " + db.getDescTableName();
			descData = dbService.queryOne(syncConnection.desc, sql).get("Create Table").toString();
			isDelete = true;
		} catch (RuntimeException e) {
			isDelete = false;
			//table is not exist
		}
		if (isDelete)
			dbService.execute(syncConnection.desc, "drop table " + db.getDescTableName());
		System.out.println("descData:" + descData);
		dbService.execute(syncConnection.desc, descData);
		logger.info(db.getDescTableName() + "表结构错误，已经重建");
	}
}
