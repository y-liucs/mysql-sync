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
        boolean isDelete;
        String srcData = null;
        String descData = null;
        try {
            srcData = dbService.queryOne(syncConnection.src, "show create table " + db.getSrcTableName()).get("Create Table").toString();
            srcData = srcData.replaceFirst(db.getSrcTableName(), db.getDescTableName());
            descData = dbService.queryOne(syncConnection.desc, "show create table " + db.getDescTableName()).get("Create Table").toString();
            isDelete = true;
        } catch (RuntimeException e) {
            isDelete = false;
        }
        if (!md5Service.md5(srcData).equals(md5Service.md5(descData))) {
            if (isDelete)
                dbService.execute(syncConnection.desc, "drop table " + db.getDescTableName());
            dbService.execute(syncConnection.desc, srcData);
            logger.info(db.getDescTableName() + "表结构错误，已经重建");
        }
    }
}
