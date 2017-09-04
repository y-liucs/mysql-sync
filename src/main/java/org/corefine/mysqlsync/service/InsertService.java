package org.corefine.mysqlsync.service;

import org.corefine.mysqlsync.config.ColumnsConfig;
import org.corefine.mysqlsync.config.SyncConfig.DatabaseSyncConfig;
import org.corefine.mysqlsync.service.DBService.SyncConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@Service
public class InsertService {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private DBService dbService;
    @Autowired
    private ColumnsConfig columnsConfig;
    @Value("${oneQueryRows}")
    private Integer oneQueryRows;

    public void sync(SyncConnection syncConnection, DatabaseSyncConfig db) {
        Long maxId = dbService.querySimple(syncConnection.desc, "select " + columnsConfig.getId()
                + " from " + db.getDescTableName() + " order by " + columnsConfig.getId() + " desc limit 1");
        if (maxId == null)
            maxId = 0l;
        while (true) {
            //1.查询数据
            String srcTableName = db.getSrcTableName();
            String sql = "select * from " + srcTableName;
            sql += " where " + columnsConfig.getId() + " > ?";
            sql += " order by " + columnsConfig.getId() + " asc limit " + oneQueryRows;
            List<Map<String, Object>> dataList = dbService.query(syncConnection.src, sql, maxId);
            if (dataList.isEmpty())
                return;
            Map<String, Object> firstMap = dataList.get(0);
            //2.写入数据
            StringBuilder sb = new StringBuilder();
            sb.append("insert into ").append(db.getDescTableName()).append("(");
            int size = 0;
            for (String key : firstMap.keySet()) {
                sb.append("`").append(key).append("`").append(",");
                size++;
            }
            sb.delete(sb.length() - 1, sb.length());
            sb.append(") values ");
            int index = 0;
            Object[] datas = new Object[dataList.size() * size];
            for (Map<String, Object> data : dataList) {
                sb.append("(");
                for (Entry<String, Object> entry : data.entrySet()) {
                    sb.append("?,");
                    datas[index++] = entry.getValue();
                }
                sb.delete(sb.length() - 1, sb.length());
                sb.append("),");
            }
            sb.delete(sb.length() - 1, sb.length());
            dbService.execute(syncConnection.desc, sb.toString(), datas);
            maxId = (Long) dataList.get(dataList.size() - 1).get(columnsConfig.getId());
            logger.debug(db.getDescDBName() + "." + db.getDescTableName() + "新增" + dataList.size() + "条记录，当前ID：" + maxId);
            if (dataList.size() < oneQueryRows)
                return;
        }
    }
}
