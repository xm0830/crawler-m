package com.rainbow.crawler.storage;

import com.rainbow.crawler.common.MD5Utils;
import com.rainbow.crawler.fetcher.FetchResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.util.Map;

/**
 * Created by xuming on 2017/4/21.
 */
public class HBaseStorage implements Storage {

    private String tableName = null;
    private Connection connection = null;
    private Table table = null;

    public HBaseStorage(String tableName) throws IOException {
        this.tableName = tableName;

        init();
    }

    @Override
    public void save(FetchResult result) throws IOException {
        if (connection == null && table == null) {
            init();
        }

        int columnNum = 0;
        for (Map<String, String> row : result.getData()) {
            if (columnNum == 0) {
                columnNum = row.size();
            }

            if (row.size() != columnNum) {
                throw new RuntimeException("error format data: all row not has save column num!");
            }

            Put put = new Put(Bytes.toBytes(MD5Utils.encrypt(row.get("url"), MD5Utils.MD5_KEY)));
            for (Map.Entry<String, String> entry : row.entrySet()) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
            }

            table.put(put);
        }
    }

    @Override
    public void close() {
        IOUtils.closeStream(table);
        IOUtils.closeStream(connection);

        connection = null;
        table = null;
    }

    private void init() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf(tableName));
    }
}
