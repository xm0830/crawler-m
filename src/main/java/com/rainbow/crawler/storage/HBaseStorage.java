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
import java.util.List;
import java.util.UUID;

/**
 * Created by xuming on 2017/4/21.
 */
public class HBaseStorage implements Storage {

    private String tableName = null;
    private Connection connection = null;
    private Table table = null;
    private boolean isRandomRowKey = false;

    public HBaseStorage(String tableName) throws IOException {
        this(tableName, false);
    }

    public HBaseStorage(String tableName, boolean isRandomRowkey) throws IOException {
        this.tableName = tableName;
        this.isRandomRowKey = isRandomRowkey;
        init();
    }

    @Override
    public void save(FetchResult result) throws IOException {
        if (connection == null && table == null) {
            init();
        }

        List<String> columnNames = result.getColumnNames();

        for (int i = 0; i < result.size(); i++) {
            List<String> row = result.getRow(i);

            if (row.size() != columnNames.size()) {
                throw new RuntimeException("error format data: all row not has same column num!");
            }

            Put put = null;
            if (isRandomRowKey) {
                put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
            } else {
                put = new Put(Bytes.toBytes(MD5Utils.encrypt(result.getColumnValue(i, "url"), MD5Utils.MD5_KEY)));
            }

            for (int j = 0; j < columnNames.size(); j++) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(columnNames.get(j)), Bytes.toBytes(result.getColumnValue(i,columnNames.get(j))));
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
