package com.fatri.utils;


import com.fatri.config.BundleUtils;
import com.fatri.pool.hbase.HbaseConnectionPool;
import com.fatri.pool.tool.ConnectionPoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author FU1189
 * @version v_1.0.0
 * 用于操作HBase的工具类
 * @since 2020/9/18
 */
public class HBaseUtils {

    private static Logger LOG = LoggerFactory.getLogger(HBaseUtils.class);

    private static Configuration conf;

    private static Connection conn;

    static {
        ConnectionPoolConfig config = new ConnectionPoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(1000);
        config.setTestOnBorrow(true);

        conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, BundleUtils.getStringByKey(HConstants.ZOOKEEPER_QUORUM));
        conf.set("hbase.defaults.for.version.skip", "true");
        HbaseConnectionPool pool = null;
        try {
            pool = new HbaseConnectionPool(config, conf);

            conn = pool.getConnection();
//            conn = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            LOG.error("connect hbase error!", e);
        } finally {
            if (null != pool) {
                pool.close();
            }
        }
    }

    private HBaseUtils() {

    }

    /**
     * @param conf conf配置
     * @return conn HBase连接
     */
    public static Connection getConnection(Configuration conf) {
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            LOG.error("connect hbase error!", e);
        }
        return conn;
    }

    /**
     * 关闭流
     * @param table Table对象
     */
    private static void close(Table table){
        if (null != table) {
            try {
                table.close();
            } catch (IOException e) {
                LOG.error("close table stream error!", e);
            }
        }
    }

    /**
     * 关闭流
     * @param admin Admin对象
     */
    private static void close(Admin admin){
        if (null != admin) {
            try {
                admin.close();
            } catch (IOException e) {
                LOG.error("close admin stream error!", e);
            }
        }
    }

    /**
     * 用于向HBase中保存数据
     *
     * @param put       HBase put对象
     * @param tableName 表名
     */
    public static void save(Put put, String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            table.put(put);
            LOG.info("save hbase success! tableName:{}", tableName);
        } catch (IOException e) {
            LOG.error("save hbase error!", e);
        } finally {
            close(table);
        }

    }

    /**
     * 用于向HBase中保存数据
     *
     * @param put       HBase put集合
     * @param tableName 表名
     */
    public static void save(List<Put> put, String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            table.put(put);
            LOG.info("save hbase success! tableName:{}", tableName);
        } catch (IOException e) {
            LOG.error("save hbase error!", e);
        } finally {
            close(table);
        }
    }

    /**
     * 用于向HBase中插入单列数据
     *
     * @param tableName 表名
     * @param rowKey    rowkey
     * @param family    列族
     * @param quailifer 列名
     * @param value     列值
     */
    public static void insert(String tableName, String rowKey, String family, String quailifer, String value) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowKey.getBytes());
            put.addColumn(family.getBytes(), quailifer.getBytes(), value.getBytes());
            table.put(put);
            LOG.info("insert table success! tableName:{}", tableName);
        } catch (IOException e) {
            LOG.error("insert table error! tableName:{}", tableName, e);
        } finally {
            close(table);
        }
    }

    /**
     * 用于向HBase中插入多列数据
     *
     * @param tableName 表名
     * @param rowKey    rowkey
     * @param family    列族
     * @param quailifer 列名数组
     * @param value     列值数组
     */
    public static void insert(String tableName, String rowKey, String family, String[] quailifer, String[] value) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            List<Put> puts = new ArrayList<Put>();
            for (int i = 0; i < quailifer.length; i++) {
                Put put = new Put(rowKey.getBytes());
                put.addColumn(family.getBytes(), quailifer[i].getBytes(), value[i].getBytes());
                puts.add(put);
            }
            table.put(puts);
            LOG.info("insert table success! tableName:{}", tableName);
        } catch (IOException e) {
            LOG.error("insert table error! tableName:{}", tableName, e);
        } finally {
            close(table);
        }
    }

    /**
     * 用于查询一条数据
     *
     * @param tableName 表名
     * @param rowKey    rowkey
     * @return
     */
    public static Result getOneRow(String tableName, String rowKey) {
        Table table = null;
        Result rs = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            rs = table.get(get);
        } catch (IOException e) {
            LOG.error("get one row error! tableName:{}", tableName, e);
        } finally {
            close(table);
        }
        return rs;
    }

    /**
     * 用于按rowkey前缀查询全部结果
     *
     * @param tableName  表名
     * @param rowKeyLike rowkey
     * @return
     */
    public static List<Result> getRows(String tableName, String rowKeyLike) {
        Table table = null;
        List<Result> rs = new ArrayList<Result>();
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            PrefixFilter preFilter = new PrefixFilter(rowKeyLike.getBytes());
            Scan scan = new Scan();
            scan.setFilter(preFilter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                rs.add(result);
            }
        } catch (IOException e) {
            LOG.error("get rows error! tableName:{}", tableName, e);
        } finally {
            close(table);
        }
        return rs;
    }

    /**
     * 用于按列族，rowkey前缀取指定列的数据
     *
     * @param tableName  表名
     * @param family     列族
     * @param rowKeyLike rowkey前缀
     * @param cols       列名
     * @return
     */
    public static List<Result> getRows(String tableName, String family, String rowKeyLike, String[] cols) {
        Table table = null;
        List<Result> rs = new ArrayList<Result>();
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            PrefixFilter preFilter = new PrefixFilter(rowKeyLike.getBytes());
            Scan scan = new Scan();
            scan.setFilter(preFilter);
            for (String col : cols) {
                scan.addColumn(family.getBytes(), col.getBytes());
            }
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                rs.add(result);
            }
        } catch (IOException e) {
            LOG.error("get rows error! tableName:{}", tableName, e);
        } finally {
            close(table);
        }
        return rs;
    }

    /**
     * 根据起始和结束的row获取全表数据
     *
     * @param tableName 表名
     * @param startRow  起始row
     * @param stopRow   结束row
     * @return
     */
    public static List<Result> getRows(String tableName, String startRow, String stopRow) {
        Table table = null;
        List<Result> rs = new ArrayList<Result>();
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(stopRow.getBytes());
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                rs.add(result);
            }
        } catch (IOException e) {
            LOG.error("get rows error! tableName:{}", tableName, e);
        } finally {
            close(table);
        }
        return rs;
    }

    /**
     * 用于根据指定rowkey前缀删除记录
     *
     * @param tableName  表名
     * @param rowKeyLike rowkey前缀
     */
    public static void deleteRecords(String tableName, String rowKeyLike) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(rowKeyLike.getBytes());
            table.delete(delete);
        } catch (IOException e) {
            LOG.error("delete records error! tableName:{}", tableName, e);
        } finally {
            close(table);
        }
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    public static void deleteTable(String tableName) {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = conn.getAdmin();
            if (!admin.tableExists(table)) {
                LOG.warn("{} is not exists!", tableName);
            } else {
                admin.disableTable(table);
                admin.deleteTable(table);
                LOG.info("delete {} succcess!", tableName);
            }
        } catch (MasterNotRunningException e) {
            LOG.info("master not running!", e);
        } catch (ZooKeeperConnectionException e) {
            LOG.info("zookeeper not connect!", e);
        } catch (IOException e) {
            LOG.info("delete {} error!", tableName, e);
        } finally {
            close(admin);
        }
    }

    /**
     * 根据表名和列族创建表，可以有多个列族
     *
     * @param tableName     表名
     * @param columnFamilys 列族
     */
    public static void createTable(String tableName, String[] columnFamilys) {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = conn.getAdmin();
            if (admin.tableExists(table)) {
                LOG.warn("{} already exists!", tableName);
            } else {
                HTableDescriptor desc = new HTableDescriptor(table);
                for (String family : columnFamilys) {
                    desc.addFamily(new HColumnDescriptor(family));
                }
                admin.createTable(desc);
                LOG.info("{} create success!", tableName);
            }
        } catch (MasterNotRunningException e) {
            LOG.info("master not running!", e);
        } catch (ZooKeeperConnectionException e) {
            LOG.info("zookeeper not connect!", e);
        } catch (IOException e) {
            LOG.info("create {} error!", tableName, e);
        } finally {
            close(admin);
        }
    }

    /**
     * 根据表名和列族创建表，只能单个列族
     *
     * @param tableName    表名
     * @param columnFamily 列族
     */
    public static void createTable(String tableName, String columnFamily) {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = conn.getAdmin();
            if (admin.tableExists(table)) {
                LOG.warn("{} already exists!", tableName);
            } else {
                HTableDescriptor desc = new HTableDescriptor(table);
                desc.addFamily(new HColumnDescriptor(columnFamily));
                admin.createTable(desc);
                LOG.info("{} create success!", tableName);
            }
        } catch (MasterNotRunningException e) {
            LOG.info("master not running!", e);
        } catch (ZooKeeperConnectionException e) {
            LOG.info("zookeeper not connect!", e);
        } catch (IOException e) {
            LOG.info("create {} error!", tableName, e);
        } finally {
            close(admin);
        }

    }

}
