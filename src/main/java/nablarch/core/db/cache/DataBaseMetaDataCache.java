package nablarch.core.db.cache;

import nablarch.core.db.DbAccessException;
import nablarch.core.util.StringUtil;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * データベースのメタデータをキャッシュするクラス。
 * @author ryo asato
 */
public class DataBaseMetaDataCache {

    private static DataBaseMetaDataCache instance = null;
    private Map<String, TableDescriptor> tableDescriptorMap = new HashMap<String, TableDescriptor>();

    private DataBaseMetaDataCache() {
    }

    public static synchronized DataBaseMetaDataCache getInstance() {
        if (instance == null) {
            instance = new DataBaseMetaDataCache();
        }
        return instance;
    }

    /**
     * 指定したテーブルに対応する{@link TableDescriptor}を取得する。
     * スキーマ名を指定しなかった場合、デフォルトスキーマを参照する。
     *
     * @param schema スキーマ名
     * @param tableName テーブル名
     * @param connection {@link Connection}
     * @return {@link TableDescriptor}
     */
    public TableDescriptor getTableDescriptor(String schema, String tableName, Connection connection) {
        if (StringUtil.isNullOrEmpty(tableName) || connection == null) {
            throw new IllegalArgumentException("tableName or connection is null or empty.");
        }

        String key = addSchema(schema, tableName);
        TableDescriptor tableDescriptor = tableDescriptorMap.get(key);
        if (tableDescriptor == null) {
            synchronized (this) {
                ResultSetMetaData metaData = getMetaData(schema, tableName, connection);
                tableDescriptor = new TableDescriptor(tableName, createColumnDescriptors(tableName, metaData));
                tableDescriptorMap.put(key, tableDescriptor);
            }
        }
        return tableDescriptor;
    }

    /**
     * 指定したテーブルのカラムに対応する{@link ColumnDescriptor}を取得する。
     * スキーマ名を指定しなかった場合、デフォルトスキーマを参照する。
     *
     * @param schema スキーマ名
     * @param tableName テーブル名
     * @param columnName カラム名
     * @param connection {@link Connection}
     * @return {@link ColumnDescriptor}
     */
    public ColumnDescriptor getColumnDescriptor(String schema, String tableName, String columnName, Connection connection) {
        if (StringUtil.isNullOrEmpty(tableName) || StringUtil.isNullOrEmpty(columnName) || connection == null) {
            throw new IllegalArgumentException("tableName, columnName or connection is null or empty.");
        }

        TableDescriptor tableDescriptor = getTableDescriptor(schema, tableName, connection);
        return tableDescriptor.getColumnDescriptor(columnName);
    }

    private Map<String, ColumnDescriptor> createColumnDescriptors(String tableName, ResultSetMetaData meta) {
        Map<String, ColumnDescriptor> ret = new HashMap<String, ColumnDescriptor>();
        String className = null;
        try {
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                String columnName = meta.getColumnName(i);
                int columnType = meta.getColumnType(i);
                className = meta.getColumnClassName(i);
                ret.put(columnName, new ColumnDescriptor(columnName, columnType, Class.forName(className)));
            }
        } catch (SQLException e) {
            throw new DbAccessException("Can not access to metadata. tablename = " + tableName, e);
        } catch (ClassNotFoundException e) {
            // データベースのメタ情報から取得したクラス名を指定しているので、ここには到達しない
            throw new IllegalStateException("Can not create Class object. class = " + className, e);
        }
        return ret;
    }

    /**
     * 指定したテーブルのメタ情報をデータベースから取得する。
     * スキーマ名を指定しなかった場合、デフォルトスキーマを参照する。
     *
     * @param schema スキーマ名
     * @param tableName テーブル名
     * @param connection {@link Connection}
     * @return {@link ResultSetMetaData}
     */
    private ResultSetMetaData getMetaData(String schema, String tableName, Connection connection) {
        // メタデータ取得用のSQL
        String sql = "SELECT * FROM " + addSchema(schema, tableName) + " WHERE 1 = 0";
        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            return ps.executeQuery().getMetaData();
        } catch (SQLException e) {
            throw new DbAccessException("Can not access to metadata. tablename = " + tableName, e);
        }
    }

    private String addSchema(String schema, String tableName) {
        return StringUtil.hasValue(schema) ? schema + "." + tableName : tableName;
    }
}