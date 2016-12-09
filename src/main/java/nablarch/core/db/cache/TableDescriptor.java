package nablarch.core.db.cache;

import java.util.Map;

/**
 * データベースのテーブルに関するメタ情報を保持するクラス。
 * @author ryo asato
 */
public class TableDescriptor {

    private String tableName;

    private Map<String, ColumnDescriptor> columnDescMap;

    public TableDescriptor(String tableName, Map<String, ColumnDescriptor> columnDescMap) {
        this.tableName = tableName;
        this.columnDescMap = columnDescMap;
    }

    /**
     * 指定したカラムの{@link ColumnDescriptor}を取得する。
     * 指定したカラムが存在しない場合、nullを返す。
     * @param columnName カラム名
     * @return {@link ColumnDescriptor}
     */
    public ColumnDescriptor getColumnDescriptor(String columnName) {
        return columnDescMap.get(columnName);
    }

    /**
     * テーブル名を返す。
     * @return テーブル名
     */
    public String getTableName() {
        return tableName;
    }
}

