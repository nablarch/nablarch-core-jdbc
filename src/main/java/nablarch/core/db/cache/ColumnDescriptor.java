package nablarch.core.db.cache;

/**
 * データベースのカラムに関するメタ情報を保持するクラス。
 * @author ryo asato
 */
public class ColumnDescriptor {

    private String columnName;

    private int columnType;

    private Class columnClass;

    public ColumnDescriptor(String columnName, int columnType, Class columnClass) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.columnClass = columnClass;
    }

    /**
     * カラム名を取得する。
     * @return カラム名
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * SQL型を取得する。
     * @return {@link java.sql.Types}からのSQL型
     */
    public int getColumnType() {
        return columnType;
    }

    /**
     * 対応するJavaのクラスを返す。
     * @return {@link Class}
     */
    public Class getColumnClass() {
        return columnClass;
    }
}