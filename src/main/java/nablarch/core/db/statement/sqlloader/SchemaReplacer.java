package nablarch.core.db.statement.sqlloader;

import nablarch.core.util.StringUtil;

/**
 * スキーマのプレースホルダーを置き換えるクラス。
 *
 * SQL文中に{@literal #SCHEMA#}というプレースホルダーがあれば、それを指定されたスキーマ名で置換する。
 *
 *
 * @author Tsuyoshi Kawasaki
 */
public class SchemaReplacer implements SqlLoaderCallback {

    /** プレースホルダー文字列 */
    private static final String SCHEMA_PLACEHOLDER = "#SCHEMA#";

    /** スキーマ名 */
    private String schemaName;

    @Override
    public String processOnAfterLoad(String sql, String unused) {
        return sql.replace(SCHEMA_PLACEHOLDER, getSchemaName());
    }

    /**
     * スキーマ名を取得する。
     * スキーマ名が設定されていない場合、例外が発生する。
     *
     * @return スキーマ名
     */
    private String getSchemaName() {
        if (StringUtil.isNullOrEmpty(schemaName)) {
            throw new IllegalStateException("schema name must be set.");
        }
        return schemaName;
    }

    /**
     * スキーマ名を設定する。
     * @param schemaName スキーマ名
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

}
