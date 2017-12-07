package nablarch.core.db.statement.sqlloader;

import java.util.HashMap;
import java.util.Map;

/**
 * スキーマのプレースホルダーを置き換えるクラス。
 *
 * SQL文中に{@literal #SCHEMA#}というプレースホルダーがあれば、それを指定されたスキーマ名で置換する。
 *
 *
 * @author Tsuyoshi Kawasaki
 */
public class SchemaReplacer implements SqlLoaderCallback {

    /** 実際の置換を行うクラス */
    private final SqlPlaceHolderReplacer replacer = new SqlPlaceHolderReplacer();

    /** プレースホルダー文字列 */
    private static final String SCHEMA_PLACEHOLDER = "#SCHEMA#";

    @Override
    public String processOnAfterLoad(String sql, String sqlId) {
        return replacer.processOnAfterLoad(sql, sqlId);
    }

    /**
     * スキーマ名を設定する。
     * @param schemaName スキーマ名
     */
    public void setSchemaName(String schemaName) {
        Map<String, String> placeHolderValuePair = new HashMap<String, String>();
        placeHolderValuePair.put(SCHEMA_PLACEHOLDER, schemaName);
        replacer.setPlaceHolderValuePair(placeHolderValuePair);
    }

}
