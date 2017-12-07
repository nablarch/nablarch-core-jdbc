package nablarch.core.db.statement.sqlloader;

import java.util.Map;
import java.util.Map.Entry;

/**
 * SQL文中のプレースホルダーに対して置換を行うクラス。
 *
 * プレースホルダーと、それに対応する置き換え後の値を設定することで、
 * SQL文中の任意のプレースホルダーに対して置換を行うことができる。
 *
 * 例えば、SQL文中でスキーマの置き換えをしたい場合、以下のようにコンポーネント設定をする。
 * <pre>{@code
 * <component class="nablarch.core.db.statement.sqlloader.SqlPlaceHolderReplacer">
 *   <property name="placeHolderValuePair">
 *     <map>
 *       <entry key="#SCHEMA_A#" value="AAA"/>
 *       <entry key="#SCHEMA_B#" value="BBB"/>
 *     </map>
 *   </property>
 * </component>
 * }</pre>
 *
 * この場合、
 * <pre>{@code SELECT FROM #SCHEMA_A#.TBL1 CROSS JOIN #SCEHMA_B#.TBL2}</pre>
 * というSQL文が
 * <pre>{@code SELECT FROM AAA.TBL1 CROSS JOIN BBB.TBL2}</pre>
 * というSQL文に変換される。
 *
 * @author Tsuyoshi Kawasaki
 */
public class SqlPlaceHolderReplacer implements SqlLoaderCallback {

    /**
     * プレースホルダーと、それに対応する置き換え後の値を保持するMap。
     */
    private Map<String, String> placeHolderValuePair;

    @Override
    public String processOnAfterLoad(String sql, String sqlId) {
        if (placeHolderValuePair == null) {
            throw new IllegalStateException("placeHolderValuePair must be set.");
        }

        String processed = sql;
        for (Entry<String, String> entry : placeHolderValuePair.entrySet()) {
            String placeHolder = entry.getKey();
            String value = entry.getValue();
            processed = processed.replace(placeHolder, value);
        }
        return processed;
    }

    /**
     * プレースホルダーと値のペアを設定する。
     *
     * @param placeHolderValuePair プレースホルダーと値のペア
     */
    public void setPlaceHolderValuePair(Map<String, String> placeHolderValuePair) {
        this.placeHolderValuePair = placeHolderValuePair;
    }
}
