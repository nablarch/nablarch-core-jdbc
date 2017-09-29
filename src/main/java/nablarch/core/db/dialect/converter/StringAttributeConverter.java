package nablarch.core.db.dialect.converter;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;

import nablarch.core.db.DbAccessException;
import nablarch.core.util.StringUtil;

/**
 * {@link String}をデータベースとの間で入出力するために変換するクラス。
 *
 * @author siosio
 */
public class StringAttributeConverter implements AttributeConverter<String> {

    /**
     * 以下の型への変換をサポートする。
     *
     * <ul>
     *     <li>{@link String}</li>
     *     <li>{@link BigDecimal}</li>
     *     <li>{@link Long}</li>
     *     <li>{@link Integer}</li>
     *     <li>{@link Short}</li>
     *     <li>{@link java.sql.Timestamp}</li>
     *     <li>{@link java.sql.Date}</li>
     * </ul>
     *
     * 上記に以外の型への変換はサポートしないため{@link IllegalArgumentException}を送出する。
     */
    @SuppressWarnings("unchecked")
    @Override
    public <DB> Object convertToDatabase(final String javaAttribute, final Class<DB> databaseType) {
        if (databaseType.isAssignableFrom(String.class)) {
            return databaseType.cast(javaAttribute);
        } else if (databaseType.isAssignableFrom(Short.class)) {
            return (DB) Short.valueOf(javaAttribute);
        } else if (databaseType.isAssignableFrom(Integer.class)) {
            return (DB) Integer.valueOf(javaAttribute);
        } else if (databaseType.isAssignableFrom(Long.class)) {
            return (DB) Long.valueOf(javaAttribute);
        } else if (databaseType.isAssignableFrom(BigDecimal.class)) {
            return (DB) new BigDecimal(javaAttribute);
        } else if (databaseType.isAssignableFrom(Date.class)) {
            return (DB) Date.valueOf(javaAttribute);
        } else if (databaseType.isAssignableFrom(Timestamp.class)) {
            return (DB) Timestamp.valueOf(javaAttribute);
        };
        throw new IllegalArgumentException("unsupported database type:"
                + databaseType.getName());
    }

    /**
     * 変換対象の値の文字列表現を返す。
     * 
     * 変換対象が{@code null}の場合は、{@code null}を返す。<br>
     * 変換対象が{@link Clob}の場合には、{@link Clob#getSubString(long, int)}の結果を返す。
     * このため、intの最大値を超える長さのCLOB値を本機能で扱うことは出来ない。
     * {@link nablarch.core.db.statement.SqlRow#get(Object)}を使用して取得した
     * {@link Clob}を直接使用すること。
     */
    @Override
    public String convertFromDatabase(final Object databaseAttribute) {
        if (databaseAttribute == null) {
            return null;
        } else if (databaseAttribute instanceof Clob) {
            final Clob clob = (Clob) databaseAttribute;
            try {
                return clob.getSubString(1, (int) clob.length());
            } catch (SQLException e) {
                throw new DbAccessException("CLOB access failed.", e);
            }
        } else {
            return StringUtil.toString(databaseAttribute);
        }
    }
}
