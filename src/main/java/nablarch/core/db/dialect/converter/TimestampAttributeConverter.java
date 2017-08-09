package nablarch.core.db.dialect.converter;

import java.sql.Date;
import java.sql.Timestamp;

import nablarch.core.db.util.DbUtil;

/**
 * {@link Timestamp}をデータベースとの間で入出力するために変換するクラス。
 *
 * @author siosio
 */
public class TimestampAttributeConverter implements AttributeConverter<Timestamp> {

    /**
     * 以下の型への変換をサポートする。
     *
     * <ul>
     *     <li>{@link Timestamp}</li>
     *     <li>{@link Date}</li>
     *     <li>{@link String}</li>
     * </ul>
     *
     * 上記に以外の型への変換はサポートしないため{@link IllegalArgumentException}を送出する。
     * また、{@code null}もサポートしない。
     */
    @SuppressWarnings("unchecked")
    @Override
    public <DB> Object convertToDatabase(final Timestamp javaAttribute, final Class<DB> databaseType) {
        if (databaseType.isAssignableFrom(Timestamp.class)) {
            return databaseType.cast(javaAttribute);
        } else if (databaseType.isAssignableFrom(Date.class)) {
            return (DB) new Date(DbUtil.trimTime(javaAttribute).getTimeInMillis());
        } else if (databaseType.isAssignableFrom(String.class)) {
            return (DB) javaAttribute.toString();
        }
        throw new IllegalArgumentException("unsupported database type:"
                + databaseType.getName());
    }

    /**
     * 以下の型からの変換をサポートする。
     *
     * <ul>
     *     <li>{@link Timestamp}</li>
     *     <li>{@link java.util.Date}</li>
     *     <li>{@link String}</li>
     * </ul>
     *
     * 上記に以外の型からの変換はサポートしないため{@link IllegalArgumentException}を送出する。
     * なお、{@code null}は変換せずに{@code null}を返却する。
     */
    @Override
    public Timestamp convertFromDatabase(final Object databaseAttribute) {
        if (databaseAttribute == null) {
            return null;
        } else if (databaseAttribute instanceof Timestamp) {
            return (Timestamp) databaseAttribute;
        } else if (databaseAttribute instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) databaseAttribute).getTime());
        } else if (databaseAttribute instanceof String) {
            return Timestamp.valueOf((String) databaseAttribute);
        }
        throw new IllegalArgumentException("unsupported data type:"
                + databaseAttribute.getClass()
                                   .getName() + ", value:" + databaseAttribute);
    }
}
