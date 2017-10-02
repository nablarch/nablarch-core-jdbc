package nablarch.core.db.dialect.converter;

import java.math.BigDecimal;

/**
 * {@link Long}をデータベースとの間で入出力するために変換するクラス。
 *
 * @author siosio
 */
public class LongAttributeConverter implements AttributeConverter<Long> {

    /**
     * 以下の型への変換をサポートする。
     *
     * <ul>
     *     <li>{@link Long}</li>
     *     <li>{@link BigDecimal}</li>
     *     <li>{@link String}</li>
     * </ul>
     *
     * 上記に以外の型への変換はサポートしないため{@link IllegalArgumentException}を送出する。
     * また、{@code null}もサポートしない。
     */
    @Override
    public <DB> Object convertToDatabase(final Long javaAttribute, final Class<DB> databaseType) {
        if (databaseType.isAssignableFrom(Long.class)) {
            return javaAttribute;
        } else if (databaseType.isAssignableFrom(BigDecimal.class)) {
            return BigDecimal.valueOf(javaAttribute);
        } else if (databaseType.isAssignableFrom(Integer.class)) {
            return Integer.valueOf(javaAttribute.toString());
        } else if (databaseType.isAssignableFrom(Short.class)) {
            return Short.valueOf(javaAttribute.toString());
        } else if (databaseType.isAssignableFrom(String.class)) {
            return String.valueOf(javaAttribute);
        }
        throw new IllegalArgumentException("unsupported database type:"
                + databaseType.getName());
    }

    /**
     * 以下の型からの変換をサポートする。
     *
     * <ul>
     *     <li>{@link Number}</li>
     *     <li>{@link String}</li>
     * </ul>
     *
     * 上記に以外の型からの変換はサポートしないため{@link IllegalArgumentException}を送出する。
     * なお、{@code null}は変換せずに{@code null}を返却する。
     */
    @Override
    public Long convertFromDatabase(final Object databaseAttribute) {
        if (databaseAttribute == null) {
            return null;
        } else if (databaseAttribute instanceof Long) {
            return Long.class.cast(databaseAttribute);
        } else if (databaseAttribute instanceof BigDecimal) {
            return BigDecimal.class.cast(databaseAttribute).longValueExact();
        } else if (databaseAttribute instanceof Number) {
            return Long.valueOf(databaseAttribute.toString());
        } else if (databaseAttribute instanceof String) {
            return Long.valueOf(String.class.cast(databaseAttribute));
        }
        throw new IllegalArgumentException(
                "unsupported data type:" + databaseAttribute.getClass()
                                                            .getName() + ", value:" + databaseAttribute);
    }
    
    /**
     * プリミティブ({@code long})を変換するクラス。
     * <p>
     * このクラスでは、データベースから変換する値がnullの場合に、{@code 0}に置き換えて返す。
     */
    public static class Primitive implements AttributeConverter<Long> {

        /** 委譲先の{@link AttributeConverter}。 */
        private final LongAttributeConverter converter = new LongAttributeConverter();

        @Override
        public <DB> Object convertToDatabase(final Long javaAttribute, final Class<DB> databaseType) {
            return converter.convertToDatabase(javaAttribute, databaseType);
        }

        @Override
        public Long convertFromDatabase(final Object databaseAttribute) {
            final Long aLong = converter.convertFromDatabase(databaseAttribute);
            if (aLong == null) {
                return 0L;
            }
            return aLong;
        }
    }
}
