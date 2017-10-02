package nablarch.core.db.dialect.converter;

import java.sql.Blob;
import java.sql.SQLException;

import nablarch.core.db.DbAccessException;

/**
 * バイナリ({@code byte[]})をデータベースとの間で入出力するために変換するクラス。
 *
 * @author siosio
 */
public class ByteArrayAttributeConverter implements AttributeConverter<byte[]> {

    /**
     * 以下の型への変換をサポートする。
     *
     * <ul>
     *     <li>byte[]</li>
     * </ul>
     *
     * 上記に以外の型への変換はサポートしないため{@link IllegalArgumentException}を送出する。
     */
    @Override
    public <DB> Object convertToDatabase(final byte[] javaAttribute, final Class<DB> databaseType) {
        if (databaseType.isAssignableFrom(byte[].class)) {
            return javaAttribute;
        }
        throw new IllegalArgumentException("unsupported database type:"
                + databaseType.getName());
    }

    /**
     * 以下の型からの変換をサポートする。
     *
     * <ul>
     *     <li>byte[]</li>
     *     <li>{@link Blob}</li>
     * </ul>
     *
     * 上記に以外の型からの変換はサポートしないため{@link IllegalArgumentException}を送出する。
     * なお、{@code null}は変換せずに{@code null}を返却する。
     */
    @Override
    public byte[] convertFromDatabase(final Object databaseAttribute) {
        if (databaseAttribute == null) {
            return null;
        } else if (databaseAttribute instanceof byte[]) {
            return byte[].class.cast(databaseAttribute);
        } else if (databaseAttribute instanceof Blob) {
            Blob blob = Blob.class.cast(databaseAttribute);
            try {
                return blob.getBytes(1, (int) blob.length());
            } catch (SQLException e) {
                throw new DbAccessException("BLOB access failed.", e);
            }
        }
        throw new IllegalArgumentException(
                "unsupported data type:" + databaseAttribute.getClass()
                                                            .getName() + ", value:" + databaseAttribute);
    }
}
