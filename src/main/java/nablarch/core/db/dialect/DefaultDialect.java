package nablarch.core.db.dialect;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import nablarch.core.db.dialect.converter.AttributeConverter;
import nablarch.core.db.statement.ResultSetConvertor;
import nablarch.core.db.statement.SelectOption;
import nablarch.core.util.annotation.Published;

/**
 * デフォルトの{@link Dialect}実装クラス。
 * <p/>
 * 本実装では、全ての方言が無効化される。
 *
 * @author hisaaki sioiri
 */
@Published(tag = "architect")
public class DefaultDialect implements Dialect {

    /** {@link ResultSet}から値を取得するクラス */
    private static final ResultSetConvertor RESULT_SET_CONVERTOR = new DefaultResultSetConvertor();

    /** 型変換を行う{@link AttributeConverterFactory}を生成するクラス */
    private AttributeConverterFactory attributeConverterFactory = new BasicAttributeConverterFactory();

    /**
     * SQL型に対応するJavaクラスのマッピング定義。
     */
    private static final Map<Integer, Class<?>> SQL_TYPE_CONVERTER_MAP;

    static {
        final Map<Integer, Class<?>> sqlTypeConverterMap = new HashMap<Integer, Class<?>>();
        sqlTypeConverterMap.put(Types.BIT, Boolean.class);
        sqlTypeConverterMap.put(Types.TINYINT, Byte.class);
        sqlTypeConverterMap.put(Types.SMALLINT, Short.class);
        sqlTypeConverterMap.put(Types.INTEGER, Integer.class);
        sqlTypeConverterMap.put(Types.BIGINT, Long.class);
        sqlTypeConverterMap.put(Types.FLOAT, Double.class);
        sqlTypeConverterMap.put(Types.REAL, Float.class);
        sqlTypeConverterMap.put(Types.DOUBLE, Double.class);
        sqlTypeConverterMap.put(Types.NUMERIC, BigDecimal.class);
        sqlTypeConverterMap.put(Types.DECIMAL, BigDecimal.class);
        sqlTypeConverterMap.put(Types.CHAR, String.class);
        sqlTypeConverterMap.put(Types.VARCHAR, String.class);
        sqlTypeConverterMap.put(Types.LONGVARCHAR, String.class);
        sqlTypeConverterMap.put(Types.DATE, java.sql.Date.class);
        sqlTypeConverterMap.put(Types.TIME, java.sql.Time.class);
        sqlTypeConverterMap.put(Types.TIMESTAMP, java.sql.Timestamp.class);
        sqlTypeConverterMap.put(Types.BINARY, byte[].class);
        sqlTypeConverterMap.put(Types.VARBINARY, byte[].class);
        sqlTypeConverterMap.put(Types.LONGVARBINARY, byte[].class);
        sqlTypeConverterMap.put(Types.BLOB, byte[].class);
        sqlTypeConverterMap.put(Types.CLOB, String.class);
        sqlTypeConverterMap.put(Types.BOOLEAN, Boolean.class);

        SQL_TYPE_CONVERTER_MAP = Collections.unmodifiableMap(sqlTypeConverterMap);
    }

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean supportsIdentity() {
        return false;
    }

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean supportsSequence() {
        return false;
    }

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean supportsOffset() {
        return false;
    }

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean isTransactionTimeoutError(SQLException sqlException) {
        return false;
    }

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean isDuplicateException(SQLException sqlException) {
        return false;
    }

    /**
     * 全てのカラムを{@link ResultSet#getObject(int)}で取得するコンバータを返す。
     *
     * @return {@inheritDoc}
     */
    @Override
    public ResultSetConvertor getResultSetConvertor() {
        return RESULT_SET_CONVERTOR;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * シーケンス採番はサポートしない。
     *
     * @throws UnsupportedOperationException 呼び出された場合
     */
    @Override
    public String buildSequenceGeneratorSql(final String sequenceName) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("sequence generator is unsupported.");
    }

    /**
     * SQL文を変換せずに返す。
     */
    @Override
    public String convertPaginationSql(String sql, SelectOption selectOption) {
        return sql;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * 以下形式のCOUNT文取得用SQL文に変換する。<br/>
     * {@code SELECT COUNT(*) COUNT_ FROM ('引数のSQL') SUB_}
     */
    @Override
    public String convertCountSql(String sql) {
        return "SELECT COUNT(*) COUNT_ FROM (" + sql + ") SUB_";
    }

    /**
     * {@inheritDoc}
     *
     * デフォルト実装では、本メソッドはサポートしない。
     */
    @Override
    public String getPingSql() {
        throw new UnsupportedOperationException("unsupported getPingSql.");
    }

    /**
     * 全て{@link ResultSet#getObject(int)}で値を取得する{@link ResultSetConvertor}の実装クラス。
     */
    private static class DefaultResultSetConvertor implements ResultSetConvertor {

        @Override
        public Object convert(ResultSet rs, ResultSetMetaData rsmd, int columnIndex) throws SQLException {
            return rs.getObject(columnIndex);
        }

        @Override
        public boolean isConvertible(ResultSetMetaData rsmd, int columnIndex) throws SQLException {
            return true;
        }
    }

    @Override
    public Object convertToDatabase(final Object value, final int sqlType) {
        final Class dbType = SQL_TYPE_CONVERTER_MAP.get(sqlType);
        if (dbType == null) {
            throw new IllegalArgumentException("unsupported sqlType: " + sqlType);
        }
        return convertToDatabase(value, dbType);
    }

    @Override
    public <T, DB> Object convertToDatabase(final T value, final Class<DB> dbType) {
        if (value == null) {
            return null;
        }
        @SuppressWarnings("unchecked")
        final AttributeConverter<T> converter = getAttributeConverter((Class<T>) value.getClass());
        return converter.convertToDatabase(value, dbType);
    }

    @Override
    public <T> T convertFromDatabase(final Object value, final Class<T> javaType) {
        final AttributeConverter<T> converter = getAttributeConverter(javaType);
        return converter.convertFromDatabase(value);
    }

    /**
     * 指定の型をデータベースの入出力で変換するためのコンバータを返す。
     *
     * @param javaType データベースへの入出力対象のクラス
     * @param <T> データベースへの入出力対象の型
     * @return 指定の型を変換するコンバータ
     */
    @SuppressWarnings("unchecked")
    protected <T> AttributeConverter<T> getAttributeConverter(Class<T> javaType) {
        return attributeConverterFactory.factory(javaType);
    }

    /**
     * {@link AttributeConverter}のファクトリクラスを設定する。
     *
     * @param attributeConverterFactory ファクトリクラス。
     */
    public void setAttributeConverterFactory(final AttributeConverterFactory attributeConverterFactory) {
        this.attributeConverterFactory = attributeConverterFactory;
    }
}

