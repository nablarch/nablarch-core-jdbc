package nablarch.core.db.dialect;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import nablarch.core.db.statement.ResultSetConvertor;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

/**
 * {@link DefaultDialect}のテストクラス。
 * 方言は無効化されている。
 */
@RunWith(DatabaseTestRunner.class)
public class DefaultDialectTest {

    private DefaultDialect sut = new DefaultDialect();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    /** Native Connection */
    private Connection connection;

    @BeforeClass
    public static void setUp() throws Exception {
        VariousDbTestHelper.createTable(DialectEntity.class);
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * IDENTITY(オートインクリメントカラム)が使用できない。
     */
    @Test
    public void testSupportsIdentity() {
        assertThat(sut.supportsIdentity(), is(false));
    }

    /**
     * SEQUENCEが使用できない。
     */
    @Test
    public void testSupportsSequence() {
        assertThat(sut.supportsSequence(), is(false));
    }

    /**
     * SQL文でのオフセット指定が使用できない。
     */
    @Test
    public void testSupportsOffset() {
        assertThat(sut.supportsOffset(), is(false));
    }

    /**
     * SQL例外がトランザクションタイムアウトと判断すべき例外でない。
     */
    @Test
    public void testIsTransactionTimeoutError() {
        assertThat(sut.isTransactionTimeoutError(new SQLException()), is(false));
    }

    /**
     * SQL例外が一意制約違反による例外でない。
     */
    @Test
    public void testIsDuplicateException() {
        assertThat(sut.isDuplicateException(new SQLException()), is(false));
    }

    /**
     * {@link java.sql.ResultSet}から値を取得するための変換クラスのデフォルトを確認する。
     * メタ情報を使わず{@link java.sql.ResultSet}からカラム番号で取得する。
     */
    @Test
    @TargetDb(include = TargetDb.Db.ORACLE)
    public void testGetResultSetConvertor() throws Exception {
        Date date = new Date();
        Timestamp timestamp = Timestamp.valueOf("2015-03-16 01:02:03.123456");
        VariousDbTestHelper.setUpTable(
                new DialectEntity(1L, "12345", 100, 1234554321L, date, new BigDecimal("12345.54321"), timestamp,
                        new byte[] {0x00, 0x50, (byte) 0xFF}));
        connection = VariousDbTestHelper.getNativeConnection();
        final PreparedStatement statement = connection.prepareStatement(
                "SELECT ENTITY_ID, STR, NUM, BIG_INT, DECIMAL_COL, DATE_COL, TIMESTAMP_COL, BINARY_COL FROM DIALECT WHERE ENTITY_ID = ?");
        statement.setLong(1, 1L);
        final ResultSet rs = statement.executeQuery();
        assertThat("1レコードは取得できているはず", rs.next(), is(true));

        ResultSetConvertor resultSetConvertor = sut.getResultSetConvertor();
        assertThat(resultSetConvertor.isConvertible(null, 0), is(true));
        assertThat((String)resultSetConvertor.convert(rs, null, 2), is("12345"));
    }

    /**
     * シーケンス採番はサポートしない。
     */
    @Test
    public void testBuildSequenceGeneratorSql() {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("sequence generator is unsupported.");

        sut.buildSequenceGeneratorSql("sequence name");
    }

    /**
     * ページング用のSQL文は変換せず、そのまま返す。
     */
    @Test
    public void testConvertPaginationSql() {
        assertThat(sut.convertPaginationSql("sql", null), is("sql"));
    }

    /**
     * レコード数取得用のSQL文に変換する。
     */
    @Test
    public void testConvertCountSql() {
        assertThat(sut.convertCountSql("sql"), is("SELECT COUNT(*) COUNT_ FROM (sql) SUB_"));
    }

    /**
     * ping用のSQL文はサポートしない。
     */
    @Test
    public void testGetPingSql() {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("unsupported getPingSql.");

        sut.getPingSql();
    }

    private static Matcher<Object> eq(Object expected) {
        return Matchers.is(expected);
    }
}
