package nablarch.core.db.dialect;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

import nablarch.core.db.statement.BasicSqlLoader;
import nablarch.core.db.statement.BasicSqlParameterParserFactory;
import nablarch.core.db.statement.BasicStatementFactory;
import nablarch.core.db.statement.ResultSetConvertor;
import nablarch.core.db.statement.SelectOption;
import nablarch.core.db.statement.StatementFactory;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.DbTestRule;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;


/**
 * {@link PostgreSQLDialect}のテストクラス。
 *
 * @author hisaaki shioiri
 */
@TargetDb(include = TargetDb.Db.POSTGRE_SQL)
@RunWith(DatabaseTestRunner.class)
public class PostgreSQLDialectTest {

    @Rule
    public DbTestRule dbTestRule = new DbTestRule();

    /** テスト対象 */
    private final PostgreSQLDialect sut = new PostgreSQLDialect();

    /** Native Connection */
    private Connection connection;

    @BeforeClass
    public static void setUpClass() {
        VariousDbTestHelper.createTable(DialectEntity.class);
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * {@link PostgreSQLDialect#supportsIdentity()}のテスト。
     * <p/>
     * PostgreSQLでは、IDENTITYをサポートするので{@code true}が返される。
     */
    @Test
    public void supportsIdentity() throws Exception {
        assertThat("IDENTITYがサポートされる。", sut.supportsIdentity(), is(true));
    }

    @Test
    public void supportsIdentityWithBatchInsert() {
        assertThat("PostgreSQLではbatch insert & identityが使えるのでtrueが返される",
                sut.supportsIdentityWithBatchInsert(), is(true));
    }

    /**
     * {@link PostgreSQLDialect#supportsSequence()}のテスト。
     * <p/>
     * {@link true}がかえされること。
     */
    @Test
    public void supportsSequence() throws Exception {
        assertThat("シーケンスがサポートされる", sut.supportsSequence(), is(true));
    }

    /**
     * {@link PostgreSQLDialect#isDuplicateException(SQLException)}のテスト。
     * <p/>
     * {@literal 23505:unique_violation}の場合のみ一意制約違反として判断されること。
     */
    @Test
    public void isDuplicateException() throws Exception {
        assertThat("SQLStateが23505の場合は一意成約違反",
                sut.isDuplicateException(new SQLException("", "23505")), is(true));

        assertThat("SQLStateが23505以外の場合は、一意制約違反ではない",
                sut.isDuplicateException(new SQLException("", "23504")), is(false));
        assertThat("エラーコードが23505は、一意制約違反ではない",
                sut.isDuplicateException(new SQLException("", "", 23505)), is(false));
    }

    /**
     * {@link PostgreSQLDialect#supportsOffset()}のテスト。
     * <p/>
     * PostgreSQLでは、offsetが使えるので{@code true}がかえる。
     */
    @Test
    public void supportsOffset() throws Exception {
        assertThat("trueがかえされること", sut.supportsOffset(), is(true));
    }

    /**
     * {@link PostgreSQLDialect#isTransactionTimeoutError(SQLException)}のテスト。
     * <p/>
     * SQLStateが、57014か55P03の場合にタイムアウト対象例外となる。
     */
    @Test
    public void isTransactionTimeoutError() throws Exception {

        assertThat("SQLStateが57014なのでトランザクション対象の例外",
                sut.isTransactionTimeoutError(new SQLException("", "57014")), is(true));

        assertThat("ロックタイムアウトのSQLStateが55P03は対象外",
                sut.isTransactionTimeoutError(new SQLException("", "55P03")), is(false));

        assertThat("エラーコードが57014は対象外", sut.isTransactionTimeoutError(new SQLException("", "", 57014)), is(false));
    }

    /**
     * {@link PostgreSQLDialect#getResultSetConvertor()} のテスト。
     * 取得したConvertorを使って、値の取得ができること。
     */
    @Test
    public void getResultSetConvertor() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, Calendar.MARCH, 9, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        Date date = calendar.getTime();
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        VariousDbTestHelper.setUpTable(
                new DialectEntity(1L, "12345", 100, 1234554321L, date, new BigDecimal("12345.54321"), timestamp,
                        new byte[] {0x00, 0x50, (byte) 0xFF}));
        connection = VariousDbTestHelper.getNativeConnection();
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            statement = connection.prepareStatement(
                    "SELECT ENTITY_ID, STR, NUM, BIG_INT, DECIMAL_COL, DATE_COL, TIMESTAMP_COL, BINARY_COL FROM DIALECT WHERE ENTITY_ID = ?");
            statement.setLong(1, 1L);
            rs = statement.executeQuery();

            assertThat("1レコードは取得できているはず", rs.next(), is(true));

            final ResultSetConvertor convertor = sut.getResultSetConvertor();

            final ResultSetMetaData meta = rs.getMetaData();
            final int columnCount = meta.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                assertThat("変換するか否かの結果は全てtrue", convertor.isConvertible(meta, i), is(true));
            }

            assertThat("文字列はStringで取得できる", (String) convertor.convert(rs, meta, 2), is("12345"));
            assertThat("数値型はIntegerで取得できる", (Integer) convertor.convert(rs, meta, 3), is(Integer.valueOf("100")));
            assertThat("10桁以上の数値型はLongで取得できる", (Long) convertor.convert(rs, meta, 4), is(Long.valueOf("1234554321")));
            assertThat("小数部ありはBigDecimalで取得できる", (BigDecimal) convertor.convert(rs, meta, 5), is(new BigDecimal(
                    "12345.54321")));
            assertThat("DATE型はDateで取得できる", (Date) convertor.convert(rs, meta, 6), is(date));
            assertThat("TIMESTAMP型はTimestampで取得できる", (Timestamp) convertor.convert(rs, meta, 7), is(timestamp));

            // binaryはbyte[]で取得される
            final byte[] bytes = (byte[]) convertor.convert(rs, meta, 8);
            assertThat("値が取得出来ていること", bytes, is(new byte[] {0x00, 0x50, (byte) 0xFF}));
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (statement != null) {
                statement.close();
            }
        }
    }

    /**
     * {@link PostgreSQLDialect#convertPaginationSql(String, SelectOption)}のテスト。
     */
    @Test
    public void convertPaginationSql() throws Exception {

        assertThat("offsetとlimitを使ったレコード数フィルタのSQL文に変換されること",
                sut.convertPaginationSql("select * from dual", new SelectOption(5, 10)),
                is("select * from dual offset 4 limit 10"));

        assertThat("offsetを使った読み飛ばしレコード数が設定されたSQL文に変換されること",
                sut.convertPaginationSql(
                        "SELECT HOGE, FUGA FROM HOGE_TABLE INNER JOIN FUGA_TABLE ON HOGE_TABLE.ID = FUGA_TABLE.HOGE_ID ORDER BY HOGE_TABLE.ID, HOGE_TABLE.NAME",
                        new SelectOption(50, 0)),
                is("SELECT HOGE, FUGA FROM HOGE_TABLE INNER JOIN FUGA_TABLE ON HOGE_TABLE.ID = FUGA_TABLE.HOGE_ID ORDER BY HOGE_TABLE.ID, HOGE_TABLE.NAME offset 49"));

        assertThat("limitを使った、取得レコード数が設定されたSQL文に変換されること",
                sut.convertPaginationSql(
                        "SELECT HOGE, FUGA FROM HOGE_TABLE INNER JOIN FUGA_TABLE ON HOGE_TABLE.ID = FUGA_TABLE.HOGE_ID ORDER BY HOGE_TABLE.ID, HOGE_TABLE.NAME",
                        new SelectOption(0, 25)
                ),
                is("SELECT HOGE, FUGA FROM HOGE_TABLE INNER JOIN FUGA_TABLE ON HOGE_TABLE.ID = FUGA_TABLE.HOGE_ID ORDER BY HOGE_TABLE.ID, HOGE_TABLE.NAME limit 25"));
    }

    /**
     * {@link PostgreSQLDialect#convertPaginationSql(String, SelectOption)}で生成したSQL文が実行できること。
     * <p/>
     * offsetのみを指定した場合
     */
    @Test
    public void convertPaginationSql_executeOffsetOnly() throws Exception {
        VariousDbTestHelper.delete(DialectEntity.class);
        for (int i = 0; i < 100; i++) {
            VariousDbTestHelper.insert(new DialectEntity((long) i + 1, "name_" + i));
        }
        connection = VariousDbTestHelper.getNativeConnection();

        String sql = "select entity_id, str from dialect where str like ? order by entity_id";
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            statement = connection.prepareStatement(
                    sut.convertPaginationSql(sql, new SelectOption(50, 0)));
            statement.setString(1, "name%");

            rs = statement.executeQuery();
            int index = 49;
            while (rs.next()) {
                index++;
                assertThat(rs.getLong(1), is((long) index));
            }
            assertThat("最後のレコード番号", index, is(100));
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (statement != null) {
                statement.close();
            }
        }
    }

    /**
     * {@link PostgreSQLDialect#convertPaginationSql(String, SelectOption)}で生成したSQL文が実行できること。
     * <p/>
     * limitのみを指定した場合
     */
    @Test
    public void convertPaginationSql_executeLimitOnly() throws Exception {
        VariousDbTestHelper.delete(DialectEntity.class);
        for (int i = 0; i < 100; i++) {
            VariousDbTestHelper.insert(new DialectEntity((long) i + 1, "name_" + i));
        }
        connection = VariousDbTestHelper.getNativeConnection();

        String sql = "select entity_id, str from dialect where str like ? order by entity_id";
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            statement = connection.prepareStatement(
                    sut.convertPaginationSql(sql, new SelectOption(0, 25)));
            statement.setString(1, "name%");

            rs = statement.executeQuery();
            int index = 0;
            while (rs.next()) {
                index++;
                assertThat(rs.getLong(1), is((long) index));
            }
            assertThat("取得件数は25であること", index, is(25));
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (statement != null) {
                statement.close();
            }
        }
    }

    /**
     * {@link PostgreSQLDialect#convertPaginationSql(String, SelectOption)}で生成したSQL文が実行できること。
     * <p/>
     * offsetとlimitの両方を指定
     */
    @Test
    public void convertPaginationSql_executeOffsetAndLimit() throws Exception {
        VariousDbTestHelper.delete(DialectEntity.class);
        for (int i = 0; i < 100; i++) {
            VariousDbTestHelper.insert(new DialectEntity((long) i + 1, "name_" + i));
        }
        connection = VariousDbTestHelper.getNativeConnection();

        String sql = "select entity_id, str from dialect where str like ? order by entity_id";
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            statement = connection.prepareStatement(
                    sut.convertPaginationSql(sql, new SelectOption(31, 15)));
            statement.setString(1, "name%");

            rs = statement.executeQuery();
            int index = 30;
            while (rs.next()) {
                index++;
                assertThat(rs.getLong(1), is((long) index));
            }
            assertThat("最後に取得されたレコードの番号は45であること", index, is(45));
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (statement != null) {
                statement.close();
            }
        }
    }

    /**
     * {@link PostgreSQLDialect#buildSequenceGeneratorSql(String)}のテスト。
     */
    @Test
    public void buildSequenceGeneratorSql() throws Exception {
        assertThat(sut.buildSequenceGeneratorSql("sequence_name"), is("select nextval('sequence_name')"));
    }

    /**
     * {@link PostgreSQLDialect#convertCountSql(String)}のテスト。
     */
    @Test
    public void convertCountSql() throws Exception {
        final String actual = sut.convertCountSql("SELECT * FROM DUAL");
        assertThat("変換されていること", actual, is("SELECT COUNT(*) COUNT_ FROM (SELECT * FROM DUAL) SUB_"));
    }

    /**
     * {@link PostgreSQLDialect#convertCountSql(String)}で変換したSQL文が実行可能であることを確認する。
     */
    @Test
    public void convertCountSql_execute() throws Exception {
        VariousDbTestHelper.delete(DialectEntity.class);
        for (int i = 0; i < 100; i++) {
            VariousDbTestHelper.insert(new DialectEntity((long) i + 1, "name_" + i));
        }
        connection = VariousDbTestHelper.getNativeConnection();
        String sql = "select entity_id, str from dialect where str like ? order by entity_id";
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            statement = connection.prepareStatement(sut.convertCountSql(sql));
            statement.setString(1, "name_3%");
            rs = statement.executeQuery();

            assertThat(rs.next(), is(true));
            assertThat(rs.getInt(1), is(11));       // name_3とname_30〜name_39の11件が取得されるはず
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (statement != null) {
                statement.close();
            }
        }
    }

    /**
     * {@link PostgreSQLDialect#convertCountSql(String, Object, StatementFactory)}のテスト。
     */
    @Test
    public void convertCountSqlFromSqlId() throws Exception {
        BasicStatementFactory statementFactory = new BasicStatementFactory();
        statementFactory.setSqlParameterParserFactory(new BasicSqlParameterParserFactory());
        statementFactory.setSqlLoader(new BasicSqlLoader());

        String actual = sut.convertCountSql("nablarch.core.db.dialect.PostgreSQLDialectTest#SQL001", null, statementFactory);
        assertThat(actual, is("SELECT COUNT(*) COUNT_ FROM (select * from hog_table order by id, name) SUB_"));
    }

    /**
     * {@link PostgreSQLDialect#convertCountSql(String, Object, StatementFactory)}で変換したSQL文が実行可能であることを確認する。
     */
    @Test
    public void convertCountSqlFromSqlId_execute() throws Exception {
        VariousDbTestHelper.delete(DialectEntity.class);
        for (int i = 0; i < 100; i++) {
            VariousDbTestHelper.insert(new DialectEntity((long) i + 1, "name_" + i));
        }
        connection = VariousDbTestHelper.getNativeConnection();
        BasicStatementFactory statementFactory = new BasicStatementFactory();
        statementFactory.setSqlParameterParserFactory(new BasicSqlParameterParserFactory());
        statementFactory.setSqlLoader(new BasicSqlLoader());

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            statement =
                    connection.prepareStatement(sut.convertCountSql("nablarch.core.db.dialect.PostgreSQLDialectTest#SQL002", null, statementFactory));
            statement.setString(1, "name_3%");
            rs = statement.executeQuery();

            assertThat(rs.next(), is(true));
            assertThat(rs.getInt(1), is(11));       // name_3とname_30〜name_39の11件が取得されるはず
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (statement != null) {
                statement.close();
            }
        }
    }

    /**
     * {@link PostgreSQLDialect#getPingSql()}のテスト。
     */
    @Test
    public void getPingSql() throws Exception {
        final String pingSql = sut.getPingSql();
        assertThat(pingSql, is("select 1"));

        connection = VariousDbTestHelper.getNativeConnection();

        final PreparedStatement statement = connection.prepareStatement(pingSql);
        final ResultSet rs = statement.executeQuery();
        assertThat(rs, is(notNullValue()));
        rs.close();
    }
}

