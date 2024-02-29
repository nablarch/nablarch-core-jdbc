package nablarch.core.db.statement;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import jakarta.persistence.Transient;
import nablarch.core.db.DbAccessException;
import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.connection.BasicDbConnection;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.db.dialect.Dialect;
import nablarch.core.db.statement.entity.ClobColumn;
import nablarch.core.db.statement.entity.TextColumn;
import nablarch.core.db.statement.exception.SqlStatementException;
import nablarch.core.db.util.DbUtil;
import nablarch.core.exception.IllegalOperationException;
import nablarch.core.log.Logger;
import nablarch.core.repository.ObjectLoader;
import nablarch.core.repository.SystemRepository;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import nablarch.test.support.log.app.OnMemoryLogWriter;
import nablarch.test.support.reflection.ReflectionUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link BasicSqlPStatement}のテストクラス。
 * サブクラスにて、{@link #createConnectionFactory}を実装し、
 * 生成するファクトリを切り替えることで、
 * 様々な組み合わせで、本クラスで用意されたテストメソッドを実行することができる。
 * これにより{@link BasicSqlPStatement}サブクラスの動作がスーパクラスと同等であることが
 * 確認できる。
 *
 * @author T.Kawasaki
 */
public abstract class BasicSqlPStatementTestLogic {

    @Entity
    @Table(name="statement_test_sqlserver")
    private static class SqlServerTestEntity {
        @Id
        @Column(name = "id", columnDefinition = "bigint identity")
        public Long id;

        @Column(name = "name", columnDefinition = "nvarchar(100)")
        public String name;
    }

    @Entity
    @Table(name = "statement_test_table")
    public static class TestEntity {

        @Id
        @Column(name = "entity_id")
        public String id;

        @Column(name = "varchar_col", columnDefinition = "varchar(50)")
        public String varcharCol;

        @Column(name = "long_col", length = 20)
        public Long longCol;

        @Column(name = "date_col", columnDefinition = "date")
        @Temporal(TemporalType.DATE)
        public Date dateCol;

        @Column(name = "timestamp_col")
        public Timestamp timestampCol;

        @Column(name = "time_col")
        public Time timeCol;

        @Column(name = "decimal_col", length = 10, scale = 2)
        public BigDecimal decimalCol;

        @Column(name = "binary_col")
        public byte[] binaryCol;

        @Column(name = "integer_col", length = 9)
        public Integer integerCol;

        @Column(name = "float_col", precision = 2, scale = 1)
        public Float floatCol;

        @Column(name = "boolean_col")
        public Boolean booleanCol;

        @Column(name = "local_date_col", columnDefinition = "date")
        public LocalDate localDateCol;

        @Column(name = "local_date_time_col", columnDefinition = "timestamp")
        public LocalDateTime localDateTimeCol;

        @Transient
        public String[] varchars;

        
        public TestEntity() {
        }

        public TestEntity(String id, String varcharCol, Long longCol, Date dateCol, Integer integerCol, Float floatCol) {
            this.id = id;
            this.varcharCol = varcharCol;
            this.longCol = longCol;
            this.dateCol = dateCol;
            this.integerCol = integerCol;
            this.floatCol = floatCol;
        }

        public String getId() {
            return id;
        }

        public String getVarcharCol() {
            return varcharCol;
        }

        public Long getLongCol() {
            return longCol;
        }

        public Date getDateCol() {
            return dateCol;
        }

        public Timestamp getTimestampCol() {
            return timestampCol;
        }

        public Time getTimeCol() {
            return timeCol;
        }

        public BigDecimal getDecimalCol() {
            return decimalCol;
        }

        public byte[] getBinaryCol() {
            return binaryCol;
        }

        public Integer getIntegerCol() {
            return integerCol;
        }

        public Float getFloatCol() {
            return floatCol;
        }

        public Boolean getBooleanCol() {
            return booleanCol;
        }

        public LocalDate getLocalDateCol() {
            return localDateCol;
        }

        public LocalDateTime getLocalDateTimeCol() {
            return localDateTimeCol;
        }

        public String[] getVarchars() {
            return varchars;
        }
    }

    /**
     * fieldアクセスして検索、更新を行うためのクラス
     */
    public static class TestFieldEntity {
        public String id;
        public String varcharCol;
        public Long longCol;
        public Date dateCol;
        public Timestamp timestampCol;
        public Time timeCol;
        public BigDecimal decimalCol;
        public byte[] binaryCol;
        public Integer integerCol;
        public Float floatCol;
        public Boolean booleanCol;
        public LocalDate localDateCol;
        public LocalDateTime localDateTimeCol;
        public String[] varchars;
    }

    /**
     * テスト対象となる{@link nablarch.core.db.connection.ConnectionFactory}を生成する。
     * サブクラスにて、生成するファクトリを切り替えることで、
     * 様々な組み合わせで、本クラスで用意されたテストメソッドを実行することができる。
     *
     * @return テスト対象となるファクトリ
     */
    protected abstract ConnectionFactory createConnectionFactory();

    /** テスト用のコネクション */
    protected TransactionManagerConnection dbCon;

    @BeforeClass
    public static void dbSetup() {
        VariousDbTestHelper.createTable(TestEntity.class);
    }

    @After
    public void terminateDb() {
        dbCon.terminate();
        SystemRepository.clear();
    }

    @Before
    public void setUpTestData() throws Exception {
        dbCon = createConnectionFactory().getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        Calendar calendar = Calendar.getInstance();
        calendar.set(2010, 6, 10);
        VariousDbTestHelper.setUpTable(
                new TestEntity("10001", "a", 10000L, calendar.getTime(), 1, 1.1f),
                new TestEntity("10002", "b", 20000L, calendar.getTime(), 2, 2.2f),
                new TestEntity("10003", "c_\\(like検索用)", 30000L, calendar.getTime(), 3, 3.3f),
                new TestEntity("10004", "\uD840\uDC0B", 40000L, calendar.getTime(), 4, 4.4f)
        );
    }

    @Before
    public void clearLog() {
        OnMemoryLogWriter.clear();
    }

    /**
     * {@link BasicSqlPStatement#executeQuery()}で条件パラメータ有りのSQLログが出力されること
     */
    @Test
    public void executeQuery_writeSqlLog() throws Exception {
        SqlPStatement statement = dbCon.prepareStatement(
                "SELECT ENTITY_ID FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        statement.setString(1, "10001");
        statement.executeQuery();

        assertLog("開始ログ", Pattern.compile(
                "\\Qnablarch.core.db.statement.BasicSqlPStatement#executeQuery "
                        + "SQL = [SELECT ENTITY_ID FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?]\\E"));
        assertLog("パラメータ", Pattern.compile("Parameters" + Logger.LS
                + "\t01 = \\Q[10001]\\E"));
        assertLog("終了ログ", Pattern.compile("\texecute time\\(ms\\) = \\[\\d+\\]"));
    }

    /**
     * {@link BasicSqlPStatement#executeQuery()}で条件パラメータ(サロゲートペアを含む文字列)有りのSQLログが出力されること。
     */
    @Test
    public void executeQuery_writeSqlLog_withSurrogatePair() throws Exception {
        SqlPStatement statement = dbCon.prepareStatement(
                "SELECT ENTITY_ID FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL = ?");
        statement.setString(1, "\uD840\uDC0B");
        statement.executeQuery();

        assertLog("開始ログ", Pattern.compile(
                "\\Qnablarch.core.db.statement.BasicSqlPStatement#executeQuery "
                        + "SQL = [SELECT ENTITY_ID FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL = ?]\\E"));
        assertLog("パラメータ", Pattern.compile("Parameters" + Logger.LS
                + "\t01 = \\Q[\uD840\uDC0B]\\E"));
        assertLog("終了ログ", Pattern.compile("\texecute time\\(ms\\) = \\[\\d+\\]"));
    }


    @Test
    public void executeQuery_writeSqlLog_withOption() throws Exception {
        setDialect(dbCon, new OffsetSupportDialcet("SELECT 'TEST_CODE' FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?"));
        SqlPStatement statement = dbCon.prepareStatement(
                "SELECT ENTITY_ID FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?", new SelectOption(2, 3));
        statement.setString(1, "10001");
        statement.executeQuery();
        assertLog("開始ログ", Pattern.compile(
                "\\Qnablarch.core.db.statement.BasicSqlPStatement#executeQuery "
                        + "SQL = [SELECT 'TEST_CODE' FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?]\\E"));
        assertLog("パラメータ", Pattern.compile("Parameters" + Logger.LS
                + "\t01 = \\Q[10001]\\E"));
        assertLog("終了ログ", Pattern.compile("\texecute time\\(ms\\) = \\[\\d+\\]"));
    }

    /**
     * {@link BasicSqlPStatement#executeQuery()}で条件指定した場合。
     */
    @Test
    public void executeQuery_withCondition() throws Exception {

        final SqlPStatement statement = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        statement.setString(1, "10002");
        final ResultSetIterator actual = statement.executeQuery();

        // ---------------------------- first row
        assertThat("1レコード目は存在している", actual.next(), is(true));
        final SqlRow row = actual.getRow();
        assertThat("条件で絞り込んだレコードが取得出来ていること", row.getString("entityId"), is("10002"));
        assertThat("属性も取得できる -> varchar", row.getString("varcharCol"), is("b"));

        // ---------------------------- next row
        assertThat("2レコード目は存在していない", actual.next(), is(false));
    }

    /**
     * 生成時に検索範囲を指定した場合に、指定した検索範囲で検索することのテスト。
     */
    @Test
    public void executeQuery_WithSelectOption() throws Exception {
        final SqlPStatement statement = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID >= ? ORDER BY ENTITY_ID", new SelectOption(2, 3));
        statement.setString(1, "1");
        final ResultSetIterator actual = statement.executeQuery();
        // -- first row (10002)
        assertThat("1件目が取得できる", actual.next(), is(true));
        assertThat(actual.getRow().getString("entityId"), is("10002"));
        // -- second result (10003)
        assertThat("2件目が取得できる", actual.next(), is(true));
        assertThat(actual.getRow().getString("entityId"), is("10003"));
        assertThat("3件目が取得できる", actual.next(), is(true));
        assertThat(actual.getRow().getString("entityId"), is("10004"));
        assertThat("4件目は取得できない", actual.next(), is(false));
    }

    /**
     * 生成時に検索範囲を指定した場合に、指定した検索範囲で検索することのテスト。
     * DialectがOffsetをサポートしていない場合のテスト。
     */
    @Test
    public void executeQuery_WithSelectOptionUnSupportOffset() throws Exception {
        setDialect(dbCon, new DefaultDialect());
        final SqlPStatement statement = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID >= ? ORDER BY ENTITY_ID", new SelectOption(2, 3));
        statement.setString(1, "1");
        final ResultSetIterator actual = statement.executeQuery();
        // -- first row (10002)
        assertThat(actual.next(), is(true));
        SqlRow row = actual.getRow();
        assertThat(row.getString("entityId"), is("10002"));
        // -- second result (10003)
        assertThat(actual.next(), is(true));
        row = actual.getRow();
        assertThat(row.getString("entityId"), is("10003"));
        assertThat(actual.next(), is(true));
        row = actual.getRow();
        assertThat(row.getString("entityId"), is("10004"));
        assertThat(actual.next(), is(false));
    }

    /**
     * {@link BasicSqlPStatement#executeQuery()}で条件なしの場合。
     */
    @Test
    public void executeQuery_withoutCondition() throws Exception {
        final SqlPStatement statement = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE ORDER BY ENTITY_ID");
        final ResultSetIterator actual = statement.executeQuery();
        assertThat("1レコード目は存在する", actual.next(), is(true));
        assertThat("1レコード目:主キー", actual.getRow()
                .getString("entityId"), is("10001"));
        assertThat("2レコード目は存在する", actual.next(), is(true));
        assertThat("2レコード目:主キー", actual.getRow()
                .getString("entityId"), is("10002"));
        assertThat("3レコード目は存在する", actual.next(), is(true));
        assertThat("3レコード目:主キー", actual.getRow()
                .getString("entityId"), is("10003"));
        assertThat("4レコード目は存在する", actual.next(), is(true));
        assertThat("4レコード目:主キー", actual.getRow()
                .getString("entityId"), is("10004"));
        assertThat("5レコード目は存在しない", actual.next(), is(false));
    }

    /**
     * {@link BasicSqlPStatement#executeQuery()}で{@link BasicSqlPStatement#setMaxRows(int)}の設定値が有効になっていること
     * <p/>
     * ※3レコード存在しているテーブルの全レコード検索で、{@link BasicSqlPStatement#setMaxRows(int)}には2を指定
     */
    @Test
    public void executeQuery_withMaxRows() throws Exception {
        final SqlPStatement statement = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        statement.setMaxRows(2);
        final ResultSetIterator rows = statement.executeQuery();

        assertThat("1レコード目はあり", rows.next(), is(true));
        assertThat("2レコード目はあり", rows.next(), is(true));
        assertThat("3レコード目はなし", rows.next(), is(false));
    }

    /**
     * {@link ResultSetIterator}に正しく参照を渡していることの確認。
     */
    @Test
    public void testSetStatement() throws Exception {
        final SqlPStatement statement = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        ResultSetIterator iterator = statement.executeQuery();
        assertThat(iterator.getStatement(), sameInstance((SqlStatement) statement));
    }

    /**
     * {@link BasicSqlPStatement#executeQuery()}でSQLExceptionが発生するケース。
     */
    @Test(expected = SqlStatementException.class)
    public void executeQuery_SQLException() throws Exception {
        final PreparedStatement mock = mock(PreparedStatement.class);
        
        when(mock.executeQuery()).thenThrow(new SQLException("executeQuery error.", "code", 100));
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mock);
        sut.executeQuery();
    }

    /**
     * {@link BasicSqlPStatement#executeUpdate()}のログ出力のテスト。
     */
    @Test
    public void executeUpdate_writeSqlLog() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(mockStatement.executeUpdate()).thenReturn(5);
        final SqlPStatement sut = dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE SET VARCHAR_COL = ? WHERE ENTITY_ID = ?");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setString(1, "あいうえお\uD840\uDC0B");
        sut.setString(2, "10001");
        sut.executeUpdate();

        assertLog("開始ログ", Pattern.compile(
                "\\Qnablarch.core.db.statement.BasicSqlPStatement#executeUpdate SQL = ["
                        + "UPDATE STATEMENT_TEST_TABLE SET VARCHAR_COL = ? WHERE ENTITY_ID = ?]\\E"));
        assertLog("パラメータ", Pattern.compile(
                "Parameters" + Logger.LS
                        + "\t\\Q01 = [あいうえお\uD840\uDC0B]" + Logger.LS + "\t02 = [10001]\\E"));

        assertLog("終了ログ", Pattern.compile(
                "nablarch.core.db.statement.BasicSqlPStatement#executeUpdate"
                        + Logger.LS + "\texecute time\\(ms\\) = \\[[0-9]+\\] update count = \\[5\\]"));
    }

    /**
     * {@link BasicSqlPStatement#executeUpdate()}の条件がある場合の実行テスト。
     */
    @Test
    public void executeUpdate_withCondition() throws Exception {

        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, 1, 16, 0, 0, 0);
        Date testDate = calendar.getTime();

        final SqlPStatement statement = dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE"
                        + " SET VARCHAR_COL = ?,"
                        + " LONG_COL = ?,"
                        + " DATE_COL = ? WHERE ENTITY_ID = ?");

        statement.setString(1, "あいうえお");
        statement.setLong(2, 12345L);
        statement.setDate(3, new java.sql.Date(testDate.getTime()));
        statement.setString(4, "10002");
        final int updated = statement.executeUpdate();
        dbCon.commit();

        assertThat("1レコード更新される", updated, is(1));
        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "10002");
        assertThat("更新されていること:varchar_col", actual.varcharCol, is("あいうえお"));
        assertThat("更新されていること:long_col", actual.longCol, is(12345L));
    }

    /**
     * {@link BasicSqlPStatement#executeUpdate()}の条件がない場合のテスト。
     */
    @Test
    public void executeUpdate_withoutCondition() throws Exception {

        final SqlPStatement statement = dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE SET LONG_COL = LONG_COL + 1");
        final int updated = statement.executeUpdate();

        assertThat("レコードが全て更新される", updated, is(4));

        dbCon.commit();

        final List<TestEntity> actual = VariousDbTestHelper.findAll(TestEntity.class, "id");
        long[] expected = {10001, 20001, 30001, 40001};
        int index = 0;
        for (TestEntity entity : actual) {
            assertThat("更新されている", entity.longCol, is(expected[index++]));
        }
    }

    /**
     * {@link BasicSqlPStatement#executeUpdate()}でSQLExceptionが発生するケース。
     */
    @Test(expected = SqlStatementException.class)
    public void executeUpdate_SQLException() throws Exception {
        final PreparedStatement mock = mock(PreparedStatement.class);

        when(mock.executeUpdate()).thenThrow(new SQLException("executeUpdate error.", "code", 101));
        final SqlPStatement sut = dbCon.prepareStatement("UPDATE STATEMENT_TEST_TABLE SET LONG_COL = 1");
        ReflectionUtil.setFieldValue(sut, "statement", mock);
        sut.executeUpdate();
    }

    /**
     * {@link BasicSqlPStatement#execute()}で更新処理を実行するケース
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.SQL_SERVER)
    public void execute() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID,VARCHAR_COL) VALUES (?,?)");
        sut.setString(1, "12345");
        sut.setString(2, "\uD840\uDC0B");
        final boolean result = sut.execute();
        assertThat(result, is(false));
        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "12345");
        assertThat(actual.getId(), is("12345"));
        assertThat(actual.getVarcharCol(), is("\uD840\uDC0B"));
    }
    
    /**
     * {@link BasicSqlPStatement#execute()}のSQLログのテスト。
     */
    @Test
    public void execute_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID,VARCHAR_COL) VALUES (?,?)");
        sut.setString(1, "12345");
        sut.setString(2, "\uD840\uDC0B");
        sut.execute();

        assertLog("開始ログ",
                Pattern.compile("\\Qnablarch.core.db.statement.BasicSqlPStatement#execute"
                        + " SQL = [INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID,VARCHAR_COL) VALUES (?,?)]\\E"));
        assertLog("パラメータログ",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [12345]" + Logger.LS
                        + "\t02 = [\uD840\uDC0B]\\E"
                ));
        assertLog("実行ログ",
                Pattern.compile("\\Qnablarch.core.db.statement.BasicSqlPStatement#execute" + Logger.LS
                        + "\texecute time(ms)\\E = \\[[0-9]+\\]"));
    }

    /**
     * {@link BasicSqlPStatement#execute()}でselectを実行するケース
     */
    @Test
    public void execute_select() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "select * from STATEMENT_TEST_TABLE where ENTITY_ID = ?");
        sut.setString(1, "10001");
        final boolean result = sut.execute();
        assertThat("検索処理なのでtrue", result, is(true));

        final ResultSet rs = sut.getResultSet();
        try {
            assertThat(rs, is(notNullValue()));
        } finally {
            rs.close();
        }
    }

    /**
     * {@link BasicSqlPStatement#execute()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void execute_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(mockStatement.execute()).thenThrow(new SQLException("execute error"));
        final SqlPStatement sut = dbCon.prepareStatement(
                "select * from STATEMENT_TEST_TABLE where ENTITY_ID = ?");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setString(1, "10001");
        sut.execute();
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}のSQLログのテスト。
     *
     * @throws Exception
     */
    @Test
    public void retrieve_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        sut.setFetchSize(100);
        sut.setQueryTimeout(123);

        sut.retrieve();

        assertLog("開始ログ", Pattern.compile(
                "nablarch.core.db.statement.BasicSqlPStatement#retrieve SQL = \\Q[SELECT * FROM STATEMENT_TEST_TABLE]\\E"
                        + Logger.LS + "\tstart position = \\Q[1] size = [0] queryTimeout = [123] fetchSize = [100]\\E"
        ));
        assertLog("パラメータログ", Pattern.compile("Parameters$"));
        assertLog("終了ログ", Pattern.compile("nablarch.core.db.statement.BasicSqlPStatement#retrieve"
                        + Logger.LS
                        + "\texecute time\\(ms\\) = \\[[0-9]+\\] retrieve time\\(ms\\) = \\[[0-9]+\\] count = \\[4\\]"
        ));
    }

    /**
     * ステートメント生成時に設定された検索処理のオプションを指定し、Dialectが変換する場合の
     * {@link BasicSqlPStatement#retrieve()}のSQLログのテスト。
     *
     * @throws Exception
     */
    @Test
    public void retrieve_writeSqlLogWithOptionOffsetSupport() throws Exception {
        final String convertedSql = "SELECT 'TEST_CODE' FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID IN ('10002','10003')";
        setDialect(dbCon, new OffsetSupportDialcet(convertedSql));
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE", new SelectOption(2, 10));
        sut.setQueryTimeout(123);
        sut.setFetchSize(100);
        sut.retrieve();
        assertLog("開始ログ", Pattern.compile(
                "nablarch.core.db.statement.BasicSqlPStatement#retrieve SQL = \\Q["+ convertedSql +"]\\E"
                        + Logger.LS + "\tstart position = \\Q[1] size = [0] queryTimeout = [123] fetchSize = [100]\\E"
        ));
        assertLog("パラメータログ", Pattern.compile("Parameters$"));
        assertLog("終了ログ", Pattern.compile("nablarch.core.db.statement.BasicSqlPStatement#retrieve"
                        + Logger.LS + "\texecute time\\(ms\\) = \\[[0-9]+\\] retrieve time\\(ms\\) = \\[[0-9]+\\] count = \\[2\\]"
        ));
    }

    /**
     * ステートメント生成時に設定された検索処理のオプションを指定した場合のDialectが変換しない場合の
     * {@link BasicSqlPStatement#retrieve()}のSQLログのテスト。
     * @throws Exception
     */
    @Test
    public void retrieve_writeSqlLogWithOptionOffsetUnSupport() throws Exception {
        String sql = "SELECT * FROM STATEMENT_TEST_TABLE";
        setDialect(dbCon, new DefaultDialect());
        final SqlPStatement sut = dbCon.prepareStatement(sql, new SelectOption(2, 10));
        sut.setQueryTimeout(123);
        sut.setFetchSize(100); //実行時にページング機能が動作するのでfecthはmaxの10となり、無視されます。
        sut.retrieve();
        assertLog("開始ログ", Pattern.compile(
                "nablarch.core.db.statement.BasicSqlPStatement#retrieve SQL = \\Q["+ sql +"]\\E"
                        + Logger.LS + "\tstart position = \\Q[2] size = [10] queryTimeout = [123] fetchSize = [10]\\E"
        ));
        assertLog("パラメータログ", Pattern.compile("Parameters$"));
        assertLog("終了ログ", Pattern.compile("nablarch.core.db.statement.BasicSqlPStatement#retrieve"
                        + Logger.LS + "\texecute time\\(ms\\) = \\[[0-9]+\\] retrieve time\\(ms\\) = \\[[0-9]+\\] count = \\[3\\]"
        ));
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}で条件ありでデータの取得ができること
     *
     * @throws Exception
     */
    @Test
    public void retrieve_withCondition() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        sut.setString(1, "10001");
        final SqlResultSet actual = sut.retrieve();

        assertThat("1レコード取得できる", actual.size(), is(1));
        assertThat(actual.get(0)
                .getString("entityId"), is("10001"));
        assertThat(actual.get(0)
                .getString("varcharCol"), is("a"));
        assertThat(actual.get(0)
                .getLong("longCol"), is(10000L));
    }

    /**
     * ステートメント生成時に設定された検索処理のオプションが有効になることのテスト。<br />
     * {@link BasicSqlPStatement#retrieve()}で条件ありでデータの取得ができること
     *
     * @throws Exception
     */
    @Test
    public void retrieve_withStatementConditionWithSelectOption() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID >= ? order by ENTITY_ID", new SelectOption(2, 3));
        sut.setString(1, "10001");

        final SqlResultSet actual = sut.retrieve();
        assertThat("3レコード取得できる", actual.size(), is(3));
        assertThat(actual.get(0).getString("entityId"), is("10002"));
        assertThat(actual.get(1).getString("entityId"), is("10003"));
        assertThat(actual.get(2).getString("entityId"), is("10004"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}で条件なしでデータが取得できること
     * @throws Exception
     */
    @Test
    public void retrieve_withoutCondition() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        final SqlResultSet actual = sut.retrieve();

        assertThat("テーブルの全てのレコードが取得できる", actual.size(), is(4));
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}でリミット指定した検索処理ができること
     * @throws Exception
     */
    @Test
    public void retrieve_withLimit() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL < ? ORDER BY ENTITY_ID");
        sut.setLong(1, 50000);

        // この設定は無視される
        sut.setMaxRows(9999);

        final SqlResultSet actual = sut.retrieve(0, 2);
        assertThat("limitの2レコード取得できる", actual.size(), is(2));
        assertThat(actual.get(0)
                .getString("entityId"), is("10001"));
        assertThat(actual.get(1)
                .getString("entityId"), is("10002"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}でオフセットとリミットを指定した検索処理ができること
     * @throws Exception
     */
    @Test
    public void retrieve_withOffsetAndLimit() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL < ? ORDER BY ENTITY_ID");
        sut.setLong(1, 50000);
        final SqlResultSet actual = sut.retrieve(2, 2);
        assertThat("limitの2レコード取得できる", actual.size(), is(2));
        assertThat("2レコード目から取得できる", actual.get(0)
                .getString("entityId"), is("10002"));
        assertThat(actual.get(1)
                .getString("entityId"), is("10003"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}でオフセットが取得可能最大件数より大きい場合
     * レコードは取得されないこと。
     *
     * @throws Exception
     */
    @Test
    public void retrieve_offsetOverRecordCount() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL < ? ORDER BY ENTITY_ID");
        sut.setLong(1, 50000);
        final SqlResultSet actual = sut.retrieve(10, 10);
        assertThat("レコードは取得されない", actual.size(), is(0));
    }

    /**
     * ステートメント生成時に設定された検索処理のオプションが有効になることのテスト。<br />
     * ステートメント作成時の検索範囲通りに取得できること。
     *
     * @throws Exception テスト時の実行時例外
     */
    @Test
    public void retrieve_withLimitOffsetOption() throws Exception {
        // 2件目から2件を取得
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE ORDER BY ENTITY_ID", new SelectOption(2, 2));
        SqlResultSet rs = sut.retrieve();
        assertThat(rs.size(), is(2));
        assertThat(rs.get(0).getString("entity_id"), is("10002"));
        assertThat(rs.get(1).getString("entity_id"), is("10003"));
    }

    /**
     * ステートメント生成時に設定された検索処理オプションで、startポジションの値(検索開始位置だけ)有効な場合のテスト。<br />
     * limitに0以下を指定しても、検索できること。
     *
     * @throws Exception テスト時の実行時例外
     */
    @Test
    public void retrieve_withOffsetOption() throws Exception {
        // 2件目以降を取得
        assertThat(VariousDbTestHelper.findAll(TestEntity.class).size(), is(4));
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE ORDER BY ENTITY_ID", new SelectOption(2, 0));
        SqlResultSet rs = sut.retrieve();
        assertThat(rs.size(), is(3));
        assertThat(rs.get(0).getString("entity_id"), is("10002"));
        assertThat(rs.get(1).getString("entity_id"), is("10003"));
        assertThat(rs.get(2).getString("entity_id"), is("10004"));
    }

    /**
     * ステートメント生成時に設定された検索処理オプションで、limitの値(検索開始位置だけ)有効な場合のテスト。<br />
     * startPos = 1以下にした場合でも、検索上限の設定が有効であること。
     *
     * @throws Exception テスト時の実行時例外
     */
    @Test
    public void retrieve_withLimitOption() throws Exception {
        // 1件目から2件取得
        assertThat(VariousDbTestHelper.findAll(TestEntity.class).size(), is(4));
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE ORDER BY ENTITY_ID", new SelectOption(1, 2));
        SqlResultSet rs = sut.retrieve();
        assertThat(rs.size(), is(2));
        assertThat(rs.get(0).getString("entity_id"), is("10001"));
        assertThat(rs.get(1).getString("entity_id"), is("10002"));
    }

    /**
     * ステートメント生成時に設定された検索処理オプションで、startポジション, limitの値が無効な場合でも、例外が発生すること。
     *
     * @throws Exception
     */
    @Test(expected = IllegalOperationException.class)
    public void retrieve_withIgnoredOption() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE ORDER BY ENTITY_ID", new SelectOption(1, 0));
        sut.retrieve(2, 2);
    }

    /**
     * ステートメント生成時に設定された検索処理オプションを指定して、実行時に範囲指定した場合に例外が発生することを確認する。
     *
     * @throws Exception
     */
    @Test(expected = IllegalOperationException.class)
    public void retrieve_withDuplicatePagenate() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE ORDER BY ENTITY_ID", new SelectOption(2, 2));
        sut.retrieve(2, 2);
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}でSQLExceptionが発生するケース
     * は、SqlStatementExceptionが送出されること。
     *
     * @throws Exception
     */
    @Test(expected = SqlStatementException.class)
    public void retrieve_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(mockStatement.executeQuery()).thenThrow(new SQLException("retrieve error", "", 999));

        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.retrieve();
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}で非キャッチ例外が発生するケースは、
     * その例外がそのまま送出されること。
     *
     * @throws Exception
     */
    @Test(expected = IllegalStateException.class)
    public void retrieve_RuntimeException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class, RETURNS_DEEP_STUBS);

        when(mockStatement.executeQuery().getMetaData()).thenThrow(new IllegalStateException("error"));

        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.retrieve();
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}でError系が発生するケースは、
     * その例外がそのまま送出されること。
     *
     * @throws Exception
     */
    @Test(expected = StackOverflowError.class)
    public void retrieve_Error() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class, RETURNS_DEEP_STUBS);

        when(mockStatement.executeQuery().getMetaData()).thenThrow(new StackOverflowError("error"));

        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.retrieve();
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}でfinallyのリソース開放で例外が発生する場合
     * その例外がそのまま送出されること。
     *
     * @throws Exception
     */
    @Test
    public void retrieve_finallyError() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class, RETURNS_DEEP_STUBS);

        ResultSet rs = mockStatement.executeQuery();
        ResultSetMetaData rsm = mockStatement.executeQuery().getMetaData();
        when(rsm.getColumnCount()).thenReturn(0);
        doThrow(new NullPointerException("null")).when(rs).close(); 

        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        try {
            sut.retrieve();
            fail("");
        } catch (RuntimeException e) {
            assertThat("causeはNullPointerException", e.getCause(), is(instanceOf(NullPointerException.class)));
        }

        assertLog("ログが出力されれる。", Pattern.compile("failed to close result set."));
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}でtryとfinallyの共に例外が発生する場合、
     * tryブロックの例外が送出されfinallyブロックの例外はログ出力されること。
     *
     * @throws Exception
     */
    @Test
    public void retrieve_tryAndFinallyError() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class, RETURNS_DEEP_STUBS);

        ResultSet rs = mockStatement.executeQuery();

        doThrow(new NullPointerException("null error")).when(rs).close();
        when(rs.getMetaData()).thenThrow(new IllegalStateException("error"));
        
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        try {
            sut.retrieve();
            fail("");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is("error"));
        }

        assertLog("ログが出力されれる。", Pattern.compile("failed to close result set."));
    }

    /**
     * {@link BasicSqlPStatement#addBatch()}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void addBatch() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");

        assertThat("addBatch実行前のサイズは0", sut.getBatchSize(), is(0));

        sut.setString(1, "88888");
        sut.addBatch();
        assertThat("addBatchが実行されたのでサイズが増える", sut.getBatchSize(), is(1));

        sut.setString(1, "99999");
        sut.addBatch();
        assertThat("addBatchが実行されたのでサイズが増える", sut.getBatchSize(), is(2));
    }

    /**
     * {@link BasicSqlPStatement#addBatch()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが発生する。
     *
     * @throws Exception
     */
    @Test(expected = DbAccessException.class)
    public void addBatch_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("addBatch error", "", 999)).when(mockStatement).addBatch();

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setString(1, "1");
        sut.addBatch();
    }


    /**
     * {@link BasicSqlPStatement#executeBatch()}でSQLログが出力されること
     */
    @Test
    public void executeBatch_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID,VARCHAR_COL) VALUES (?,?)");
        sut.setString(1, "88888");
        sut.setString(2, "\uD840\uDC0B");
        sut.addBatch();
        sut.setString(1, "99999");
        sut.setString(2, "\uD844\uDE3D");
        sut.addBatch();
        sut.executeBatch();

        assertLog("開始ログ", Pattern.compile(
                "\\Qablarch.core.db.statement.BasicSqlPStatement#executeBatch SQL = ["
                        + "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID,VARCHAR_COL) VALUES (?,?)]\\E"));

        assertLog("パラメータログ", Pattern.compile(
                "Parameters" + Logger.LS
                        + "\t\\Qbatch count = [1]" + Logger.LS + "\t\t01 = [88888]" + Logger.LS + "\t\t02 = [\uD840\uDC0B]" + Logger.LS
                        + "\tbatch count = [2]" + Logger.LS + "\t\t01 = [99999]"+ Logger.LS + "\t\t02 = [\uD844\uDE3D]\\E"
        ));
        assertLog("終了ログ", Pattern.compile(
                "nablarch.core.db.statement.BasicSqlPStatement#executeBatch"
                        + Logger.LS + "\texecute time\\(ms\\) = \\[[0-9]+\\] batch count = \\[2\\]"));
    }

    /**
     * {@link BasicSqlPStatement#executeBatch()}が実行できること。
     */
    @Test
    public void executeBatch() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");
        sut.setString(1, "88888");
        sut.addBatch();
        sut.setString(1, "99999");
        sut.addBatch();
        final int[] result = sut.executeBatch();
        dbCon.commit();

        assertThat("戻り値のサイズは2", result.length, is(2));
        assertThat("executeBatch実行後はバッチサイズは0になる", sut.getBatchSize(), is(0));

        final List<TestEntity> actual = VariousDbTestHelper.findAll(TestEntity.class, "id");
        assertThat("2レコード増えていること", actual.size(), is(6));

        assertThat("登録されていること", actual.get(4).id, is("88888"));
        assertThat("登録されていること", actual.get(5).id, is("99999"));
    }

    /**
     * {@link BasicSqlPStatement#executeBatch()}でSQLExceptionが発生した場合、
     * SqlStatementExceptionが送出されること。
     */
    @Test(expected = SqlStatementException.class)
    public void executeBatch_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(mockStatement.executeBatch()).thenThrow(new SQLException("executeBatch error", "", 999));

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");
        sut.setString(1, "99999");
        sut.addBatch();
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.executeBatch();
    }

    /**
     * {@link BasicSqlPStatement#clearBatch()} のテスト。
     */
    @Test
    public void clearBatch() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");
        sut.setString(1, "99999");
        sut.addBatch();

        assertThat("追加されたのでバッチサイズは1", sut.getBatchSize(), is(1));

        sut.clearBatch();
        assertThat("クリアしたのでバッチサイズは0", sut.getBatchSize(), is(0));
    }

    public static class ParamObject {

        private String id = null;

        public String getId() {
            return id == null ? "12345" : id;
        }
    }

    /**
     * {@link BasicSqlPStatement#clearBatch()} のテスト。
     *
     * パラメータ化されたステートメントオブジェクトでも使用できることを確認する。
     */
    @Test
    public void clearBatch_Parametarized() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (:id)");

        final ParamObject param = new ParamObject();
        sut.addBatchObject(param);

        assertThat("追加されたのでバッチサイズは1", sut.getBatchSize(), is(1));

        sut.clearBatch();
        assertThat("クリアしたのでバッチサイズは0", sut.getBatchSize(), is(0));
    }


    /**
     * {@link BasicSqlPStatement#clearBatch()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void clearBatch_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("clear batch error")).when(mockStatement).clearBatch();

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.clearBatch();
    }

    /**
     * {@link BasicSqlPStatement#setNull(int, int)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void setNull() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, VARCHAR_COL, LONG_COL, DATE_COL, TIMESTAMP_COL, DECIMAL_COL, BINARY_COL, INTEGER_COL) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        sut.setString(1, "99999");
        sut.setNull(2, Types.VARCHAR);
        sut.setNull(3, Types.BIGINT);
        sut.setNull(4, Types.DATE);
        sut.setNull(5, Types.TIMESTAMP);
        sut.setNull(6, Types.DECIMAL);
        sut.setNull(7, Types.BINARY);
        sut.setNull(8, Types.INTEGER);
        final int updated = sut.executeUpdate();

        assertThat("1レコード登録出来ている", updated, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "99999");
        assertThat(actual.varcharCol, nullValue());
        assertThat(actual.longCol, is(nullValue()));
        assertThat(actual.dateCol, nullValue());
        assertThat(actual.timestampCol, nullValue());
        assertThat(actual.decimalCol, nullValue());
        assertThat(actual.binaryCol, nullValue());
        assertThat(actual.integerCol, nullValue());
    }

    /**
     * {@link BasicSqlPStatement#setNull(int, int)}のSQLログのテスト。
     */
    @Test
    public void setNull_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, VARCHAR_COL) "
                        + "VALUES (?, ?)");
        sut.setString(1, "99999");
        sut.setNull(2, Types.VARCHAR);
        sut.executeUpdate();

        assertLog("nullパラメータのログ", Pattern.compile("\\QParameters" + Logger.LS
                + "\t01 = [99999]" + Logger.LS
                + "\t02 = [null]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setNull(int, int)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setNull_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setNull error")).when(mockStatement).setNull(anyInt(), anyInt());

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, VARCHAR_COL) "
                        + "VALUES (?, ?)");

        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setString(1, "99999");
        sut.setNull(2, Types.VARCHAR);
    }

    /**
     * {@link BasicSqlPStatement#setBoolean(int, boolean)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void setBoolean() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE SET BOOLEAN_COL = ? WHERE ENTITY_ID = ?");
        sut.setBoolean(1, true);
        sut.setString(2, "10001");

        final int updated = sut.executeUpdate();

        assertThat("1レコード更新される", updated, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "10001");
        assertThat("trueを設定したので1に更新される", actual.booleanCol, is(true));
    }

    /**
     * {@link BasicSqlPStatement#setBoolean(int, boolean)}のSQLログのテスト。
     */
    @Test
    public void setBoolean_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE SET BOOLEAN_COL = ?");
        sut.setBoolean(1, true);
        sut.executeUpdate();

        assertLog("Booleanパラメータが出力されていること", Pattern.compile(
                "\\QParameters" + Logger.LS
                        + "\t01 = [true]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setBoolean(int, boolean)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setBoolean_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setBoolean error")).when(mockStatement).setBoolean(anyInt(), anyBoolean());
        final SqlPStatement sut = dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE SET VARCHAR_COL = ?, LONG_COL = ?");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setBoolean(2, false);
    }

    /**
     * {@link BasicSqlPStatement#setByte(int, byte)}のテスト。
     */
    @Test
    public void setByte() throws Exception {
        final SqlPStatement sut= dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE SET INTEGER_COL = ?, LONG_COL = ?, VARCHAR_COL = ? WHERE ENTITY_ID = ?");
        sut.setByte(1, (byte) 0x30);
        sut.setByte(2, (byte) 0x31);
        sut.setByte(3, (byte) 0x32);
        sut.setString(4, "10002");

        final int updated = sut.executeUpdate();
        assertThat("1レコード更新される", updated, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "10002");
        assertThat(actual.integerCol, is(0x30));
        assertThat(actual.longCol, is((long) 0x31));
        assertThat(actual.varcharCol, is("50"));        // 0x32 -> 50
    }

    /**
     * {@link BasicSqlPStatement#setByte(int, byte)}のSQLログのテスト。
     */
    @Test
    public void setByte_writeSqlLog() throws Exception {
        final SqlPStatement sut= dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE SET INTEGER_COL = ?, LONG_COL = ?");
        sut.setByte(1, (byte) 0x30);
        sut.setByte(2, (byte) 0x31);
        sut.executeUpdate();

        assertLog("パラメータ情報がログに出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [48]" + Logger.LS
                        + "\t02 = [49]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setByte(int, byte)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setByte_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setByte error.")).when(mockStatement).setByte(anyInt(), anyByte());
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT '1' FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setByte(1, (byte) 0x00);
    }

    /**
     * {@link BasicSqlPStatement#setShort(int, short)}のテスト。
     */
    @Test
    public void setShort() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL = ?");
        sut.setShort(1, (short) 20000);
        final SqlResultSet actual = sut.retrieve();

        assertThat("データが取得できること", actual.size(), is(1));
        assertThat(actual.get(0)
                .getLong("longCol"), is(20000L));
    }

    /**
     * {@link BasicSqlPStatement#setShort(int, short)}のSQLログのテスト。
     */
    @Test
    public void setShort_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL = ?");
        sut.setShort(1, (short) 20000);
        sut.retrieve();

        assertLog("パラメータがログ出力されること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [20000]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setShort(int, short)}でSQLExceptinoが発生するケース。
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setShort_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setShort error")).when(mockStatement).setShort(anyInt(), anyShort());
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL = ?");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setShort(1, (short) 1);
    }

    /**
     * {@link BasicSqlPStatement#setInt(int, int)}のテスト。
     */
    @Test
    public void setInt() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE INTEGER_COL = ?");
        sut.setInt(1, 2);
        final SqlResultSet actual = sut.retrieve();

        assertThat("1レコード取得できる", actual.size(), is(1));
        assertThat("条件の値が取得できる", actual.get(0)
                .getInteger("integerCol"), is(2));
    }

    /**
     * {@link BasicSqlPStatement#setInt(int, int)}のSQLログのテスト。
     */
    @Test
    public void setInt_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE INTEGER_COL IN (?, ?)");
        sut.setInt(1, 2);
        sut.setInt(2, 1);
        sut.retrieve();

        assertLog("パラメータが出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [2]" + Logger.LS
                        + "\t02 = [1]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setInt(int, int)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setInt_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setInt error")).when(mockStatement).setInt(anyInt(), anyInt());
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE INTEGER_COL IN (?, ?)");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setInt(1, 2);
    }

    /**
     * {@link BasicSqlPStatement#setLong(int, long)}のテスト。
     */
    @Test
    public void setLong() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL = ?");
        sut.setLong(1, 30000L);
        final SqlResultSet actual = sut.retrieve();

        assertThat("1レコード取得できる", actual.size(), is(1));
        assertThat("条件の値が取得できる", actual.get(0)
                .getLong("longCol"), is(30000L));
    }

    /**
     * {@link BasicSqlPStatement#setLong(int, long)}のSQLログのテスト。
     */
    @Test
    public void setLong_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL IN (?, ?)");
        sut.setLong(1, 30000L);
        sut.setLong(2, 10000L);
        sut.retrieve();

        assertLog("パラメータが出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [30000]" + Logger.LS
                        + "\t02 = [10000]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setLong(int, long)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setLong_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setLong error")).when(mockStatement).setLong(anyInt(), anyLong());
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE INTEGER_COL IN (?, ?)");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setLong(1, 2);
    }

    /**
     * {@link BasicSqlPStatement#setFloat(int, float)}のテスト。
     */
    @Test
    public void setFloat() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE FLOAT_COL > ?");
        sut.setFloat(1, 4.3f);
        final SqlResultSet actual = sut.retrieve();

        assertThat("1レコード取得できる", actual.size(), is(1));
        assertThat("条件の値が取得できる", actual.get(0)
                .getBigDecimal("floatCol")
                .floatValue(), is(4.4f));
    }

    /**
     * {@link BasicSqlPStatement#setFloat(int, float)}のSQLログのテスト。
     */
    @Test
    public void setFloat_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE FLOAT_COL IN (?, ?)");
        sut.setFloat(1, 2.2f);
        sut.setFloat(2, 1.1f);
        sut.retrieve();

        assertLog("パラメータが出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [2.2]" + Logger.LS
                        + "\t02 = [1.1]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setLong(int, long)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setFloat_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setFloat error")).when(mockStatement).setFloat(anyInt(), anyFloat());
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE INTEGER_COL IN (?, ?)");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setFloat(1, 2.2f);
    }

    /**
     * {@link BasicSqlPStatement#setDouble(int, double)}のテスト。
     */
    @Test
    public void setDouble() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("insert into STATEMENT_TEST_TABLE (entity_id, float_col) values (?, ?)");
        sut.setString(1, "99999");
        sut.setDouble(2, 9.0);
        sut.executeUpdate();
        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "99999");

        assertThat("条件の値が取得できる", (double) actual.floatCol, is(9.0));
    }

    /**
     * {@link BasicSqlPStatement#setDouble(int, double)}のSQLログのテスト。
     */
    @Test
    public void setDouble_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE FLOAT_COL IN (?, ?)");
        sut.setDouble(1, 2.2);
        sut.setDouble(2, 3.3);
        sut.retrieve();

        assertLog("パラメータが出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [2.2]" + Logger.LS
                        + "\t02 = [3.3]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setDouble(int, double)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setDouble_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setDouble error")).when(mockStatement).setDouble(anyInt(), anyDouble());
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE INTEGER_COL IN (?, ?)");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setDouble(1, 2.2);
    }

    /**
     * {@link BasicSqlPStatement#setBigDecimal(int, BigDecimal)}のテスト。
     */
    @Test
    public void setBigDecimal() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE FLOAT_COL > ?");
        sut.setBigDecimal(1, new BigDecimal("4.3"));
        final SqlResultSet actual = sut.retrieve();

        assertThat("1レコード取得できる", actual.size(), is(1));
        assertThat("条件の値が取得できる", actual.get(0)
                .getBigDecimal("floatCol")
                .floatValue(), is(4.4f));
    }

    /**
     * {@link BasicSqlPStatement#setBigDecimal(int, BigDecimal)}のSQLログのテスト。
     */
    @Test
    public void setBigDecimal_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE FLOAT_COL IN (?, ?)");
        sut.setBigDecimal(1, new BigDecimal("2.2"));
        sut.setBigDecimal(2, new BigDecimal("3.3"));
        sut.retrieve();

        assertLog("パラメータが出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [2.2]" + Logger.LS
                        + "\t02 = [3.3]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setBigDecimal(int, BigDecimal)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setBigDecimal_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setBigDecimal error")).when(mockStatement).setBigDecimal(anyInt(), eq(BigDecimal.ONE));
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setBigDecimal(1, BigDecimal.ONE);
    }

    /**
     * {@link BasicSqlPStatement#setString(int, String)}のテスト。
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.SQL_SERVER)
    public void setString() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ? AND VARCHAR_COL = ?");
        sut.setString(1, "10004");
        sut.setString(2, "\uD840\uDC0B");
        final SqlResultSet actual = sut.retrieve();

        assertThat("1レコード取得できる", actual.size(), is(1));
        assertThat("条件の値が取得できる", actual.get(0)
                .getString("entityId"), is("10004"));
        assertThat("条件の値が取得できる", actual.get(0)
                .getString("varcharCol"), is("\uD840\uDC0B"));
    }

    /**
     * {@link BasicSqlPStatement#setString(int, String)}のSQLログのテスト。
     */
    @Test
    public void setString_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID IN(?, ?)");
        sut.setString(1, "10001");
        sut.setString(2, "99999");
        sut.retrieve();

        assertLog("パラメータが出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [10001]" + Logger.LS
                        + "\t02 = [99999]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setString(int, String)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setString_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setString error")).when(mockStatement).setString(anyInt(), anyString());
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setString(1, "1");
    }

    /**
     * {@link BasicSqlPStatement#setBytes(int, byte[])}のテスト。
     */
    @Test
    public void setBytes() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, BINARY_COL) VALUES (?, ?)");

        sut.setString(1, "99999");
        sut.setBytes(2, new byte[] {0x00, 0x30, 0x31});
        final int inserted = sut.executeUpdate();
        assertThat("1レコード登録できている", inserted, is(1));
        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "99999");
        assertThat("バイナリデータが登録出来ている", actual.binaryCol, is(new byte[] {0x00, 0x30, 0x31}));
    }

     /**
     * {@link BasicSqlPStatement#setBytes(int, byte[])}のSQLログの出力テスト。
     */
    @Test
    public void setBytes_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, BINARY_COL) VALUES (?, ?)");

        sut.setString(1, "99999");
        sut.setBytes(2, new byte[] {0x00, 0x30, 0x31});
        sut.executeUpdate();

        assertLog("パラメータがログに出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [99999]" + Logger.LS
                        + "\t02 = [bytes]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setBytes(int, byte[])}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setBytes_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setBytes error")).when(mockStatement).setBytes(anyInt(), notNull());
        
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, BINARY_COL) VALUES (?, ?)");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setBytes(2, new byte[] {0x00, 0x01});
    }

    /**
     * {@link BasicSqlPStatement#setDate(int, java.sql.Date)}のテスト。
     */
    @Test
    public void setDate() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, 1, 20, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        final java.sql.Date insertDate = new java.sql.Date(calendar.getTimeInMillis());

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, DATE_COL) VALUES  (?, ?)");
        sut.setString(1, "99999");
        sut.setDate(2, insertDate);

        final int updated = sut.executeUpdate();
        assertThat("1レコード登録されている", updated, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "99999");
        assertThat("日付が登録出来ている", actual.dateCol, is((Date) insertDate));
    }

    /**
     * {@link BasicSqlPStatement#setDate(int, java.sql.Date)}のSQLログのテスト。
     */
    @Test
    public void setDate_writeSqlLog() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, 1, 20, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        final java.sql.Date insertDate = new java.sql.Date(calendar.getTimeInMillis());

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, DATE_COL) VALUES  (?, ?)");
        sut.setString(1, "99999");
        sut.setDate(2, insertDate);
        sut.executeUpdate();

        assertLog("パラメータがログ出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [99999]" + Logger.LS
                        + "\t02 = [2015-02-20]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setDate(int, java.sql.Date)}のSQLExceptionが発生するケース。
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setDate_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setDate error")).when(mockStatement).setDate(anyInt(), any(java.sql.Date.class));

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, DATE_COL) VALUES  (?, ?)");

        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setDate(2, new java.sql.Date(0));
    }

    /**
     * {@link BasicSqlPStatement#setTime(int, Time)}のテスト。
     */
    @Test
    public void setTime() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.set(1970, 1, 1, 1, 2, 3);
        calendar.set(Calendar.MILLISECOND, 0);
        final Time insertTime = new Time(calendar.getTimeInMillis());

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT  INTO STATEMENT_TEST_TABLE (ENTITY_ID, TIME_COL) VALUES (?, ?)");
        sut.setString(1, "88888");
        sut.setTime(2, insertTime);
        final int inserted = sut.executeUpdate();
        assertThat("1レコード登録される", inserted, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "88888");

        assertThat("Timeが登録されること", actual.timeCol.toString(), is(insertTime.toString()));
    }

    /**
     * {@link BasicSqlPStatement#setTime(int, Time)}のSQLログのテスト。
     */
    @Test
    public void setTime_writeSqlLog() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.set(1970, 1, 1, 1, 2, 3);
        calendar.set(Calendar.MILLISECOND, 0);
        final Time insertTime = new Time(calendar.getTimeInMillis());

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT  INTO STATEMENT_TEST_TABLE (ENTITY_ID, TIME_COL) VALUES (?, ?)");
        sut.setString(1, "88888");
        sut.setTime(2, insertTime);
        sut.executeUpdate();

        assertLog("パラメータがログ出力されること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [88888]" + Logger.LS
                        + "\t02 = [01:02:03]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setTime(int, Time)}のSQLExceptionが発生するケース。
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setTime_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setTime error")).when(mockStatement).setTime(anyInt(), any(Time.class));

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, TIME_COL) VALUES  (?, ?)");

        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setTime(2, new Time(0));
    }

    /**
     * {@link BasicSqlPStatement#setTimestamp(int, Timestamp)}のテスト。
     */
    @Test
    public void setTimestamp() throws Exception {
        final Timestamp insertTimestamp = Timestamp.valueOf("2015-03-14 11:12:13.007");

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, TIMESTAMP_COL) VALUES (?, ?)");
        sut.setString(1, "77777");
        sut.setTimestamp(2, insertTimestamp);
        final int inserted = sut.executeUpdate();

        assertThat("1レコード登録される", inserted, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "77777");
        assertThat("登録されていること", actual.timestampCol, is(insertTimestamp));
    }

    /**
     * {@link BasicSqlPStatement#setTimestamp(int, Timestamp)}のSQLログのテスト。
     */
    @Test
    public void setTimestamp_writeSqlLog() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, 1, 19, 1, 2, 3);
        calendar.set(Calendar.MILLISECOND, 321);
        final Timestamp insertTimestamp = new Timestamp(calendar.getTimeInMillis());

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, TIMESTAMP_COL) VALUES (?, ?)");
        sut.setString(1, "77777");
        sut.setTimestamp(2, insertTimestamp);
        sut.executeUpdate();

        assertLog("パラメータがログ出力されること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [77777]" + Logger.LS
                        + "\t02 = [2015-02-19 01:02:03.321]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setTimestamp(int, Timestamp)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setTimestamp_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setTimestamp error")).when(mockStatement).setTimestamp(anyInt(), any(Timestamp.class));

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, TIMESTAMP_COL) VALUES  (?, ?)");

        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setTimestamp(2, new Timestamp(0));
    }

    /**
     * {@link BasicSqlPStatement#setAsciiStream(int, InputStream, int)}のテスト。
     */
    @Test
    @TargetDb(exclude = {TargetDb.Db.SQL_SERVER, TargetDb.Db.DB2})
    public void setAsciiStream() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, VARCHAR_COL) VALUES (?, ?)");
        sut.setString(1, "55555");
        InputStream stream = new ByteArrayInputStream("12345".getBytes("utf-8"));
        sut.setAsciiStream(2, stream, 2);
        final int inserted = sut.executeUpdate();
        stream.close();
        assertThat("1レコード登録される", inserted, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "55555");
        assertThat("登録されていること", actual.varcharCol, is("12"));

    }

    /**
     * {@link BasicSqlPStatement#setAsciiStream(int, InputStream, int)}のテスト。
     *
     * ※SQLServerは、Streamの長さとlengthを一致させる必要があるため別でテストを実施
     */
    @Test
    @TargetDb(include = {TargetDb.Db.SQL_SERVER, TargetDb.Db.DB2})
    public void setAsciiStream_SQLServer_DB2() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, VARCHAR_COL) VALUES (?, ?)");
        sut.setString(1, "55555");
        InputStream stream = new ByteArrayInputStream("12345".getBytes("utf-8"));
        sut.setAsciiStream(2, stream, 5);
        final int inserted = sut.executeUpdate();
        stream.close();
        assertThat("1レコード登録される", inserted, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "55555");
        assertThat("登録されていること", actual.varcharCol, is("12345"));

    }

    /**
     * {@link BasicSqlPStatement#setAsciiStream(int, InputStream, int)}のSQLログのテスト。
     */
    @Test
    public void setAsciiStream_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, VARCHAR_COL) VALUES (?, ?)");
        sut.setString(1, "55555");
        InputStream stream = new ByteArrayInputStream("12345".getBytes("utf-8"));
        sut.setAsciiStream(2, stream, 5);
        sut.executeUpdate();
        stream.close();

        assertLog("パラメータがログ出力されること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [55555]" + Logger.LS
                        + "\t02 = [InputStream]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setAsciiStream(int, InputStream, int)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setAsciiStream_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setAsciiStream error")).when(mockStatement).setAsciiStream(anyInt(), any(ByteArrayInputStream.class), anyInt());

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, VARCHAR_COL) VALUES  (?, ?)");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setAsciiStream(2, new ByteArrayInputStream(new byte[0]), 0);
    }

    /**
     * {@link BasicSqlPStatement#setObject(int, Object)}のテスト。
     */
    @Test
    public void setObject() throws Exception {
        final Timestamp timestamp = new Timestamp(0);

        final SqlPStatement sut = dbCon.prepareStatement(
                "insert into STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, INTEGER_COL, TIMESTAMP_COL) VALUES (?, ?, ?, ?)");
        sut.setObject(1, "44444");
        sut.setObject(2, 100L);
        sut.setObject(3, -1);
        sut.setObject(4, timestamp);
        final int inserted = sut.executeUpdate();
        assertThat("1レコード登録される", inserted, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "44444");
        assertThat(actual.id, is("44444"));
        assertThat(actual.longCol, is(100L));
        assertThat(actual.integerCol, is(-1));
        assertThat(actual.timestampCol, is(timestamp));
    }

    /**
     * {@link BasicSqlPStatement#setObject(int, Object)}のSQLログのテスト。
     */
    @Test
    public void setObject_writeSqlLog() throws Exception {

        final SqlPStatement sut = dbCon.prepareStatement(
                "insert into STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, INTEGER_COL) VALUES (?, ?, ?)");
        sut.setObject(1, "44444");
        sut.setObject(2, 100L);
        sut.setObject(3, -1);
        sut.executeUpdate();

        assertLog("パラメータがログ出力されること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [44444]" + Logger.LS
                        + "\t02 = [100]" + Logger.LS
                        + "\t03 = [-1]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setObject(int, Object)}でSQLExceptionが発生する場合は
     * DbAccessExceptionが送出される。
     */
    @Test(expected = DbAccessException.class)
    public void setObject_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setObject error")).when(mockStatement).setObject(anyInt(), any());
        
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setObject(1, "12345");
    }

    /**
     * {@link BasicSqlPStatement#setObject(int, Object, int)}のテスト。
     */
    @Test
    public void setObjectWithType() throws Exception {
        final Timestamp timestamp = new Timestamp(0);

        final SqlPStatement sut = dbCon.prepareStatement(
                "insert into STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, INTEGER_COL, TIMESTAMP_COL) VALUES (?, ?, ?, ?)");
        sut.setObject(1, "44444", Types.CHAR);
        sut.setObject(2, "100", Types.BIGINT);
        sut.setObject(3, -1, Types.INTEGER);
        sut.setObject(4, timestamp, Types.TIMESTAMP);
        final int inserted = sut.executeUpdate();
        assertThat("1レコード登録される", inserted, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "44444");
        assertThat(actual.id, is("44444"));
        assertThat(actual.longCol, is(100L));
        assertThat(actual.integerCol, is(-1));
        assertThat(actual.timestampCol, is(timestamp));
    }

    /**
     * {@link BasicSqlPStatement#setObject(int, Object, int)}のSQLログのテスト。
     */
    @Test
    public void setObjectWithType_writeSqlLog() throws Exception {

        final SqlPStatement sut = dbCon.prepareStatement(
                "insert into STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, INTEGER_COL) VALUES (?, ?, ?)");
        sut.setObject(1, "44444", Types.CHAR);
        sut.setObject(2, 100L, Types.BIGINT);
        sut.setObject(3, -1, Types.INTEGER);
        sut.executeUpdate();

        assertLog("パラメータがログ出力されること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [44444]" + Logger.LS
                        + "\t02 = [100]" + Logger.LS
                        + "\t03 = [-1]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setObject(int, Object, int)}でSQLExceptionが発生する場合は
     * DbAccessExceptionが送出される。
     */
    @Test(expected = DbAccessException.class)
    public void setObjectWithType_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setObjectWithType error")).when(mockStatement).setObject(anyInt(), any(), anyInt());
        
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setObject(1, "12345", Types.CHAR);
    }

    /**
     * {@link BasicSqlPStatement#getResultSet()}のテスト。
     */
    @Test
    public void getResultSet() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT ENTITY_ID FROM  STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        sut.setString(1, "10002");
        final boolean result = sut.execute();
        assertThat(result, is(true));
        final ResultSet rs = sut.getResultSet();
        try {
            assertThat(rs.next(), is(true));
            assertThat(rs.getString(1), is("10002"));
            assertThat(rs.next(), is(false));
        } finally {
            rs.close();
        }
    }

    /**
     * {@link BasicSqlPStatement#getResultSet()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getResultSet_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("getResultSet error")).when(mockStatement).getResultSet();
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.getResultSet();
    }

    /**
     * {@link BasicSqlPStatement#getUpdateCount()}のテスト。
     */
    @Test
    public void getUpdateCount() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("UPDATE STATEMENT_TEST_TABLE SET ENTITY_ID = ENTITY_ID");
        assertThat("更新件数は4", sut.executeUpdate(), is(4));
        assertThat("更新件数が取得できる", sut.getUpdateCount(), is(4));
    }

    public static class ParameterizedParameterObject {

        private String id1 = "10001";
        private String id2 = "10002";

        public String getId1() {
            return id1;
        }

        public String getId2() {
            return id2;
        }
    }

    /**
     * {@link BasicSqlPStatement#getUpdateCount()}のテスト。
     * <p/>
     * パラメタ化されたステートメントの場合でも、更新後に更新件数が取得できること。
     */
    @Test
    public void getUpdateCount_Parametarized() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "UPDATE STATEMENT_TEST_TABLE SET ENTITY_ID = ENTITY_ID WHERE ENTITY_ID = :id1 OR ENTITY_ID = :id2");
        assertThat("更新件数は2", sut.executeUpdateByObject(new ParameterizedParameterObject()), is(2));
        assertThat("更新件数が取得できる", sut.getUpdateCount(), is(2));
    }

    /**
     * {@link BasicSqlPStatement#getUpdateCount()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getUpdateCount_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(mockStatement.getUpdateCount()).thenThrow(new SQLException("getUpdateCount error"));
        
        final SqlPStatement sut = dbCon.prepareStatement("UPDATE STATEMENT_TEST_TABLE SET ENTITY_ID = ENTITY_ID");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.getUpdateCount();
    }

    /**
     * {@link BasicSqlPStatement#getMetaData()}のテスト。
     */
    @Test
    public void getMetaData() throws Exception {
        SqlPStatement sut = dbCon.prepareStatement("SELECT ENTITY_ID, LONG_COL from STATEMENT_TEST_TABLE where ENTITY_ID = ?");
        final ResultSetMetaData actual = sut.getMetaData();
        assertThat(actual, is(notNullValue()));
    }

    /**
     * {@link BasicSqlPStatement#getMetaData()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getMetaData_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(mockStatement.getMetaData()).thenThrow(new SQLException("getMetaData error"));
        SqlPStatement sut = dbCon.prepareStatement(
                "SELECT ENTITY_ID, LONG_COL from STATEMENT_TEST_TABLE where ENTITY_ID = ?");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.getMetaData();
    }

    /**
     * {@link BasicSqlPStatement#setMaxRows(int)}と{@link BasicSqlPStatement#getMaxRows()}のテスト。
     */
    @Test
    public void setMaxRows() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        sut.setMaxRows(2);

        assertThat("設定した値が取得できること", sut.getMaxRows(), is(2));
        final ResultSetIterator actual = sut.executeQuery();
        assertThat("1レコード目は取得できる", actual.next(), is(true));
        assertThat("2レコード目は取得できる", actual.next(), is(true));
        assertThat("maxrowsが2なので3レコード目は取得されない", actual.next(), is(false));
    }

    /**
     * {@link BasicSqlPStatement#setMaxRows(int)}と{@link BasicSqlPStatement#getMaxRows()}のテスト。
     *
     * パラメータ化されたステートメントオブジェクトでも使用できることを確認する。
     */
    @Test
    public void setMaxRows_Parametarized() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        sut.setMaxRows(2);

        assertThat("設定した値が取得できること", sut.getMaxRows(), is(2));
        final ResultSetIterator actual = sut.executeQueryByObject(new Object());
        assertThat("1レコード目は取得できる", actual.next(), is(true));
        assertThat("2レコード目は取得できる", actual.next(), is(true));
        assertThat("maxrowsが2なので3レコード目は取得されない", actual.next(), is(false));
    }

    /**
     * {@link BasicSqlPStatement#setMaxRows(int)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setMaxRows_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setMaxRows error")).when(mockStatement).setMaxRows(anyInt());

        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setMaxRows(2);
    }

    /**
     * {@link BasicSqlPStatement#getMaxRows()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getMaxRows_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(mockStatement.getMaxRows()).thenThrow(new SQLException("getMaxRows error"));
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.getMaxRows();
    }

    /**
     * {@link BasicSqlPStatement#setFetchSize(int)}と{@link BasicSqlPStatement#getFetchSize()}のテスト。
     */
    @Test
    public void setFetchSize() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        sut.setFetchSize(2);
        assertThat("設定したFetchSizeが取得できること", sut.getFetchSize(), is(2));
    }

    /**
     * {@link BasicSqlPStatement#setFetchSize(int)}と{@link BasicSqlPStatement#getFetchSize()}のテスト。
     *
     * パラメータ化されたステートメントオブジェクトでも使用できることを確認する。
     */
    @Test
    public void setFetchSize_Parametarized() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        sut.setFetchSize(7);
        assertThat("設定したFetchSizeが取得できること", sut.getFetchSize(), is(7));
    }

    /**
     * {@link BasicSqlPStatement#setFetchSize(int)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setFetchSize_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setFetchSize error")).when(mockStatement).setFetchSize(anyInt());

        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setFetchSize(10);
    }

    /**
     * {@link BasicSqlPStatement#getFetchSize()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getFetchSize_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(mockStatement.getFetchSize()).thenThrow(new SQLException("getFetchSize error"));

        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setFetchSize(10);
        sut.getFetchSize();
    }

    /**
     * {@link BasicSqlPStatement#setQueryTimeout(int)}と{@link BasicSqlPStatement#getQueryTimeout()}のテスト。
     */
    @Test
    public void setQueryTimeout() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        sut.setQueryTimeout(999);
        assertThat("設定したQueryTimeoutが取得できること", sut.getQueryTimeout(), is(999));
    }

    /**
     * {@link BasicSqlPStatement#setQueryTimeout(int)}と{@link BasicSqlPStatement#getQueryTimeout()}のテスト。
     */
    @Test
    public void setQueryTimeout_Parametarized() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        sut.setQueryTimeout(321);
        assertThat("設定したQueryTimeoutが取得できること", sut.getQueryTimeout(), is(321));
    }

    /**
     * {@link BasicSqlPStatement#setQueryTimeout(int)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setQueryTimeout_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setQueryTimeout error")).when(mockStatement).setQueryTimeout(anyInt());

        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setQueryTimeout(10);
    }

    /**
     * {@link BasicSqlPStatement#getQueryTimeout()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getQueryTimeout_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(mockStatement.getQueryTimeout()).thenThrow(new SQLException("getQueryTimeout error"));

        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.getQueryTimeout();
    }

    /**
     * {@link BasicSqlPStatement#setBinaryStream(int, InputStream, int)}のテスト。
     */
    @Test
    @TargetDb(exclude = {TargetDb.Db.SQL_SERVER, TargetDb.Db.DB2})
    public void setBinaryStream() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, BINARY_COL) VALUES (?, ?)");
        sut.setString(1, "55555");
        sut.setBinaryStream(2, new ByteArrayInputStream(new byte[]{0x31, 0x32, 0x33}), 2);
        final int updated = sut.executeUpdate();
        assertThat("1レコード更新される", updated, is(1));
        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "55555");
        assertThat("登録されていること", actual.binaryCol, is(new byte[] {0x31, 0x32}));
    }

    /**
     * {@link BasicSqlPStatement#setBinaryStream(int, InputStream, int)}のテスト。
     *
     * ※SQLServerはStreamの長さとlengthを一致させる必要がある。
     */
    @Test
    @TargetDb(include = {TargetDb.Db.SQL_SERVER, TargetDb.Db.DB2})
    public void setBinaryStream_SQLServer() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, BINARY_COL) VALUES (?, ?)");
        sut.setString(1, "55555");
        sut.setBinaryStream(2, new ByteArrayInputStream(new byte[]{0x31, 0x32, 0x33}), 3);
        final int updated = sut.executeUpdate();
        assertThat("1レコード更新される", updated, is(1));
        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "55555");
        assertThat("登録されていること", actual.binaryCol, is(new byte[] {0x31, 0x32, 0x33}));
    }

    /**
     * {@link BasicSqlPStatement#setBinaryStream(int, InputStream, int)}でSQLExceptionが発生した場合、
     * DbAccessExceptionが発生する。
     */
    @Test(expected = DbAccessException.class)
    public void setBinaryStream_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setBinaryStream error")).when(mockStatement).setBinaryStream(anyInt(), any(ByteArrayInputStream.class), anyInt());
        
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, BINARY_COL) VALUES (?, ?)");

        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setBinaryStream(2, new ByteArrayInputStream(new byte[] {0x31, 0x32, 0x33}), 2);
    }

    /**
     * {@link BasicSqlPStatement#setCharacterStream(int, Reader, int)}のテスト。
     */
    @Test
    @TargetDb(include = {TargetDb.Db.ORACLE, TargetDb.Db.DB2, TargetDb.Db.H2})
    public void setCharacterStream() throws Exception {
        VariousDbTestHelper.createTable(ClobColumn.class);
        final SqlPStatement sut = dbCon.prepareStatement( "insert into clob_table (id, clob_col) values (?, ?)");
        sut.setLong(1, 1L);
        sut.setCharacterStream(2, new StringReader("あいうえおかきくけこ"), 10);
        assertThat(sut.executeUpdate(), is(1));
        dbCon.commit();

        final ClobColumn actual = VariousDbTestHelper.findById(ClobColumn.class, 1L);
        assertThat(actual.clob, is("あいうえおかきくけこ"));
    }
    
    /**
     * {@link BasicSqlPStatement#setCharacterStream(int, Reader, int)}のテスト。
     */
    @Test(expected = DbAccessException.class)
    public void setCharacterStream_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("setCharacterStream error")).when(mockStatement).setCharacterStream(anyInt(), any(Reader.class), anyInt());
        
        final SqlPStatement sut = dbCon.prepareStatement(
                "insert into STATEMENT_TEST_TABLE (entity_id, varchar_col) values (?, ?)");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setCharacterStream(2, new StringReader("1234554321"), 10);
    }

    /**
     * {@link BasicSqlPStatement#close()}と{@link BasicSqlPStatement#isClosed()}のテスト。
     */
    @Test
    public void close() {
        final List<SqlStatement> statements = ReflectionUtil.getFieldValue(dbCon, "statements");
        assertThat("ステートメントリストは空であること", statements.isEmpty(), is(true));

        final SqlPStatement sut = dbCon.prepareStatement("select * from STATEMENT_TEST_TABLE");
        assertThat("ステートメントリストに追加されていること", statements.size(), is(1));

        assertThat("クローズ前はクローズされていないこと", sut.isClosed(), is(false));
        sut.close();

        assertThat("クローズされていること", sut.isClosed(), is(true));
        assertThat("クローズされるとステートメントリストからも削除されること", statements.isEmpty(), is(true));

        try {
            sut.executeQuery();
            fail("ここはとおらない");
        } catch (SqlStatementException e) {
            assertThat(e, is(notNullValue()));
        }
    }

    /**
     * {@link BasicSqlPStatement#close()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void close_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("close error")).when(mockStatement).close();
        final SqlPStatement sut = dbCon.prepareStatement("select * from STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        try {
            sut.close();
        } finally {
            // @After で dbCon が close されるときにモックが例外をスローしたままだとテストが落ちるのでリセットしておく
            reset(mockStatement);
        }
    }

    /**
     * {@link BasicSqlPStatement#getMoreResults()}のテスト
     *
     * Mockを使ってテストを行う。
     */
    @Test
    public void getMoreResults() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(mockStatement.getMoreResults()).thenReturn(true, false);
        final SqlPStatement sut = dbCon.prepareStatement("select * from STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);

        assertThat("初回はtrue", sut.getMoreResults(), is(true));
        assertThat("2回目はfalse", sut.getMoreResults(), is(false));
    }

    /**
     * {@link BasicSqlPStatement#getMoreResults()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getMoreResults_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(mockStatement.getMoreResults()).thenThrow(new SQLException("getMoreResults error"));
        final SqlPStatement sut = dbCon.prepareStatement("select * from STATEMENT_TEST_TABLE");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.getMoreResults();
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Map)}のテスト。
     */
    @Test
    public void retrieve_map() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10002");

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("1レコード取得できること", actual.size(), is(1));
        assertThat(actual.get(0)
                .getString("entityId"), is("10002"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Map)}で{@link LocalDate}を検索条件とした場合のテスト。
     */
    @Test
    public void retrieve_map_with_localDate_condition() throws Exception {
        LocalDate localdate = LocalDate.parse("2015-04-01");
        TestEntity entity =  new TestEntity();
        entity.id = "12345";
        entity.localDateCol = localdate;
        VariousDbTestHelper.insert(entity);

        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE LOCAL_DATE_COL = :localDate");

        Map<String, Object> condition = new HashMap<>();
        condition.put("localDate", localdate);

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("1レコード取得できること", actual.size(), is(1));
        assertThat(((java.sql.Date)actual.get(0).get("LOCAL_DATE_COL")).toLocalDate(), is(localdate));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Map)}で{@link LocalDate}を検索条件とした場合のテスト。
     */
    @Test
    public void retrieve_map_with_localDateTime_condition() throws Exception {
        LocalDateTime localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        TestEntity entity =  new TestEntity();
        entity.id = "12345";
        entity.localDateTimeCol = localDateTime;
        VariousDbTestHelper.insert(entity);

        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE LOCAL_DATE_TIME_COL = :localDateTime");

        Map<String, Object> condition = new HashMap<>();
        condition.put("localDateTime", localDateTime);

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("1レコード取得できること", actual.size(), is(1));
        assertThat(((java.sql.Timestamp)actual.get(0).get("LOCAL_DATE_TIME_COL")).toLocalDateTime(), is(localDateTime));
    }
    
    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}のテスト。
     */
    @Test
    public void retrieve_map_withOffsetAndLimit() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id ORDER BY ENTITY_ID");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10002");

        final SqlResultSet firstRow = sut.retrieve(1, 1, condition);
        assertThat("1レコード取得できること", firstRow.size(), is(1));
        assertThat(firstRow.get(0)
                .getString("entityId"), is("10001"));

        final SqlResultSet secondRow = sut.retrieve(2, 1, condition);
        assertThat("1レコード取得できること", secondRow.size(), is(1));
        assertThat(secondRow.get(0)
                .getString("entityId"), is("10002"));

        final SqlResultSet multiRow = sut.retrieve(1, 2, condition);
        assertThat("2レコード取得できること", multiRow.size(), is(2));
        assertThat(multiRow.get(0)
                .getString("entityId"), is("10001"));
        assertThat(multiRow.get(1)
                .getString("entityId"), is("10002"));
    }

    /**
     * ステートメント生成時に設定された検索処理オプションで検索条件にMAPを使用しても、範囲指定できる事。
     *
     * @throws Exception 例外
     */
    @Test
    public void retrieve_map_withSelectOption() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID >= :id ORDER BY ENTITY_ID", new SelectOption(2, 2));
        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10001");

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("2レコード取得できること", actual.size(), is(2));
        assertThat(actual.get(0).getString("entityId"), is("10002"));
        assertThat(actual.get(1).getString("entityId"), is("10003"));
    }

    /**
     * ステートメント生成時と実行時に検索範囲を指定した場合例外が発生すること。
     *
     * @throws Exception 例外
     */
    @Test(expected = IllegalOperationException.class)
    public void retrieve_map_withDuplicatePagenate() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id ORDER BY ENTITY_ID", new SelectOption(2, 2));
        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10002");
        sut.retrieve(1, 1, condition);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}のSQLログのテスト。
     */
    @Test
    public void retrieve_map_writeSqlLog() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id ORDER BY ENTITY_ID");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10002");
        sut.retrieve(1, 2, condition);

        assertLog("開始ログ",
                Pattern.compile("\\Qnablarch.core.db.statement.BasicSqlPStatement#retrieve"
                        + " SQL = [SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= ? ORDER BY ENTITY_ID]"
                        + Logger.LS
                        + "\tstart position = [1] size = [2] queryTimeout = [600] fetchSize = [2]" + Logger.LS
                        + "\toriginal sql = [SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id ORDER BY ENTITY_ID]\\E"));

        assertLog("パラメータ",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\tid = [10002]\\E"));

        assertLog("終了ログ",
                Pattern.compile("nablarch.core.db.statement.BasicSqlPStatement#retrieve" + Logger.LS
                        + "\texecute time\\(ms\\) = \\[[0-9]+\\] retrieve time\\(ms\\) = \\[[0-9]+\\] "));
    }

    @Test
    public void retrieve_writeLogWithMapOffsetSupport() throws Exception {
        String convertedSql = "SELECT 'TEST_CODE' FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID > :id ORDER BY ENTITY_ID";
        setDialect(dbCon, new OffsetSupportDialcet(convertedSql));
        ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id",
                new SelectOption(2, 3));
        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "1");
        sut.retrieve(condition);
        assertLog("開始ログ",
                Pattern.compile("\\Qnablarch.core.db.statement.BasicSqlPStatement#retrieve"
                        + " SQL = [SELECT 'TEST_CODE' FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID > ? ORDER BY ENTITY_ID]"
                        + Logger.LS
                        + "\tstart position = [1] size = [0] queryTimeout = [600] fetchSize = [50]" + Logger.LS
                        + "\toriginal sql = ["+ convertedSql +"]\\E"));
        assertLog("パラメータ",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\tid = [1]\\E"));
        assertLog("終了ログ",
                Pattern.compile("nablarch.core.db.statement.BasicSqlPStatement#retrieve" + Logger.LS
                        + "\texecute time\\(ms\\) = \\[[0-9]+\\] retrieve time\\(ms\\) = \\[[0-9]+\\] "));
    }

    @Test
    public void retrieve_writeLogWithMapOffsetUnSupport() throws Exception {
        setDialect(dbCon, new DefaultDialect());
        ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id", new SelectOption(2, 3));
        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "1");
        sut.retrieve(condition);
        assertLog("開始ログ",
                Pattern.compile("\\Qnablarch.core.db.statement.BasicSqlPStatement#retrieve"
                        + " SQL = [SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= ?]"
                        + Logger.LS
                        + "\tstart position = [2] size = [3] queryTimeout = [600] fetchSize = [3]" + Logger.LS
                        + "\toriginal sql = [SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id]\\E"));
        assertLog("パラメータ",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\tid = [1]\\E"));
        assertLog("終了ログ",
                Pattern.compile("nablarch.core.db.statement.BasicSqlPStatement#retrieve" + Logger.LS
                        + "\texecute time\\(ms\\) = \\[[0-9]+\\] retrieve time\\(ms\\) = \\[[0-9]+\\] "));

    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}の前方一致のテスト
     */
    @Test
    public void retrieve_map_with_likeForwardMatch() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :id% ORDER BY ENTITY_ID");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "1");
        final SqlResultSet actual = sut.retrieve(1, 2, condition);
        assertThat(actual.size(), is(2));
        assertThat(actual.get(0)
                .getString("entityId"), is("10001"));
        assertThat(actual.get(1)
                .getString("entityId"), is("10002"));
        OnMemoryLogWriter.assertLogContains("writer.memory", "id% = [1%]");
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}の部分一致のテスト
     */
    @Test
    public void retrieve_map_with_likePartialMatch() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :%id% ORDER BY ENTITY_ID");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "000");
        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(4));

        assertThat(actual.get(0)
                .getString("entityId"), is("10001"));
        assertThat(actual.get(1)
                .getString("entityId"), is("10002"));
        assertThat(actual.get(2)
                .getString("entityId"), is("10003"));
        assertThat(actual.get(3)
                .getString("entityId"), is("10004"));
        OnMemoryLogWriter.assertLogContains("writer.memory", "%id% = [%000%]");
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}の後方一致のテスト
     */
    @Test
    public void retrieve_map_with_likeBackwardMatch() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :%id ORDER BY ENTITY_ID");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "0002");
        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));

        assertThat(actual.get(0)
                .getString("entityId"), is("10002"));
        OnMemoryLogWriter.assertLogContains("writer.memory", "%id = [%0002]");
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}のLIKE検索でエスケープ対象の文字が含まれている場合
     */
    @Test
    public void retrieve_map_with_likeWithEscapeChar() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL LIKE :condition% ORDER BY ENTITY_ID");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("condition", "c_\\");
        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));

        assertThat(actual.get(0)
                .getString("entityId"), is("10003"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}で、条件の名前付きパラメータがMapに存在しない場合、
     * 例外が送出されること。
     */
    @Test
    public void retrieve_map_invalidKey() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id AND VARCHAR_COL = :varcharCol ORDER BY ENTITY_ID");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "0002");
        try {
            sut.retrieve(condition);
            fail("とおらない");
        } catch (IllegalArgumentException e) {
            assertThat("エラーになる。", e.getMessage(), is("SQL parameter was not found in Object. parameter name=[varcharCol]"));
        }
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}で、条件の名前付きパラメータがMapに存在しない場合、
     * 例外が送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void retrieve_map_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("retrieve map error")).when(mockStatement).setObject(anyInt(), any());
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id AND VARCHAR_COL = :varcharCol ORDER BY ENTITY_ID");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10002");
        condition.put("varcharCol", "b");

        sut.retrieve(condition);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}のテスト。
     */
    @Test
    public void retrieve_object() throws Exception {
        doRetrieve_object(false);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}のテスト。(Fieldアクセステスト用)
     */
    @Test
    public void retrieve_objectViaFieldAccess() throws Exception {
        doRetrieve_object(true);
    }


    private void doRetrieve_object(boolean isFieldAccess) {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id");

        Object condition;
        if (!isFieldAccess) {
            final TestEntity entity = new TestEntity();
            entity.id = "10002";
            condition = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "10002";
            condition = entity;
        }

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("1レコード取得できること", actual.size(), is(1));
        assertThat(actual.get(0)
                .getString("entityId"), is("10002"));
    }

    /**
     * fieldアクセスモードに変更する。
     */
    private void setFieldAccessMode() {
        //fieldアクセス
        SystemRepository.load(new ObjectLoader() {
            @Override
            public Map<String, Object> load() {
                final Map<String, Object> map = new HashMap<String,Object>();
                map.put("nablarch.dbAccess.isFieldAccess", "true");
                return map;
            }
        });
        assertThat("Fieldアクセスか", DbUtil.isFieldAccess(), is(true));

    }

    public static class WithEnum {
        enum Authority {
            ADMIN,
            STANDARD
        }

        private Authority authority = Authority.ADMIN;

        public String getAuthorityString() {
            if (authority == Authority.ADMIN) {
                return "10001";
            } else {
                return "10002";
            }
        }
    }

    /**
     * フィールドにenumオブジェクトを持つオブジェクトを条件に検索。
     *
     * アクセッサ(getter)で文字列表現に変換して、その条件で検索できていることを検証する。
     */
    @Test
    public void retrieve_objectWithEnum() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :authorityString");

        final WithEnum withEnum = new WithEnum();
        withEnum.authority = WithEnum.Authority.STANDARD;

        final SqlResultSet actual = sut.retrieve(withEnum);
        assertThat(actual, hasSize(1));
        assertThat(actual.get(0).getString("entityId"), is("10002"));
    }

    /**
     * フィールドに{@link LocalDate}を持つオブジェクトを条件に検索
     */
    @Test
    public void retrieve_objectWithLocalDate() throws Exception {

        LocalDate localdate = LocalDate.parse("2015-04-01");
        TestEntity entity =  new TestEntity();
        entity.id = "12345";
        entity.localDateCol = localdate;
        VariousDbTestHelper.insert(entity);

        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE LOCAL_DATE_COL = :localDateCol");
        final TestEntity condition =  new TestEntity();
        condition.localDateCol = localdate;

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("1レコード取得できること", actual.size(), is(1));
        assertThat(((java.sql.Date)actual.get(0).get("LOCAL_DATE_COL")).toLocalDate(), is(localdate));
    }

    /**
     * フィールドに{@link LocalDateTime}を持つオブジェクトを条件に検索
     */
    @Test
    public void retrieve_objectWithLocalDateTime() {

        LocalDateTime localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        TestEntity entity =  new TestEntity();
        entity.id = "12345";
        entity.localDateTimeCol = localDateTime;
        VariousDbTestHelper.insert(entity);

        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE LOCAL_DATE_TIME_COL = :localDateTimeCol");
        final TestEntity condition =  new TestEntity();
        condition.localDateTimeCol = localDateTime;

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("1レコード取得できること", actual.size(), is(1));
        assertThat(((java.sql.Timestamp)actual.get(0).get("LOCAL_DATE_TIME_COL")).toLocalDateTime(), is(localDateTime));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}のテスト。
     */
    @Test
    public void retrieve_object_withOption() throws Exception {
        doRetrieve_object_withOption(false);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}のテスト。(Fieldアクセステスト用)
     */
    @Test
    public void retrieve_object_withOptionViaFieldAccess() throws Exception {
        doRetrieve_object_withOption(true);
    }

    private void doRetrieve_object_withOption(final boolean isFieldAccess) {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID > :id ORDER BY ENTITY_ID", new SelectOption(2, 3));
        Object condition;
        if(!isFieldAccess) {
            final TestEntity entity = new TestEntity();
            entity.id = "1";
            condition = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "1";
            condition = entity;
        }
        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("3レコード取得できること", actual.size(), is(3));
        assertThat(actual.get(0)
                .getString("entityId"), is("10002"));
        assertThat(actual.get(1)
                .getString("entityId"), is("10003"));
        assertThat(actual.get(2)
                .getString("entityId"), is("10004"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}のテスト。
     */
    @Test
    public void retrieve_object_withOffsetAndLimit() throws Exception {
        doRetrieve_object_withOffsetAndLimit(false);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}のテスト。(Fieldアクセステスト用)
     */
    @Test
    public void retrieve_object_withOffsetAndLimitViaFieldAccess() throws Exception {
        doRetrieve_object_withOffsetAndLimit(false);
    }

    private void doRetrieve_object_withOffsetAndLimit(final boolean isFieldAccess) {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id ORDER BY ENTITY_ID");
        Object condition;
        if (!isFieldAccess) {
            final TestEntity entity = new TestEntity();
            entity.id = "10002";
            condition = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "10002";
            condition = entity;
        }

        final SqlResultSet firstRow = sut.retrieve(1, 1, condition);
        assertThat("1レコード取得できること", firstRow.size(), is(1));
        assertThat(firstRow.get(0)
                .getString("entityId"), is("10001"));

        final SqlResultSet secondRow = sut.retrieve(2, 1, condition);
        assertThat("1レコード取得できること", secondRow.size(), is(1));
        assertThat(secondRow.get(0)
                .getString("entityId"), is("10002"));

        final SqlResultSet multiRow = sut.retrieve(1, 2, condition);
        assertThat("2レコード取得できること", multiRow.size(), is(2));
        assertThat(multiRow.get(0)
                .getString("entityId"), is("10001"));
        assertThat(multiRow.get(1)
                .getString("entityId"), is("10002"));
    }

    @Test(expected = IllegalOperationException.class)
    public void retrieve_object_withDuplicatePagenate() {
        doRetrieve_object_withDuplicatePagenate(false);
    }

    @Test(expected = IllegalOperationException.class)
    public void retrieve_object_withDuplicatePagenateViaFieldAccess() {
        doRetrieve_object_withDuplicatePagenate(true);
    }

    private void doRetrieve_object_withDuplicatePagenate(final boolean isFieldAccess)
    {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID > :id ORDER BY ENTITY_ID", new SelectOption(2, 3));
        Object condition;
        if (!isFieldAccess) {
            final TestEntity entity = new TestEntity();
            entity.id = "1";
            condition = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "1";
            condition = entity;
        }
        sut.retrieve(1, 2, condition);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}のSQLログのテスト。
     */
    @Test
    public void retrieve_object_writeSqlLog() throws Exception {
        doRetrieve_object_writeSqlLog(false);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}のSQLログのテスト。(Fieldアクセステスト用)
     */
    @Test
    public void retrieve_object_writeSqlLogViaFieldAccess() throws Exception {
        doRetrieve_object_writeSqlLog(true);
    }

    private void doRetrieve_object_writeSqlLog(final boolean isFieldAccess) {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id ORDER BY ENTITY_ID");
        Object condition;
        if (!isFieldAccess) {
            final TestEntity entity = new TestEntity();
            entity.id = "10002";
            condition = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "10002";
            condition = entity;
        }
        sut.setFetchSize(123);
        sut.setQueryTimeout(30);
        sut.retrieve(condition);

        assertLog("開始ログ",
                Pattern.compile("\\Qnablarch.core.db.statement.BasicSqlPStatement#retrieve"
                        + " SQL = [SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= ? ORDER BY ENTITY_ID]"
                        + Logger.LS
                        + "\tstart position = [1] size = [0] queryTimeout = [30] fetchSize = [123]" + Logger.LS
                        + "\toriginal sql = [SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id ORDER BY ENTITY_ID]\\E"));

        assertLog("パラメータ",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\tid = [10002]\\E"));

        assertLog("終了ログ",
                Pattern.compile("nablarch.core.db.statement.BasicSqlPStatement#retrieve" + Logger.LS
                        + "\texecute time\\(ms\\) = \\[[0-9]+\\] retrieve time\\(ms\\) = \\[[0-9]+\\] "));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}の前方一致のテスト
     */
    @Test
    public void retrieve_object_with_likeForwardMatch() throws Exception {
        doRetrieve_object_with_likeForwardMatch(false);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}の前方一致のテスト (Fieldアクセステスト用)
     */
    @Test
    public void retrieve_object_with_likeForwardMatchViaFieldAccess() throws Exception {
        doRetrieve_object_with_likeForwardMatch(true);
    }

    private void doRetrieve_object_with_likeForwardMatch(final boolean isFieldAccess) {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :id% ORDER BY ENTITY_ID");
        final Object condition;
        if(!isFieldAccess) {
            final TestEntity entity = new TestEntity();
            entity.id = "1";
            condition = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "1";
            condition = entity;
        }
        final SqlResultSet actual = sut.retrieve(1, 2, condition);
        assertThat(actual.size(), is(2));
        assertThat(actual.get(0)
                .getString("entityId"), is("10001"));
        assertThat(actual.get(1)
                .getString("entityId"), is("10002"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}の部分一致のテスト
     */
    @Test
    public void retrieve_object_with_likePartialMatch() throws Exception {
        doRetrieve_object_with_likePartialMatch(false);
    }


    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}の部分一致のテスト (Fieldアクセステスト用)
     */
    @Test
    public void retrieve_object_with_likePartialMatchViaFieldAccess() throws Exception {
        doRetrieve_object_with_likePartialMatch(true);
    }

    private void doRetrieve_object_with_likePartialMatch(final boolean isFieldAccess) {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :%id% ORDER BY ENTITY_ID");
        final Object condition;
        if(!isFieldAccess) {
            final TestEntity entity = new TestEntity();
            entity.id = "000";
            condition = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "000";
            condition = entity;
        }
        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(4));

        assertThat(actual.get(0)
                .getString("entityId"), is("10001"));
        assertThat(actual.get(1)
                .getString("entityId"), is("10002"));
        assertThat(actual.get(2)
                .getString("entityId"), is("10003"));
        assertThat(actual.get(3)
                .getString("entityId"), is("10004"));

    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}の後方一致のテスト
     */
    @Test
    public void retrieve_object_with_likeBackwardMatch() throws Exception {
        //doRetrieve_object_with_likeBackwardMatch
        doRetrieve_object_with_likeBackwardMatch(false);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}の後方一致のテスト (Fieldアクセステスト用)
     */
    @Test
    public void retrieve_object_with_likeBackwardMatchViaFieldAccess() throws Exception {
        //doRetrieve_object_with_likeBackwardMatch
        doRetrieve_object_with_likeBackwardMatch(true);
    }

    private void doRetrieve_object_with_likeBackwardMatch(final boolean isFieldAccess) {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :%id ORDER BY ENTITY_ID");
        final Object condition;
        if (!isFieldAccess) {
            final TestEntity entity = new TestEntity();
            entity.id = "0002";
            condition = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "0002";
            condition = entity;
        }
        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));

        assertThat(actual.get(0)
                .getString("entityId"), is("10002"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}のLIKE検索でエスケープ対象の文字が含まれている場合
     */
    @Test
    public void retrieve_object_with_likeWithEscapeChar() throws Exception {
        doRetrieve_object_with_likeWithEscapeChar(false);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}のLIKE検索でエスケープ対象の文字が含まれている場合 (Fieldアクセステスト用)
     */
    @Test
    public void retrieve_object_with_likeWithEscapeCharViaFieldAccess() throws Exception {
        doRetrieve_object_with_likeWithEscapeChar(true);
    }

    private void doRetrieve_object_with_likeWithEscapeChar(final boolean isFieldAccess) {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL LIKE :id% ORDER BY ENTITY_ID");
        final Object condition;
        if (!isFieldAccess) {
            final TestEntity entity = new TestEntity();
            entity.id = "c_\\";
            condition = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "c_\\";
            condition = entity;
        }
        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));

        assertThat(actual.get(0)
                .getString("entityId"), is("10003"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}で、条件の名前付きパラメータがオブジェクトのフィールド
     * に存在しない場合、例外が送出されること。
     */
    @Test
    public void retrieve_object_invalidProperty() throws Exception {
        doRetrieve_object_invalidProperty(false);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}で、条件の名前付きパラメータがオブジェクトのフィールド (Fieldアクセステスト用)
     * に存在しない場合、例外が送出されること。
     */
    @Test
    public void retrieve_object_invalidPropertyViaFieldAccess() throws Exception {
        doRetrieve_object_invalidProperty(true);
    }

    private void doRetrieve_object_invalidProperty(final boolean isFieldAccess) {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id AND VARCHAR_COL = :invalid ORDER BY ENTITY_ID");
        final Object condition;
        if (!isFieldAccess) {
            final TestEntity entity = new TestEntity();
            entity.id = "0002";
            condition = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "0002";
            condition = entity;
        }
        try {
            sut.retrieve(condition);
            fail("とおらない");
        } catch (IllegalArgumentException e) {
            assertThat("エラーになる。", e.getMessage(),
                    containsString("SQL parameter was not found in Object. parameter name=[invalid]"));
        }
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}で、条件の名前付きパラメータがMapに存在しない場合、
     * 例外が送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void retrieve_object_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doRetrieve_object_SQLException(mockStatement, false);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}で、条件の名前付きパラメータがMapに存在しない場合、
     * 例外が送出されること。(Fieldアクセステスト用)
     */
    @Test(expected = DbAccessException.class)
    public void retrieve_object_SQLExceptionViaFieldAccess() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doRetrieve_object_SQLException(mockStatement, true);
    }

    private void doRetrieve_object_SQLException(final PreparedStatement mockStatement, final boolean isFieldAccess) throws SQLException {
        doThrow(new SQLException("retrieve map error")).when(mockStatement).setObject(anyInt(), any());
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id AND VARCHAR_COL = :varcharCol ORDER BY ENTITY_ID");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        final Object condition;
        if (!isFieldAccess) {
            final TestEntity entity = new TestEntity();
            entity.id = "10002";
            entity.varcharCol = "b";
            condition = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "10002";
            entity.varcharCol = "b";
            condition = entity;
        }
        sut.retrieve(condition);
    }

    /**
     * {@link BasicSqlPStatement#executeQueryByMap(Map)}のテスト。
     */
    @Test
    public void executeQueryByMap() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10002");

        final ResultSetIterator actual = sut.executeQueryByMap(condition);
        assertThat(actual.next(), is(true));
        assertThat(actual.getRow()
                .getString("entityId"), is("10002"));
        assertThat(actual.next(), is(false));
    }

    /**
     * {@link BasicSqlPStatement#executeQueryByMap(Map)}のSQLExceptionのテスト。
     */
    @Test(expected = DbAccessException.class)
    public void executeQueryByMap_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("executeQuery map error")).when(mockStatement).setObject(anyInt(), any());
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id");

        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10002");
        sut.executeQueryByMap(condition);
    }

    /**
     * {@link BasicSqlPStatement#executeQueryByObject(Object)}
     */
    @Test
    public void executeQueryByObject() throws Exception {
        doExecuteQueryByObject(false);
    }

    /**
     * {@link BasicSqlPStatement#executeQueryByObject(Object)}  (Fieldアクセステスト用)
     * @throws Exception
     */
    @Test
    public void executeQueryByObjectViaFieldAccess() throws Exception {
        doExecuteQueryByObject(true);
    }

    private void doExecuteQueryByObject(final boolean isFieldAccess)
    {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id");
        final Object data;
        if(!isFieldAccess){
            TestEntity entity = new TestEntity();
            entity.id = "10002";
            data = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            TestFieldEntity entity = new TestFieldEntity();
            entity.id = "10002";
            data = entity;
        }
        final ResultSetIterator actual = sut.executeQueryByObject(data);
        assertThat(actual.next(), is(true));
        assertThat(actual.getRow().getString("entityId"), is("10002"));
        assertThat(actual.next(), is(false));
    }

    /**
     * {@link BasicSqlPStatement#executeQueryByObject(Object)}のSQLExceptionのテスト。
     */
    @Test(expected = SqlStatementException.class)
    public void executeQueryByObject_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doExecuteQueryByObject_SQLException(mockStatement, false);
    }

    /**
     * {@link BasicSqlPStatement#executeQueryByObject(Object)}のSQLExceptionのテスト。(Fieldアクセステスト用)
     */
    @Test(expected = SqlStatementException.class)
    public void executeQueryByObject_SQLExceptionViaFieldAccess() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doExecuteQueryByObject_SQLException(mockStatement, true);
    }

    private void doExecuteQueryByObject_SQLException(final PreparedStatement mockStatement, final boolean isFieldAccess) throws
            SQLException {
        doThrow(new SQLException("executeQuery object error")).when(mockStatement).setObject(anyInt(), any());
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);

        final Object data;
        if(!isFieldAccess){
            final TestEntity entity = new TestEntity();
            entity.id = "10002";
            data = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "10002";
            data = entity;
        }

        sut.executeQueryByObject(data);
    }

    /**
     * {@link BasicSqlPStatement#executeUpdateByMap(Map)}のテスト。
     */
    @Test
    public void executeUpdateByMap() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL, LOCAL_DATE_COL, LOCAL_DATE_TIME_COL) VALUES (:id, :long, :binary, :localDate, :localDateTime)");
        LocalDate localdate = LocalDate.parse("2015-04-01");
        LocalDateTime localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        Map<String, Object> insertData = new HashMap<String, Object>();
        insertData.put("id", "44444");
        insertData.put("long", 100L);
        insertData.put("binary", new byte[]{0x00, 0x01});
        insertData.put("localDate", localdate);
        insertData.put("localDateTime", localDateTime);
        final int result = sut.executeUpdateByMap(insertData);

        assertThat("1レコード登録される", result, is(1));
        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "44444");
        assertThat(actual.id, is("44444"));
        assertThat(actual.longCol, is(100L));
        assertThat(actual.binaryCol, is(new byte[] {0x00, 0x01}));
        assertThat(actual.localDateCol, is(localdate));
        assertThat(actual.localDateTimeCol, is(localDateTime));
    }

    /**
     * {@link BasicSqlPStatement#executeUpdateByMap(Map)}でSQLExceptionが発生する場合、
     * SqlStatementExceptionが送出されること。
     */
    @Test(expected = SqlStatementException.class)
    public void executeUpdateByMap_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("executeUpdate map error")).when(mockStatement).setObject(anyInt(), any());
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL) VALUES (:id, :long, :binary)");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        Map<String, Object> insertData = new HashMap<String, Object>();
        insertData.put("id", "44444");
        insertData.put("long", 100L);
        insertData.put("binary", new byte[] {0x00, 0x01});
        sut.executeUpdateByMap(insertData);
    }

    /**
     * {@link BasicSqlPStatement#executeUpdateByObject(Object)}のテスト。
     */
    @Test
    public void executeUpdateByObject() throws Exception {
        doExecuteUpdateByObject(false);
    }

    /**
     * {@link BasicSqlPStatement#executeUpdateByObject(Object)}のテスト。(Fieldアクセステスト用)
     */
    @Test
    public void executeUpdateByObjectViaFieldAccess() throws Exception {
        doExecuteUpdateByObject(true);
    }

    protected void doExecuteUpdateByObject(final boolean isFieldAccess) {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL, LOCAL_DATE_COL, LOCAL_DATE_TIME_COL) VALUES (:id, :longCol, :binaryCol, :localDateCol, :localDateTimeCol)");
        final Object data;
        final TestEntity entity = new TestEntity();
        entity.id = "44444";
        entity.longCol = 100L;
        entity.binaryCol = new byte[] {0x00, 0x01};
        entity.localDateCol = LocalDate.parse("2015-04-01");
        entity.localDateTimeCol =  LocalDateTime.parse("2015-04-01T12:34:56");
        if(!isFieldAccess) {
            data = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity fieldEntity = new TestFieldEntity();
            fieldEntity.id = "44444";
            fieldEntity.longCol = 100L;
            fieldEntity.binaryCol = new byte[] {0x00, 0x01};
            fieldEntity.localDateCol = LocalDate.parse("2015-04-01");
            fieldEntity.localDateTimeCol =  LocalDateTime.parse("2015-04-01T12:34:56");
            data = fieldEntity;
        }
        final int result = sut.executeUpdateByObject(data);
        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "44444");
        assertThat(actual.id, is(entity.id));
        assertThat(actual.longCol, is(entity.longCol));
        assertThat(actual.binaryCol, is(entity.binaryCol));
        assertThat(actual.localDateCol, is(entity.localDateCol));
        assertThat(actual.localDateTimeCol, is(entity.localDateTimeCol));
    }

    /**
     * {@link BasicSqlPStatement#executeUpdateByObject(Object)}のテスト。
     */
    @Test(expected = SqlStatementException.class)
    public void executeUpdateByObject_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doExecuteUpdateByObject_SQLException(mockStatement, false);
    }

    /**
     * {@link BasicSqlPStatement#executeUpdateByObject(Object)}のテスト。(Fieldアクセステスト用)
     */
    @Test(expected = SqlStatementException.class)
    public void executeUpdateByObject_SQLExceptionViaFieldAccess() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doExecuteUpdateByObject_SQLException(mockStatement, true);
    }

    private void doExecuteUpdateByObject_SQLException(final PreparedStatement mockStatement, final boolean isFieldAccess) throws
            SQLException {
        doThrow(new SQLException("executeUpdate object error")).when(mockStatement).setObject(anyInt(), any());
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL) VALUES (:id, :long, :binary)");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        final Object data;
        if(!isFieldAccess){
            final TestEntity entity = new TestEntity();
            entity.id = "44444";
            entity.longCol = 100L;
            entity.binaryCol = new byte[]{0x00, 0x01};
            data = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "44444";
            entity.longCol = 100L;
            entity.binaryCol = new byte[]{0x00, 0x01};
            data = entity;
        }
        sut.executeUpdateByObject(data);
    }

    /**
     * {@link BasicSqlPStatement#addBatchMap(Map)}のテスト。
     */
    @Test
    public void addBatchMap() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL) VALUES (:id, :long, :binary)");
        Map<String, Object> insertData = new HashMap<String, Object>();
        insertData.put("id", "44444");
        insertData.put("long", 100L);
        insertData.put("binary", new byte[]{0x00, 0x01});
        sut.addBatchMap(insertData);
        assertThat("バッチサイズがインクリメントされること", sut.getBatchSize(), is(1));

        insertData.put("id", "55555");
        insertData.put("long", Long.MAX_VALUE);
        sut.addBatchMap(insertData);
        assertThat("バッチサイズがインクリメントされること", sut.getBatchSize(), is(2));

        sut.executeBatch();

        dbCon.commit();

        final TestEntity actual1 = VariousDbTestHelper.findById(TestEntity.class, "44444");
        assertThat(actual1.id, is("44444"));
        assertThat(actual1.longCol, is(100L));

        final TestEntity actual2 = VariousDbTestHelper.findById(TestEntity.class, "55555");
        assertThat(actual2.id, is("55555"));
        assertThat(actual2.longCol, is(Long.MAX_VALUE));
    }

    /**
     * {@link BasicSqlPStatement#addBatchMap(Map)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void addBatchMap_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("addBatch map error")).when(mockStatement).setObject(anyInt(), any());
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL) VALUES (:id, :long, :binary)");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        Map<String, Object> insertData = new HashMap<String, Object>();
        insertData.put("id", "44444");
        insertData.put("long", 100L);
        insertData.put("binary", new byte[]{0x00, 0x01});
        sut.addBatchMap(insertData);
    }


    /**
     * {@link BasicSqlPStatement#addBatchObject(Object)}のテスト。
     */
    @Test
    public void addBatchObject() throws Exception {
        doAddBatchObject(false);
    }

    /**
     * {@link BasicSqlPStatement#addBatchObject(Object)}のテスト。(Fieldアクセステスト用)
     * @throws Exception
     */
    @Test
    public void addBatchObjectViaFieldAccess() throws Exception {
        doAddBatchObject(true);
    }

    private void doAddBatchObject(boolean isFieldAccess) {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL) VALUES (:id, :longCol, :binaryCol)");
        if(!isFieldAccess) {
            final TestEntity entity = new TestEntity();
            entity.id = "44444";
            entity.longCol = 100L;
            entity.binaryCol = new byte[] {0x00, 0x01};
            sut.addBatchObject(entity);
            assertThat("バッチサイズがインクリメントされる", sut.getBatchSize(), is(1));

            entity.id = "55555";
            entity.longCol = Long.MAX_VALUE;
            sut.addBatchObject(entity);
            assertThat("バッチサイズがインクリメントされる", sut.getBatchSize(), is(2));
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "44444";
            entity.longCol = 100L;
            entity.binaryCol = new byte[] {0x00, 0x01};
            sut.addBatchObject(entity);
            assertThat("バッチサイズがインクリメントされる", sut.getBatchSize(), is(1));

            entity.id = "55555";
            entity.longCol = Long.MAX_VALUE;
            sut.addBatchObject(entity);
            assertThat("バッチサイズがインクリメントされる", sut.getBatchSize(), is(2));
        }
        sut.executeBatch();
        dbCon.commit();

        final TestEntity actual1 = VariousDbTestHelper.findById(TestEntity.class, "44444");
        assertThat(actual1.id, is("44444"));
        assertThat(actual1.longCol, is(100L));

        final TestEntity actual2 = VariousDbTestHelper.findById(TestEntity.class, "55555");
        assertThat(actual2.id, is("55555"));
        assertThat(actual2.longCol, is(Long.MAX_VALUE));
    }

    /**
     * {@link BasicSqlPStatement#addBatchObject(Object)}でSQLExcepitonが発生した場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void addBatchObject_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doAddBatchObject_SQLException(mockStatement, false);
    }

    /**
     * {@link BasicSqlPStatement#addBatchObject(Object)}でSQLExcepitonが発生した場合、
     * DbAccessExceptionが送出されること。(Fieldアクセステスト用)
     */
    @Test(expected = DbAccessException.class)
    public void addBatchObject_SQLExceptionViaFieldAccess() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doAddBatchObject_SQLException(mockStatement, true);
    }

    private void doAddBatchObject_SQLException(final PreparedStatement mockStatement, final boolean isFieldAccess) throws
            SQLException {
        doThrow(new SQLException("addBatch object error")).when(mockStatement).setObject(anyInt(), any());
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL) VALUES (:id, :long, :binary)");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        final Object data;
        if(!isFieldAccess){
            final TestEntity entity = new TestEntity();
            entity.id = "12345";
            data = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "12345";
            data = entity;
        }
        sut.addBatchObject(data);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}で可変条件(IF文)を持つSQLの場合
     */
    @Test
    public void retrieve_withIfCondition() throws Exception {
        doRetrieve_withIfCondition(false);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}で可変条件(IF文)を持つSQLの場合(Fieldアクセステスト用)
     */
    @Test
    public void retrieve_withIfConditionViaFieldAccess() throws Exception {
        doRetrieve_withIfCondition(true);
    }

    private void doRetrieve_withIfCondition(final boolean isFieldAccess) {
        final Object condition;
        if(!isFieldAccess){
            final TestEntity entity = new TestEntity();
            entity.id = "1";
            condition = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.id = "1";
            condition = entity;
        }
        ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :id% AND $if(varcharCol) {VARCHAR_COL = :varcharCol}",
                condition);

        SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(4));

        if(!isFieldAccess){
            final TestEntity entity = (TestEntity) condition;
            entity.varcharCol = "";
        } else {
            final TestFieldEntity entity = (TestFieldEntity) condition;
            entity.varcharCol = "";
        }
        sut = dbCon.prepareParameterizedSqlStatement(
                "select * from STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :id% AND $if(varcharCol) {VARCHAR_COL = :varcharCol}",
                condition);
        actual = sut.retrieve(condition);
        assertThat(actual.size(), is(4));

        if(!isFieldAccess){
            final TestEntity entity = (TestEntity) condition;
            entity.varcharCol = "a";
        } else {
            final TestFieldEntity entity = (TestFieldEntity) condition;
            entity.varcharCol = "a";
        }
        sut = dbCon.prepareParameterizedSqlStatement(
                "select * from STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :id% AND $if(varcharCol) {VARCHAR_COL = :varcharCol}",
                condition);
        actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}でIN条件を持つSQL文の場合
     */
    @Test
    public void retrieve_withInCondition() throws Exception {
        doRetrieve_withInCondition(false);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}でIN条件を持つSQL文の場合(Fieldアクセステスト用)
     */
    @Test
    public void retrieve_withInConditionViaFieldAccess() throws Exception {
        doRetrieve_withInCondition(true);
    }

    private void doRetrieve_withInCondition(final boolean isFieldAccess) {
        final Object condition;
        if(!isFieldAccess){
            final TestEntity entity = new TestEntity();
            entity.varchars = new String[]{"a", "b"};
            condition = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.varchars = new String[]{"a", "b"};
            condition = entity;
        }
        ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL IN (:varchars[]) ORDER BY ENTITY_ID",
                condition);
        SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(2));
        assertThat(actual.get(0)
                .getString("varcharcol"), is("a"));
        assertThat(actual.get(1)
                .getString("varcharcol"), is("b"));
        OnMemoryLogWriter.assertLogContains("writer.memory", "varchars[0] = [a]");
        OnMemoryLogWriter.assertLogContains("writer.memory", "varchars[1] = [b]");

        sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL IN (:varchars[1]) ORDER BY ENTITY_ID",
                condition);
        actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));
        assertThat(actual.get(0)
                .getString("varcharCol"), is("b"));
        OnMemoryLogWriter.assertLogContains("writer.memory", "varchars[1] = [b]");

        if(!isFieldAccess){
            final TestEntity entity = (TestEntity) condition;
            entity.varchars = new String[0];
        } else {
            final TestFieldEntity entity = (TestFieldEntity) condition;
            entity.varchars = new String[0];
        }
        sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL IN (:varchars[]) ORDER BY ENTITY_ID",
                condition);
        actual = sut.retrieve(condition);
        assertThat(actual.size(), is(0));
        OnMemoryLogWriter.assertLogContains("writer.memory", "varchars[] = [null]");

        if(!isFieldAccess){
            final TestEntity entity = (TestEntity) condition;
            entity.varchars = null;
        } else {
            final TestFieldEntity entity = (TestFieldEntity) condition;
            entity.varchars = null;
        }

        sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL IN (:varchars[]) ORDER BY ENTITY_ID",
                condition);
        actual = sut.retrieve(condition);
        assertThat(actual.size(), is(0));
        OnMemoryLogWriter.assertLogContains("writer.memory", "varchars[] = [null]");
    }
    
    public static class ListCondition {
        private List<String> inCondition;

        public ListCondition(final List<String> inCondition) {
            this.inCondition = inCondition;
        }

        public List<String> getInCondition() {
            return inCondition;
        }
    }
    
    /**
     * {@link BasicSqlPStatement#retrieve(Object)}でIN条件を持つSQL文にListを条件で指定するテスト。
     */
    @Test
    public void retrieve_withListCondition() throws Exception {
        final ListCondition condition = new ListCondition(Arrays.asList("a", "b"));
        final ParameterizedSqlPStatement statement = dbCon.prepareParameterizedSqlStatement(
                "select * from statement_test_table where varchar_col in (:inCondition[]) order by entity_id",
                condition);
        final SqlResultSet actual = statement.retrieve(condition);

        assertThat(actual, hasSize(2));
        assertThat(actual.get(0).getString("varcharCol"), is("a"));
        assertThat(actual.get(1).getString("varcharCol"), is("b"));

        OnMemoryLogWriter.assertLogContains("writer.memory", "inCondition[0] = [a]");
        OnMemoryLogWriter.assertLogContains("writer.memory", "inCondition[1] = [b]");
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}でIN条件を持つSQL文のにListを条件で指定するテスト(Fieldアクセステスト用)
     */
    @Test
    public void retrieve_withListConditionViaFieldAccess() throws Exception {
        setFieldAccessMode();
        final ListCondition condition = new ListCondition(Arrays.asList("c_\\(like検索用)", "b"));
        final ParameterizedSqlPStatement statement = dbCon.prepareParameterizedSqlStatement(
                "select * from statement_test_table where varchar_col in (:inCondition[]) order by entity_id",
                condition);
        final SqlResultSet actual = statement.retrieve(condition);

        assertThat(actual, hasSize(2));
        assertThat(actual.get(0).getString("varcharCol"), is("b"));
        assertThat(actual.get(1).getString("varcharCol"), is("c_\\(like検索用)"));

        OnMemoryLogWriter.assertLogContains("writer.memory", "inCondition[0] = [c_\\(like検索用)]");
        OnMemoryLogWriter.assertLogContains("writer.memory", "inCondition[1] = [b]");
    }
    
    public static class ArrayCondition {
        private int[] cond;

        public ArrayCondition(final int[] cond) {
            this.cond = cond;
        }

        public int[] getCond() {
            return cond;
        }
    }
    
    /**
     * {@link BasicSqlPStatement#retrieve(Object)}でIN条件を持つSQL文に配列を条件で指定するテスト。
     */
    @Test
    public void retrieve_withArrayCondition() throws Exception {
        final ArrayCondition condition = new ArrayCondition(new int[] {30000, 10000});
        final ParameterizedSqlPStatement statement = dbCon.prepareParameterizedSqlStatement(
                "select * from statement_test_table where long_col in (:cond[]) order by entity_id",
                condition);
        final SqlResultSet actual = statement.retrieve(condition);

        assertThat(actual, hasSize(2));
        assertThat(actual.get(0).getLong("longCol"), is(10000L));
        assertThat(actual.get(1).getLong("longCol"), is(30000L));

        OnMemoryLogWriter.assertLogContains("writer.memory", "cond[0] = [30000]");
        OnMemoryLogWriter.assertLogContains("writer.memory", "cond[1] = [10000]");
    }
    
    /**
     * {@link BasicSqlPStatement#retrieve(Object)}でIN条件を持つSQL文に配列を条件で指定するテスト。(Fieldアクセステスト用)
     */
    @Test
    public void retrieve_withArrayConditionViaFieldAccess() throws Exception {
        setFieldAccessMode();
        final ArrayCondition condition = new ArrayCondition(new int[] {30000, 10000});
        final ParameterizedSqlPStatement statement = dbCon.prepareParameterizedSqlStatement(
                "select * from statement_test_table where long_col in (:cond[]) order by entity_id",
                condition);
        final SqlResultSet actual = statement.retrieve(condition);

        assertThat(actual, hasSize(2));
        assertThat(actual.get(0).getLong("longCol"), is(10000L));
        assertThat(actual.get(1).getLong("longCol"), is(30000L));

        OnMemoryLogWriter.assertLogContains("writer.memory", "cond[0] = [30000]");
        OnMemoryLogWriter.assertLogContains("writer.memory", "cond[1] = [10000]");
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}でIN条件が不正な場合
     */
    @Test(expected = IllegalArgumentException.class)
    public void retrieve_invalidInCondition() throws Exception {
        doRetrieve_invalidInCondition(false);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}でIN条件が不正な場合 (Fieldアクセステスト用)
     */
    @Test(expected = IllegalArgumentException.class)
    public void retrieve_invalidInConditionViaFieldAccess() throws Exception {
        doRetrieve_invalidInCondition(true);
    }

    private void doRetrieve_invalidInCondition(final boolean isFieldAccess) {
        final Object condition;
        if(!isFieldAccess){
            final TestEntity entity = new TestEntity();
            entity.varchars = new String[]{"a", "b"};
            condition = entity;
        } else {
            setFieldAccessMode();//Fieldアクセスモードに変更
            final TestFieldEntity entity = new TestFieldEntity();
            entity.varchars = new String[]{"a", "b"};
            condition = entity;
        }
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL IN (:varchars[9999999999]) ORDER BY ENTITY_ID",
                condition);
        sut.retrieve(condition);
    }

    /**
     * {@link BasicSqlPStatement#getGeneratedKeys()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getGeneratedKeys_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(mockStatement.getGeneratedKeys()).thenThrow(new SQLException("getGeneratedKeys error"));
        SqlPStatement sut = dbCon.prepareStatement("INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES ('12345')");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.getGeneratedKeys();
    }

    /**
     * {@link BasicSqlPStatement#getGeneratedKeys()}のテスト。
     */
    @Test
    @TargetDb(exclude = {TargetDb.Db.SQL_SERVER, TargetDb.Db.H2})
    public void getGeneratedKeys() throws Exception {
        final Connection connection = VariousDbTestHelper.getNativeConnection();
        String pkName = "entity_id";
        try {
            final DatabaseMetaData data = connection.getMetaData();
            if (!data.storesMixedCaseIdentifiers() && data.storesUpperCaseIdentifiers()) {
                pkName = "ENTITY_ID";
            }
        } finally {
            connection.close();
        }
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE(ENTITY_ID) VALUES (?)", new String[] {pkName});
        sut.setString(1, "12345");
        final int inserted = sut.executeUpdate();
        assertThat(inserted, is(1));

        final ResultSet actual = sut.getGeneratedKeys();
        try {
            assertThat(actual.next(), is(true));
            assertThat(actual.getString(1), is("12345"));
        } finally {
            actual.close();
        }
    }

    /**
     * {@link BasicSqlPStatement#getGeneratedKeys()}のテスト。
     *
     * ※SQLServerは、自動生成キーは自動生成カラムのみ対応
     */
    @Test
    @TargetDb(include = {TargetDb.Db.SQL_SERVER})
    public void getGeneratedKeys_SQLServer() throws Exception {
        VariousDbTestHelper.createTable(SqlServerTestEntity.class);
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO statement_test_sqlserver(name) VALUES (?)", new String[] {"id"});
        sut.setString(1, "name");
        final int inserted = sut.executeUpdate();
        assertThat(inserted, is(1));

        final ResultSet actual = sut.getGeneratedKeys();
        try {
            assertThat(actual.next(), is(true));
            assertThat(actual.getLong(1), is(1L));
        } finally {
            actual.close();
        }
    }

    /**
     * {@link BasicSqlPStatement#clearParameters()} のテスト。
     */
    @Test
    public void clearParameters() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.setString(1, "10001");
        sut.clearParameters();

        verify(mockStatement).clearParameters();
    }

    /**
     * {@link BasicSqlPStatement#clearParameters()}でSQLExceptionが発生する場合
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void clearParameters_SQLException() throws Exception {
        final PreparedStatement mockStatement = mock(PreparedStatement.class);

        doThrow(new SQLException("clearParameters error")).when(mockStatement).clearParameters();
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        ReflectionUtil.setFieldValue(sut, "statement", mockStatement);
        sut.clearParameters();
    }

    /**
     * プレフックハンドラが未設定の場合でもエラーとならずに処理が実行出来ること。
     */
    @Test
    public void preHookHandlerWasNull() throws Exception {
        final BasicSqlPStatement sut = (BasicSqlPStatement) dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id");
        sut.setUpdatePreHookObjectHandlerList(null);

        final ParamObject object = new ParamObject();
        final SqlResultSet rs = sut.retrieve(object);
        assertThat("検索できること", rs, is(notNullValue()));
    }

    private void assertLog(String msg, Pattern pattern) {

        List<String> logMessages = OnMemoryLogWriter.getMessages("writer.memory");
        for (String message : logMessages) {
            final Matcher matcher = pattern.matcher(message);
            if (matcher.find()) {
                return;
            }
        }
        throw new AssertionError(MessageFormat.format("expected log pattern not found. message = {0}\n"
                        + " expected pattern = [{1}]\n"
                        + " actual log messages = [\n{2}\n]",
                msg,
                pattern.pattern(),
                logMessages.toString()));
    }

    /**
     * SQLの関数を使ったときに小数(BigDecimal)で取得できること。
     */
    @Test
    public void testUseSqlFunctionBigDecimal() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT  sum(1.1) RET FROM STATEMENT_TEST_TABLE"
        );
        SqlResultSet actual = sut.retrieve();

        assertThat(actual.get(0).getBigDecimal("ret"), is(new BigDecimal("4.4")));
    }

    /**
     * SQLの関数を使ったときに整数(Integer)で取得できること。
     */
    @Test
    public void testUseSqlFunctionInteger() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT sum(10) RET FROM STATEMENT_TEST_TABLE"
        );
        SqlResultSet actual = sut.retrieve();

        assertThat(actual.get(0).getInteger("ret"), is(40));
    }

    /**
     * SQLの関数を使ったときに整数(Long)で取得できること。
     */
    @Test
    public void testUseSqlFunctionLong() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT sum(10000000000) RET FROM STATEMENT_TEST_TABLE"
        );
        SqlResultSet actual = sut.retrieve();

        assertThat(actual.get(0).getLong("ret"), is(40000000000L));
    }

    /**
     * CLOBカラムが登録できることをテストする。
     * @throws Exception
     */
    @Test
    @TargetDb(include = {TargetDb.Db.ORACLE, TargetDb.Db.DB2, TargetDb.Db.H2})
    public void testClobColumn() throws Exception {
        VariousDbTestHelper.createTable(ClobColumn.class);
        final SqlPStatement sut = dbCon.prepareStatement(
                "insert into clob_table (id, clob_col) values (99999, ?)");
        sut.setObject(1, "input");
        sut.executeUpdate();
        dbCon.commit();

        final ClobColumn result = VariousDbTestHelper.findById(ClobColumn.class, "99999");
        assertThat(result.clob, is("input"));
    }
    
    /**
     * TEXTカラムが登録できることをテストする。
     * @throws Exception
     */
    @Test
    @TargetDb(exclude = {TargetDb.Db.ORACLE, TargetDb.Db.DB2})
    public void testTextColumn() throws Exception {
        VariousDbTestHelper.createTable(TextColumn.class);
        final SqlPStatement sut = dbCon.prepareStatement(
                "insert into text_table (id, text_col) values (99999, ?)");
        sut.setObject(1, "input");
        sut.executeUpdate();
        dbCon.commit();

        final TextColumn result = VariousDbTestHelper.findById(TextColumn.class, "99999");
        assertThat(result.text, is("input"));
    }

    /**
     * 任意のDialectをテスト用のコネクションに設定する。
     */
    private void setDialect(TransactionManagerConnection dbCon, Dialect dialect) {
        ((BasicDbConnection)dbCon).setContext(new DbExecutionContext(dbCon, dialect, TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
    }

    private static class OffsetSupportDialcet extends DefaultDialect {
        private String convertedSql;
        private OffsetSupportDialcet(String convertedSql) {
            this.convertedSql = convertedSql;
        }
        @Override
        public boolean supportsOffset() {
            return true;
        }
        @Override
        public String convertPaginationSql(String sql,
                SelectOption selectOption) {
            return convertedSql;
        }
    }
}


