package nablarch.core.db.connection;

import nablarch.core.db.DbAccessException;
import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.db.dialect.Dialect;
import nablarch.core.db.statement.BasicStatementFactory;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.transaction.TransactionContext;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link nablarch.core.db.connection.BasicDbConnectionFactoryForDataSource}のテストクラス。
 *
 * @author Hisaaki Sioiri
 */
public class BasicDbConnectionFactoryForDataSourceTest {

    public DataSource dataSource = mock(DataSource.class);

    public Connection con = mock(Connection.class, RETURNS_DEEP_STUBS);

    public DbAccessExceptionFactory dbAccessExceptionFactory = mock(DbAccessExceptionFactory.class);

    private static final String CONNECTION_NAME = TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY;

    /**
     * {@link BasicDbConnectionFactoryForDataSource#getConnection()} のテスト。
     * @throws Exception
     */
    @Test
    public void testGetConnection() throws Exception {
        when(dataSource.getConnection()).thenReturn(con);

        BasicDbConnectionFactoryForDataSource factory = new BasicDbConnectionFactoryForDataSource();
        factory.setDataSource(dataSource);

        // BasicDbConnectionが生成される事を確認
        TransactionManagerConnection connection = factory.getConnection(CONNECTION_NAME);
        assertThat(connection, instanceOf(BasicDbConnection.class));
        assertThat("DataSourceから取得したコネクションが参照できる。",
                  ((BasicDbConnection) connection).getConnection(), is(con));

    }

    @Test
    public void testGetDialect() throws Exception {
        when(dataSource.getConnection()).thenReturn(con);
        Dialect dialect = new DefaultDialect();

        BasicDbConnectionFactoryForDataSource factory = new BasicDbConnectionFactoryForDataSource();
        factory.setDataSource(dataSource);
        factory.setDialect(dialect);

        // BasicDbConnectionが生成される事を確認
        TransactionManagerConnection connection = factory.getConnection(CONNECTION_NAME);
        assertThat(connection, instanceOf(BasicDbConnection.class));
        assertThat("DataSourceから取得したコネクションが参照できる。",
                  connection.getDialect(), is(dialect));

    }

    /**
     * statementReuseのデフォルト設定でSqlPStatementが同じものが取得できる。
     *
     * @throws Exception
     */
    @Test
    public void testStatementReuseDefault() throws Exception {
        when(dataSource.getConnection()).thenReturn(con);
        BasicDbConnectionFactoryForDataSource factory = new BasicDbConnectionFactoryForDataSource();
        factory.setDataSource(dataSource);
        factory.setStatementFactory(new BasicStatementFactory());
        TransactionManagerConnection connection = factory.getConnection(CONNECTION_NAME);

        String sql = "SELECT * FROM TEST_TABLE";
        SqlPStatement statement1 = connection.prepareStatement(sql);
        SqlPStatement statement2 = connection.prepareStatement(sql);
        assertThat("デフォルト設定でSqlPStatementが同一のものが返ってくる。",
                statement1, is(statement2));

    }

    /**
     * statementReuseの値をfalseに設定することでSqlPStatementが異なるものが返ってくる。
     *
     * @throws Exception
     */
    @Test
    public void testStatementReuseFalse() throws Exception {
        when(dataSource.getConnection()).thenReturn(con);
                
        BasicDbConnectionFactoryForDataSource factory = new BasicDbConnectionFactoryForDataSource();
        factory.setDataSource(dataSource);
        factory.setStatementFactory(new BasicStatementFactory());
        factory.setStatementReuse(false);
        TransactionManagerConnection connection = factory.getConnection(CONNECTION_NAME);

        String sql = "SELECT * FROM TEST_TABLE";
        SqlPStatement statement1 = connection.prepareStatement(sql);
        SqlPStatement statement2 = connection.prepareStatement(sql);
        assertThat("statementReuseの値をfalseに設定することでSqlPStatementが異なるものが返ってくる。",
                statement1, not(is(statement2)));

    }

    /**
     * {@link BasicDbConnectionFactoryForDataSource#getConnection(String)}でエラーが発生した場合のテスト。
     * <p />
     * connection取得時に失敗した場合は、{@link DbAccessExceptionFactory}に処理を委譲することを確認する。
     *
     */
    @Test
    public void testDbAccessException() throws Exception {
        BasicDbConnectionFactoryForDataSource factory = new BasicDbConnectionFactoryForDataSource();
        final SQLException nativeException = new SQLException("DataSourceが返却する例外");
        // datasourceの設定。connectionの取得で例外を送出する。
        when(dataSource.getConnection()).thenThrow(nativeException);
        // 例外を委譲されるクラスの振る舞い
        final DbAccessException dbAccessException = new DbAccessException("message", nativeException);
        when(dbAccessExceptionFactory.createDbAccessException("failed to get database connection.", nativeException, null))
                .thenReturn(dbAccessException);

        factory.setDataSource(dataSource);
        factory.setDbAccessExceptionFactory(dbAccessExceptionFactory);

        try {
            factory.getConnection(CONNECTION_NAME);
            fail();
        } catch (DbAccessException e) {
            assertThat("委譲したインスタンスが返却した例外を投げること", e, is(dbAccessException));
        }
    }
}
