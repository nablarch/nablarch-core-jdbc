package nablarch.core.db.statement;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.transaction.JdbcTransactionFactory;
import nablarch.core.repository.SystemRepository;
import nablarch.core.transaction.Transaction;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.transaction.TransactionTimeoutException;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.log.app.OnMemoryLogWriter;
import nablarch.test.support.reflection.ReflectionUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link BasicSqlCStatement}のトランザクションタイムアウトに関連したテスト。
 */
@RunWith(DatabaseTestRunner.class)
@TargetDb(include = TargetDb.Db.ORACLE)
public class BasicSqlCStatementTestWithTransactionTimeout {

    @ClassRule
    public static final SystemRepositoryResource SYSTEM_REPOSITORY_RESOURCE = new SystemRepositoryResource("db-default.xml");

    /** テストで使用するデータベース接続 */
    private TransactionManagerConnection testConnection;

    private SqlCStatement sut;

    @BeforeClass
    public static void setUpClass() throws Exception {
        DbConnectionContext.removeConnection();
    }

    @Before
    public void setUp() throws Exception {
        OnMemoryLogWriter.clear();

        ConnectionFactory connectionFactory = SystemRepository.get("connectionFactory");
        testConnection = connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        DbConnectionContext.setConnection(testConnection);

        JdbcTransactionFactory transactionFactory = SystemRepository.get("jdbcTransactionFactory");
        transactionFactory.setTransactionTimeoutSec(1);
        final Transaction transaction = transactionFactory.getTransaction(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        TransactionContext.setTransaction(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY,
                transaction);

        transaction.begin();
    }

    @After
    public void tearDown() throws Exception {
        TransactionContext.removeTransaction();
        DbConnectionContext.removeConnection();
        testConnection.terminate();
        System.out.println("BasicSqlCStatementTestWithTransactionTimeout.tearDown");
    }

    /**
     * トランザクションタイムアウトしないケース
     *
     * @throws Exception
     */
    @Test
    public void noTimeout() throws Exception {
        sut = DbConnectionContext.getConnection()
                .prepareCall("BEGIN ? := 'ok'; END;");
        sut.registerOutParameter(1, Types.VARCHAR);

        Thread.sleep(900);
        sut.execute();
        assertThat("結果が取れること", sut.getString(1), is("ok"));
    }

    /**
     * ストアド実行前にタイムアウト時間に達していた場合、タイムアウトエラーとなること。
     *
     * @throws Exception
     */
    @Test(expected = TransactionTimeoutException.class)
    public void timeoutPreExec() throws Exception {
        sut = DbConnectionContext.getConnection()
                .prepareCall("BEGIN NULL; END;");

        Thread.sleep(2000);
        sut.execute();
    }

    /**
     * タイムアウトを示す任意のエラーが発生して、トランザクションタイムアウト時間を超過していた場合
     * タイムアウトエラーとなること。
     *
     * @throws Exception
     */
    @Test(expected = TransactionTimeoutException.class)
    public void timeoutTargetError() throws Exception {
        final Connection mockConnection = mock(Connection.class, RETURNS_DEEP_STUBS);
        
        when(mockConnection.prepareCall(anyString()).execute()).then((context) -> {
            System.out.println("execute sleep...");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            throw new SQLException("timeoutとなる例外", null, 1013);
        });

        final Connection originalConnection = ReflectionUtil.getFieldValue(testConnection, "con");
        try {
            ReflectionUtil.setFieldValue(testConnection, "con", mockConnection);
            sut = testConnection.prepareCall("BEGIN NULL; END;");
            sut.execute();
        } finally {
            ReflectionUtil.setFieldValue(testConnection, "con", originalConnection);
        }
    }
}
