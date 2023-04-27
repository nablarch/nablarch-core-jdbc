package nablarch.core.db.transaction;

import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.test.support.log.app.OnMemoryLogWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * {@link JdbcTransaction}のテスト。
 */
public class JdbcTransactionTest {

    /** テストで使うコネクション名 */
    private static final String CONNECTION_NAME = "connection name";

    /** テスト対象 */
    private JdbcTransaction sut;

    private final TransactionManagerConnection mockConnection = mock(TransactionManagerConnection.class, RETURNS_DEEP_STUBS);

    private final JdbcTransactionTimeoutHandler mockTimeoutHandler = mock(JdbcTransactionTimeoutHandler.class);

    @Before
    public void setUp() throws Exception {
        DbConnectionContext.setConnection(CONNECTION_NAME, mockConnection);
        sut = new JdbcTransaction(CONNECTION_NAME);
        sut.setInitSqlList(Collections.<String>emptyList());
        OnMemoryLogWriter.clear();
    }

    @After
    public void tearDown() throws Exception {
        DbConnectionContext.removeConnection(CONNECTION_NAME);
    }

    /**
     * トランザクション開始時に、トランザクション分離レベルが設定されること。
     */
    @Test
    public void setIsolationLevel() throws Exception {
        sut.setIsolationLevel(Connection.TRANSACTION_SERIALIZABLE);
        sut.begin();

        // コネクションに対してトランザクション分離レベルが設定されたことを検証する。
        verify(mockConnection).setIsolationLevel(Connection.TRANSACTION_SERIALIZABLE);
    }

    /**
     * 初期処理用SQL文がトランザクション開始時に実行されることを検証する。
     */
    @Test
    public void executeInitSql() throws Exception {
        sut.setInitSqlList(new ArrayList<String>() {{
            add("select 1 from table_name");
            add("select 2 from table_name2");
        }});
        sut.begin();

        verify(mockConnection).prepareStatement("select 1 from table_name");
        verify(mockConnection).prepareStatement("select 2 from table_name2");
    }

    /**
     * コミットがされることを検証する。
     */
    @Test
    public void commit() throws Exception {
        sut.begin();
        sut.commit();

        verify(mockConnection).commit();
        OnMemoryLogWriter.assertLogContains("writer.memory", "transaction commit. resource=[connection name]");
    }

    /**
     * ロールバックされることを検証する。
     */
    @Test
    public void rollback() throws Exception {
        sut.begin();
        sut.rollback();

        // ロールバックは、トランザクション開始時にも実行されるので、2回実行される。
        verify(mockConnection, times(2)).rollback();
        OnMemoryLogWriter.assertLogContains("writer.memory", "transaction rollback. resource=[connection name]");
    }

    /**
     * トランザクションタイムアウトがサポートされる場合のテスト。
     *
     * トランザクションチェックのタイムアウトが開始され、その情報がコネクションに引き継がれること。
     */
    @Test
    public void supportTransactionTimeout() throws Exception {
        sut.setTransactionTimeoutHandler(mockTimeoutHandler);
        sut.begin();

        verify(mockTimeoutHandler).begin();
        verify(mockConnection).setJdbcTransactionTimeoutHandler(mockTimeoutHandler);
    }
}

