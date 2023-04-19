package nablarch.core.db;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.db.dialect.Dialect;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;


/**
 * {@link DbExecutionContext}のテスト。
 *
 * @author tani takanori
 */
public class DbExecutionContextTest {
    private final TransactionManagerConnection connection = mock(TransactionManagerConnection.class);

    private Dialect dialect = new DefaultDialect();

    private String connectionName = "test";

    /**
     * {@link DbExecutionContext#create(AppDbConnection, DefaultDialect)}のテスト。
     */
    @Test
    public void testInitCreate() {
        DbExecutionContext actual = new DbExecutionContext(connection, dialect, connectionName);
        assertThat("引数で渡したConnectionが取得できること。", actual.getConnection(), sameInstance(connection));
        assertThat("引数で渡したDialectが取得できること。", actual.getDialect(), sameInstance(dialect));
        assertThat("引数で渡したConnectionNameが取得できること。", actual.getConnectionName(), is(connectionName));
    }
}
