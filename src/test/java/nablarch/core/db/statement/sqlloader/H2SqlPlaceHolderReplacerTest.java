package nablarch.core.db.statement.sqlloader;

import nablarch.core.db.connection.BasicDbConnectionFactoryForDataSource;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * H2を使って実際にSQL文の置き換えができることを確認するテスト。
 */
@TargetDb(include = TargetDb.Db.H2)
@RunWith(DatabaseTestRunner.class)
public class H2SqlPlaceHolderReplacerTest {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/core/db/statement/sqlloader/H2SqlPlaceHolderReplacerTest.xml");

    private TransactionManagerConnection connection;

    @Before
    public void prepareConnection() {
        BasicDbConnectionFactoryForDataSource factory = repositoryResource.getComponentByType(BasicDbConnectionFactoryForDataSource.class);
        connection = factory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
    }

    @After
    public void closeConnection() {
        if (connection != null) {
            connection.terminate();
        }
    }

    @Test
    public void スキーマを置き換えてSELECT文が発行できること() {
        String sqlId = H2SqlPlaceHolderReplacerTest.class.getName() + "#SELECT_SCHEMA";
        SqlResultSet resultSet = connection.prepareStatementBySqlId(sqlId)
                                           .retrieve();
        assertThat(resultSet.size(), is(1));
        SqlRow row = resultSet.get(0);
        assertThat("スキーマを指定したクエリが発行できること",
                   row.getString("SCHEMA_NAME"), is("INFORMATION_SCHEMA"));
    }

}
