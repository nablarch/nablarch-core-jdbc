package nablarch.core.db.statement;

import nablarch.core.log.LogTestSupport;
import nablarch.core.text.json.BasicJsonSerializationManager;
import nablarch.core.text.json.JsonSerializationSettings;
import nablarch.core.text.json.JsonSerializer;
import nablarch.test.support.SystemPropertyCleaner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import java.io.IOException;
import java.io.Writer;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.isJson;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.withJsonPath;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.withoutJsonPath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThrows;

/**
 * {@link SqlJsonLogFormatter}のテストクラス。
 *
 * @author Shuji Kitamura
 */
public class SqlJsonLogFormatterTest extends LogTestSupport {

    @Rule
    public SystemPropertyCleaner systemPropertyCleaner = new SystemPropertyCleaner();

    /**
     * {@link SqlJsonLogFormatter#startRetrieve}メソッドのテスト。
     */
    @Test
    public void testStartRetrieve() {
        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        String sql = "select * from T";
        int startPosition = 0;
        int size = 10;
        int queryTimeout = 3000;
        int fetchSize = 100;
        String additionalInfo = "ex";

        String message = formatter.startRetrieve(methodName, sql, startPosition, size, queryTimeout, fetchSize, additionalInfo);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
                withJsonPath("$", hasEntry("methodName", "method01")),
                withJsonPath("$", hasEntry("sql", "select * from T")),
                withJsonPath("$", hasEntry("startPosition", 0)),
                withJsonPath("$", hasEntry("size", 10)),
                withJsonPath("$", hasEntry("queryTimeout", 3000)),
                withJsonPath("$", hasEntry("fetchSize", 100)),
                withJsonPath("$", hasEntry("additionalInfo", "ex")))));
    }

    /**
     * {@link SqlJsonLogFormatter#startRetrieve}メソッドのテスト。
     * <p>
     * targetsを指定した場合。
     * </p>
     */
    @Test
    public void testStartRetrieveWithTargets() {
        System.setProperty("sqlLogFormatter.startRetrieveTargets", "methodName,sql");

        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        String sql = "select * from T";
        int startPosition = 0;
        int size = 10;
        int queryTimeout = 3000;
        int fetchSize = 100;
        String additionalInfo = "ex";

        String message = formatter.startRetrieve(methodName, sql, startPosition, size, queryTimeout, fetchSize, additionalInfo);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
            withJsonPath("$.*", hasSize(2)),
            withJsonPath("$", hasEntry("methodName", "method01")),
            withJsonPath("$", hasEntry("sql", "select * from T"))
        )));
    }

    /**
     * {@link SqlJsonLogFormatter#endRetrieve}メソッドのテスト。
     */
    @Test
    public void testEndRetrieve() {
        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        int executeTime = 30;
        int retrieveTime = 20;
        int count = 7;

        String message = formatter.endRetrieve(methodName, executeTime, retrieveTime, count);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
                withJsonPath("$", hasEntry("methodName", "method01")),
                withJsonPath("$", hasEntry("executeTime", 30)),
                withJsonPath("$", hasEntry("retrieveTime", 20)),
                withJsonPath("$", hasEntry("count", 7)))));
    }

    /**
     * {@link SqlJsonLogFormatter#endRetrieve}メソッドの出力項目切り換えテスト。
     */
    @Test
    public void testEndRetrieveWithTargets() {
        System.setProperty("sqlLogFormatter.endRetrieveTargets", " methodName , ,,methodName");

        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        int executeTime = 30;
        int retrieveTime = 20;
        int count = 7;

        String message = formatter.endRetrieve(methodName, executeTime, retrieveTime, count);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
                withJsonPath("$", hasEntry("methodName", "method01")),
                withoutJsonPath("$.executeTime"),
                withoutJsonPath("$.retrieveTime"),
                withoutJsonPath("$.count"))));
    }

    /**
     * {@link SqlJsonLogFormatter#startExecuteQuery}メソッドのテスト。
     */
    @Test
    public void testStartExecuteQuery() {
        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        String sql = "select * from T";
        String additionalInfo = "ex";

        String message = formatter.startExecuteQuery(methodName, sql, additionalInfo);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
                withJsonPath("$", hasEntry("methodName", "method01")),
                withJsonPath("$", hasEntry("sql", "select * from T")),
                withJsonPath("$", hasEntry("additionalInfo", "ex")))));
    }

    /**
     * {@link SqlJsonLogFormatter#startExecuteQuery}メソッドのテスト。
     * <p>
     * targetsを指定した場合。
     * </p>
     */
    @Test
    public void testStartExecuteQueryWithTargets() {
        System.setProperty("sqlLogFormatter.startExecuteQueryTargets", "sql,additionalInfo");
        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        String sql = "select * from T";
        String additionalInfo = "ex";

        String message = formatter.startExecuteQuery(methodName, sql, additionalInfo);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
            withJsonPath("$.*", hasSize(2)),
            withJsonPath("$", hasEntry("sql", "select * from T")),
            withJsonPath("$", hasEntry("additionalInfo", "ex"))
        )));
    }

    /**
     * {@link SqlJsonLogFormatter#endExecuteQuery}メソッドのテスト。
     */
    @Test
    public void testEndExecuteQuery() {
        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        int executeTime = 30;

        String message = formatter.endExecuteQuery(methodName, executeTime);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
                withJsonPath("$", hasEntry("methodName", "method01")),
                withJsonPath("$", hasEntry("executeTime", 30)))));
    }

    /**
     * {@link SqlJsonLogFormatter#endExecuteQuery}メソッドのテスト。
     * <p>
     * targetsを指定した場合。
     * </p>
     */
    @Test
    public void testEndExecuteQueryWithTargets() {
        System.setProperty("sqlLogFormatter.endExecuteQueryTargets", "executeTime");

        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        int executeTime = 30;

        String message = formatter.endExecuteQuery(methodName, executeTime);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
            withJsonPath("$.*", hasSize(1)),
            withJsonPath("$", hasEntry("executeTime", 30))
        )));
    }

    /**
     * {@link SqlJsonLogFormatter#startExecuteUpdate}メソッドのテスト。
     */
    @Test
    public void testStartExecuteUpdate() {
        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        String sql = "select * from T";
        String additionalInfo = "ex";

        String message = formatter.startExecuteUpdate(methodName, sql, additionalInfo);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
                withJsonPath("$", hasEntry("methodName", "method01")),
                withJsonPath("$", hasEntry("sql", "select * from T")),
                withJsonPath("$", hasEntry("additionalInfo", "ex")))));
    }

    /**
     * {@link SqlJsonLogFormatter#startExecuteUpdate}メソッドのテスト。
     * <p>
     * targetsを指定した場合。
     * </p>
     */
    @Test
    public void testStartExecuteUpdateWithTargets() {
        System.setProperty("sqlLogFormatter.startExecuteUpdateTargets", "methodName,sql");

        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        String sql = "select * from T";
        String additionalInfo = "ex";

        String message = formatter.startExecuteUpdate(methodName, sql, additionalInfo);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
            withJsonPath("$.*", hasSize(2)),
            withJsonPath("$", hasEntry("methodName", "method01")),
            withJsonPath("$", hasEntry("sql", "select * from T"))
        )));
    }

    /**
     * {@link SqlJsonLogFormatter#endExecuteUpdate}メソッドのテスト。
     */
    @Test
    public void testEndExecuteUpdate() {
        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        int executeTime = 30;
        int updateCount = 7;

        String message = formatter.endExecuteUpdate(methodName, executeTime, updateCount);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
                withJsonPath("$", hasEntry("methodName", "method01")),
                withJsonPath("$", hasEntry("executeTime", 30)),
                withJsonPath("$", hasEntry("updateCount", 7)))));
    }

    /**
     * {@link SqlJsonLogFormatter#endExecuteUpdate}メソッドのテスト。
     * <p>
     * targetsを指定した場合。
     * </p>
     */
    @Test
    public void testEndExecuteUpdateWithTargets() {
        System.setProperty("sqlLogFormatter.endExecuteUpdateTargets", "executeTime,updateCount");

        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        int executeTime = 30;
        int updateCount = 7;

        String message = formatter.endExecuteUpdate(methodName, executeTime, updateCount);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
            withJsonPath("$.*", hasSize(2)),
            withJsonPath("$", hasEntry("executeTime", 30)),
            withJsonPath("$", hasEntry("updateCount", 7))
        )));
    }

    /**
     * {@link SqlJsonLogFormatter#startExecute}メソッドのテスト。
     */
    @Test
    public void testStartExecute() {
        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        String sql = "select * from T";
        String additionalInfo = "ex";

        String message = formatter.startExecute(methodName, sql, additionalInfo);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
                withJsonPath("$", hasEntry("methodName", "method01")),
                withJsonPath("$", hasEntry("sql", "select * from T")),
                withJsonPath("$", hasEntry("additionalInfo", "ex")))));
    }

    /**
     * {@link SqlJsonLogFormatter#startExecute}メソッドのテスト。
     * <p>
     * targetsを指定した場合。
     * </p>
     */
    @Test
    public void testStartExecuteWithTargets() {
        System.setProperty("sqlLogFormatter.startExecuteTargets", "sql,additionalInfo");

        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        String sql = "select * from T";
        String additionalInfo = "ex";

        String message = formatter.startExecute(methodName, sql, additionalInfo);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
            withJsonPath("$.*", hasSize(2)),
            withJsonPath("$", hasEntry("sql", "select * from T")),
            withJsonPath("$", hasEntry("additionalInfo", "ex"))
        )));
    }

    /**
     * {@link SqlJsonLogFormatter#endExecute}メソッドのテスト。
     */
    @Test
    public void testEndExecute() {
        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        int executeTime = 30;

        String message = formatter.endExecute(methodName, executeTime);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
                withJsonPath("$", hasEntry("methodName", "method01")),
                withJsonPath("$", hasEntry("executeTime", 30)))));
    }

    /**
     * {@link SqlJsonLogFormatter#endExecute}メソッドのテスト。
     * <p>
     * targetsを指定した場合。
     * </p>
     */
    @Test
    public void testEndExecuteWithTargets() {
        System.setProperty("sqlLogFormatter.endExecuteTargets", "executeTime");

        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        int executeTime = 30;

        String message = formatter.endExecute(methodName, executeTime);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
            withJsonPath("$.*", hasSize(1)),
            withJsonPath("$", hasEntry("executeTime", 30))
        )));
    }

    /**
     * {@link SqlJsonLogFormatter#startExecuteBatch}メソッドのテスト。
     */
    @Test
    public void testStartExecuteBatch() {
        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        String sql = "select * from T";
        String additionalInfo = "ex";

        String message = formatter.startExecuteBatch(methodName, sql, additionalInfo);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
                withJsonPath("$", hasEntry("methodName", "method01")),
                withJsonPath("$", hasEntry("sql", "select * from T")),
                withJsonPath("$", hasEntry("additionalInfo", "ex")))));
    }

    /**
     * {@link SqlJsonLogFormatter#startExecuteBatch}メソッドのテスト。
     * <p>
     * targetsを指定した場合。
     * </p>
     */
    @Test
    public void testStartExecuteBatchWithTargets() {
        System.setProperty("sqlLogFormatter.startExecuteBatchTargets", "methodName,sql");

        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        String sql = "select * from T";
        String additionalInfo = "ex";

        String message = formatter.startExecuteBatch(methodName, sql, additionalInfo);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
            withJsonPath("$.*", hasSize(2)),
            withJsonPath("$", hasEntry("methodName", "method01")),
            withJsonPath("$", hasEntry("sql", "select * from T"))
        )));
    }

    /**
     * {@link SqlJsonLogFormatter#endExecuteBatch}メソッドのテスト。
     */
    @Test
    public void testEndExecuteBatch() {
        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        int executeTime = 30;
        int batchCount = 7;

        String message = formatter.endExecuteBatch(methodName, executeTime, batchCount);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
                withJsonPath("$", hasEntry("methodName", "method01")),
                withJsonPath("$", hasEntry("executeTime", 30)),
                withJsonPath("$", hasEntry("batchCount", 7)))));
    }

    /**
     * {@link SqlJsonLogFormatter#endExecuteBatch}メソッドのテスト。
     * <p>
     * targetsを指定した場合。
     * </p>
     */
    @Test
    public void testEndExecuteBatchWithTargets() {
        System.setProperty("sqlLogFormatter.endExecuteBatchTargets", "batchCount");

        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        int executeTime = 30;
        int batchCount = 7;

        String message = formatter.endExecuteBatch(methodName, executeTime, batchCount);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
            withJsonPath("$.*", hasSize(1)),
            withJsonPath("$", hasEntry("batchCount", 7))
        )));
    }

    /**
     * additionalInfoが空の文字列の場合に項目が出力されないことのテスト。
     */
    @Test
    public void testAdditionalInfoIsEmpty() {
        SqlLogFormatter formatter = new SqlJsonLogFormatter();

        String methodName = "method01";
        String sql = "select * from T";
        String additionalInfo = "";

        String message = formatter.startExecuteQuery(methodName, sql, additionalInfo);
        assertThat(message.startsWith("$JSON$"), is(true));
        assertThat(message.substring("$JSON$".length()), isJson(allOf(
                withJsonPath("$", hasEntry("methodName", "method01")),
                withJsonPath("$", hasEntry("sql", "select * from T")),
                withoutJsonPath("$.additionalInfo"))));
    }

    /**
     * startRetrieve でサポートされていないターゲットが指定されている場合はエラーになることをテスト。
     */
    @Test
    public void testUnsupportedTargetByStartRetrieve() {
        System.setProperty("sqlLogFormatter.startRetrieveTargets", "methodName,sql,executeTime,additionalInfo");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() {
                new SqlJsonLogFormatter();
            }
        });

        assertThat(e.getMessage(), is("[executeTime] is not supported target by [sqlLogFormatter.startRetrieveTargets]."));
    }

    /**
     * endRetrieve でサポートされていないターゲットが指定されている場合はエラーになることをテスト。
     */
    @Test
    public void testUnsupportedTargetByEndRetrieve() {
        System.setProperty("sqlLogFormatter.endRetrieveTargets", "methodName,sql,executeTime");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() {
                new SqlJsonLogFormatter();
            }
        });

        assertThat(e.getMessage(), is("[sql] is not supported target by [sqlLogFormatter.endRetrieveTargets]."));
    }

    /**
     * startExecute でサポートされていないターゲットが指定されている場合はエラーになることをテスト。
     */
    @Test
    public void testUnsupportedTargetByStartExecute() {
        System.setProperty("sqlLogFormatter.startExecuteTargets", "methodName,sql,executeTime");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() {
                new SqlJsonLogFormatter();
            }
        });

        assertThat(e.getMessage(), is("[executeTime] is not supported target by [sqlLogFormatter.startExecuteTargets]."));
    }

    /**
     * endExecute でサポートされていないターゲットが指定されている場合はエラーになることをテスト。
     */
    @Test
    public void testUnsupportedTargetByEndExecute() {
        System.setProperty("sqlLogFormatter.endExecuteTargets", "methodName,sql,executeTime");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() {
                new SqlJsonLogFormatter();
            }
        });

        assertThat(e.getMessage(), is("[sql] is not supported target by [sqlLogFormatter.endExecuteTargets]."));
    }

    /**
     * startExecuteQuery でサポートされていないターゲットが指定されている場合はエラーになることをテスト。
     */
    @Test
    public void testUnsupportedTargetByStartExecuteQuery() {
        System.setProperty("sqlLogFormatter.startExecuteQueryTargets", "methodName,sql,executeTime");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() {
                new SqlJsonLogFormatter();
            }
        });

        assertThat(e.getMessage(), is("[executeTime] is not supported target by [sqlLogFormatter.startExecuteQueryTargets]."));
    }

    /**
     * endExecuteQuery でサポートされていないターゲットが指定されている場合はエラーになることをテスト。
     */
    @Test
    public void testUnsupportedTargetByEndExecuteQuery() {
        System.setProperty("sqlLogFormatter.endExecuteQueryTargets", "methodName,sql,executeTime");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() {
                new SqlJsonLogFormatter();
            }
        });

        assertThat(e.getMessage(), is("[sql] is not supported target by [sqlLogFormatter.endExecuteQueryTargets]."));
    }

    /**
     * startExecuteUpdate でサポートされていないターゲットが指定されている場合はエラーになることをテスト。
     */
    @Test
    public void testUnsupportedTargetByStartExecuteUpdate() {
        System.setProperty("sqlLogFormatter.startExecuteUpdateTargets", "methodName,sql,executeTime");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() {
                new SqlJsonLogFormatter();
            }
        });

        assertThat(e.getMessage(), is("[executeTime] is not supported target by [sqlLogFormatter.startExecuteUpdateTargets]."));
    }

    /**
     * endExecuteUpdate でサポートされていないターゲットが指定されている場合はエラーになることをテスト。
     */
    @Test
    public void testUnsupportedTargetByEndExecuteUpdate() {
        System.setProperty("sqlLogFormatter.endExecuteUpdateTargets", "methodName,sql,executeTime,updateCount");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() {
                new SqlJsonLogFormatter();
            }
        });

        assertThat(e.getMessage(), is("[sql] is not supported target by [sqlLogFormatter.endExecuteUpdateTargets]."));
    }

    /**
     * startExecuteBatch でサポートされていないターゲットが指定されている場合はエラーになることをテスト。
     */
    @Test
    public void testUnsupportedTargetByStartExecuteBatch() {
        System.setProperty("sqlLogFormatter.startExecuteBatchTargets", "methodName,sql,executeTime");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() {
                new SqlJsonLogFormatter();
            }
        });

        assertThat(e.getMessage(), is("[executeTime] is not supported target by [sqlLogFormatter.startExecuteBatchTargets]."));
    }

    /**
     * endExecuteBatch でサポートされていないターゲットが指定されている場合はエラーになることをテスト。
     */
    @Test
    public void testUnsupportedTargetByEndExecuteBatch() {
        System.setProperty("sqlLogFormatter.endExecuteBatchTargets", "methodName,sql,executeTime");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() {
                new SqlJsonLogFormatter();
            }
        });

        assertThat(e.getMessage(), is("[sql] is not supported target by [sqlLogFormatter.endExecuteBatchTargets]."));
    }

    /**
     * {@code structuredMessagePrefix}が指定できることをテスト。
     */
    @Test
    public void testStructuredMessagePrefix() {
        System.setProperty("sqlLogFormatter.startRetrieveTargets", "methodName");
        System.setProperty("sqlLogFormatter.endRetrieveTargets", "methodName");
        System.setProperty("sqlLogFormatter.startExecuteTargets", "methodName");
        System.setProperty("sqlLogFormatter.endExecuteTargets", "methodName");
        System.setProperty("sqlLogFormatter.startExecuteQueryTargets", "methodName");
        System.setProperty("sqlLogFormatter.endExecuteQueryTargets", "methodName");
        System.setProperty("sqlLogFormatter.startExecuteUpdateTargets", "methodName");
        System.setProperty("sqlLogFormatter.endExecuteUpdateTargets", "methodName");
        System.setProperty("sqlLogFormatter.startExecuteBatchTargets", "methodName");
        System.setProperty("sqlLogFormatter.endExecuteBatchTargets", "methodName");

        System.setProperty("sqlLogFormatter.structuredMessagePrefix", "@JSON@");

        final SqlJsonLogFormatter sut = new SqlJsonLogFormatter();

        final String startRetrieve = sut.startRetrieve("startRetrieve", null, 0, 0, 0, 0, null);
        assertThat(startRetrieve, startsWith("@JSON@"));

        final String endRetrieve = sut.endRetrieve("endRetrieve", 1, 1, 1);
        assertThat(endRetrieve, startsWith("@JSON@"));

        final String startExecute = sut.startExecute("startExecute", null, null);
        assertThat(startExecute, startsWith("@JSON@"));

        final String endExecute = sut.endExecute("endExecute", 1);
        assertThat(endExecute, startsWith("@JSON@"));

        final String startExecuteQuery = sut.startExecuteQuery("startExecuteQuery", null, null);
        assertThat(startExecuteQuery, startsWith("@JSON@"));

        final String endExecuteQuery = sut.endExecuteQuery("endExecuteQuery", 1);
        assertThat(endExecuteQuery, startsWith("@JSON@"));

        final String startExecuteUpdate = sut.startExecuteUpdate("startExecuteUpdate", null, null);
        assertThat(startExecuteUpdate, startsWith("@JSON@"));

        final String endExecuteUpdate = sut.endExecuteUpdate("endExecuteUpdate", 1, 1);
        assertThat(endExecuteUpdate, startsWith("@JSON@"));

        final String startExecuteBatch = sut.startExecuteBatch("startExecuteBatch", null, null);
        assertThat(startExecuteBatch, startsWith("@JSON@"));

        final String endExecuteBatch = sut.endExecuteBatch("endExecuteBatch", 1, 1);
        assertThat(endExecuteBatch, startsWith("@JSON@"));
    }

    /**
     * {@code structuredMessagePrefix}が指定できることをテスト。
     */
    @Test
    public void testJsonSerializationManagerClassName() {
        System.setProperty("sqlLogFormatter.startRetrieveTargets", "methodName");
        System.setProperty("sqlLogFormatter.endRetrieveTargets", "methodName");
        System.setProperty("sqlLogFormatter.startExecuteTargets", "methodName");
        System.setProperty("sqlLogFormatter.endExecuteTargets", "methodName");
        System.setProperty("sqlLogFormatter.startExecuteQueryTargets", "methodName");
        System.setProperty("sqlLogFormatter.endExecuteQueryTargets", "methodName");
        System.setProperty("sqlLogFormatter.startExecuteUpdateTargets", "methodName");
        System.setProperty("sqlLogFormatter.endExecuteUpdateTargets", "methodName");
        System.setProperty("sqlLogFormatter.startExecuteBatchTargets", "methodName");
        System.setProperty("sqlLogFormatter.endExecuteBatchTargets", "methodName");

        System.setProperty("sqlLogFormatter.jsonSerializationManagerClassName", MockJsonSerializationManager.class.getName());

        final SqlJsonLogFormatter sut = new SqlJsonLogFormatter();

        final String startRetrieve = sut.startRetrieve("startRetrieve", null, 0, 0, 0, 0, null);
        assertThat(startRetrieve, is("$JSON$mock serialization"));

        final String endRetrieve = sut.endRetrieve("endRetrieve", 1, 1, 1);
        assertThat(endRetrieve, is("$JSON$mock serialization"));

        final String startExecute = sut.startExecute("startExecute", null, null);
        assertThat(startExecute, is("$JSON$mock serialization"));

        final String endExecute = sut.endExecute("endExecute", 1);
        assertThat(endExecute, is("$JSON$mock serialization"));

        final String startExecuteQuery = sut.startExecuteQuery("startExecuteQuery", null, null);
        assertThat(startExecuteQuery, is("$JSON$mock serialization"));

        final String endExecuteQuery = sut.endExecuteQuery("endExecuteQuery", 1);
        assertThat(endExecuteQuery, is("$JSON$mock serialization"));

        final String startExecuteUpdate = sut.startExecuteUpdate("startExecuteUpdate", null, null);
        assertThat(startExecuteUpdate, is("$JSON$mock serialization"));

        final String endExecuteUpdate = sut.endExecuteUpdate("endExecuteUpdate", 1, 1);
        assertThat(endExecuteUpdate, is("$JSON$mock serialization"));

        final String startExecuteBatch = sut.startExecuteBatch("startExecuteBatch", null, null);
        assertThat(startExecuteBatch, is("$JSON$mock serialization"));

        final String endExecuteBatch = sut.endExecuteBatch("endExecuteBatch", 1, 1);
        assertThat(endExecuteBatch, is("$JSON$mock serialization"));
    }

    public static class MockJsonSerializationManager extends BasicJsonSerializationManager {
        @Override
        public JsonSerializer getSerializer(Object value) {
            return new JsonSerializer() {
                @Override
                public void serialize(Writer writer, Object value) throws IOException {
                    writer.write("mock serialization");
                }

                @Override
                public void initialize(JsonSerializationSettings settings) {
                }

                @Override
                public boolean isTarget(Class<?> valueClass) {
                    return false;
                }
            };
        }
    }
}
