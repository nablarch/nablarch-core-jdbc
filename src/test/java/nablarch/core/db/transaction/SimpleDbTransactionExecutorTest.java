package nablarch.core.db.transaction;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.transaction.TransactionFactory;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import nablarch.test.support.log.app.OnMemoryLogWriter;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link SimpleDbTransactionExecutor}のテスト。
 *
 * @author hisaaki sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class SimpleDbTransactionExecutorTest {

    public static final String EXECUTE_RUNTIME_EXCEPTION_MESSAGE = "runtime exception.";
    public static final String EXECUTE_ERROR_MESSAGE = "Error.";
    public static final String ROLLBACK_RUNTIME_EXCEPTION_MESSAGE = "rollback runtime exception.";
    public static final String ROLLBACK_ERROR_MESSAGE = "rollback Error.";
    public static final String END_RUNTIME_EXCEPTION_MESSAGE = "end runtime exception.";
    public static final String END_ERROR_MESSAGE = "end Error.";
    /** テストターゲット */
    private SimpleDbTransactionManager transaction;

    @ClassRule
    public static SystemRepositoryResource repository = new SystemRepositoryResource("nablarch/core/db/transaction/SimpleDbTransactionExecutorTest.xml");

    private static final String ID = "00001";

    private static final String INIT_VALUE = "初期値";

    private static final String UPDATED_VALUE = "更新済み";

    @BeforeClass
    public static void beforeClass() {
        VariousDbTestHelper.createTable(TestEntity.class);
    }

    /** データベースのセットアップとLogのクリアを行う */
    @Before
    public void setUp() {
        VariousDbTestHelper.setUpTable(TestEntity.create(ID, INIT_VALUE));
        OnMemoryLogWriter.clear();
        transaction = repository.getComponent("transactionManager");
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul><li>更新した内容が正常にコミットされていること</li></ul>
     */
    @Test
    public void testDoTransactionSuccess() {
        // ターゲットの呼び出し
        Integer updateCnt = new SimpleDbTransactionExecutorSub(transaction)
                .doTransaction();
        assertThat(updateCnt, is(1));

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されること", entity.col2, is(UPDATED_VALUE));
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生する。例外発生前に更新された内容はロールバックされること。</li>
     * <li>呼び出し元には、発生した例外が送出されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecute() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorRuntimeException(transaction)
                    .doTransaction();
            fail("does not run.");
        } catch (Exception e) {
            assertThat(e.getMessage(), is(EXECUTE_RUNTIME_EXCEPTION_MESSAGE));
        }
        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        assertWarnLogCountIs(0);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生する。例外発生前に更新された内容はロールバックされること。</li>
     * <li>呼び出し元には、発生した例外が送出されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecute() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorError(transaction)
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is(EXECUTE_ERROR_MESSAGE));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        assertWarnLogCountIs(0);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生し、ロールバック時に再度RuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、ロールバック時に発生した例外が送出されること。</li>
     * <li>SQL実行時に発生したRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecuteAndRollback() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorRuntimeException(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is(ROLLBACK_RUNTIME_EXCEPTION_MESSAGE));
        }

        // 更新されていないことをアサート
        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(1);
        assertWarnLog(EXECUTE_RUNTIME_EXCEPTION_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生し、ロールバック時にはErrorが発生する場合</li>
     * <li>呼び出し元には、ロールバック時に発生した例外が送出されること。</li>
     * <li>SQL実行時に発生したRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecute_ErrorAtRollback() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorRuntimeException(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_ERROR))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is(ROLLBACK_ERROR_MESSAGE));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(1);
        assertWarnLog(EXECUTE_RUNTIME_EXCEPTION_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生し、ロールバック時にはErrorが発生し、トランザクション終了時にはRuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、ロールバック時に発生した例外が送出されること。</li>
     * <li>SQL実行時に発生したRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecute_ErrorAtRollback_ExceptionAtEnd() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorRuntimeException(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_ERROR,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is(ROLLBACK_ERROR_MESSAGE));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(2);
        assertWarnLog(EXECUTE_RUNTIME_EXCEPTION_MESSAGE);
        assertWarnLog(END_RUNTIME_EXCEPTION_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生し、ロールバック時にはRuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、SQL実行時に発生した例外が送出されること。</li>
     * <li>ロールバック時のRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecute_ExceptionAtRollback() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorError(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is(EXECUTE_ERROR_MESSAGE));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(1);
        assertWarnLog(ROLLBACK_RUNTIME_EXCEPTION_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生し、ロールバック時にはErrorが発生する場合</li>
     * <li>呼び出し元には、ロールバック時に発生した例外が送出されること。</li>
     * <li>SQL実行時のErrorは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecuteAndRollback() {
        try {
            new SimpleDbTransactionExecutorError(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_ERROR))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is(ROLLBACK_ERROR_MESSAGE));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(1);
        assertWarnLog(EXECUTE_ERROR_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生し、ロールバック時にはErrorが発生し、トランザクション終了時にはRuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、ロールバック時に発生した例外が送出されること。</li>
     * <li>SQL実行時のErrorは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecuteAndRollback_ExceptionAtEnd() {
        try {
            new SimpleDbTransactionExecutorError(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_ERROR,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is(ROLLBACK_ERROR_MESSAGE));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(2);
        assertWarnLog(EXECUTE_ERROR_MESSAGE);
        assertWarnLog(END_RUNTIME_EXCEPTION_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生し、ロールバック時とトランザクション終了時にはErrorが発生する場合</li>
     * <li>呼び出し元には、ロールバック時に発生した例外が送出されること。</li>
     * <li>SQL実行時のErrorは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecuteAndRollbackAndEnd() {
        try {
            new SimpleDbTransactionExecutorError(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_ERROR,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_ERROR))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is(ROLLBACK_ERROR_MESSAGE));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(2);
        assertWarnLog(EXECUTE_ERROR_MESSAGE);
        assertWarnLog(END_ERROR_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生し、トランザクションの終了時にはRuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、SQL実行時に発生した例外が送出されること。</li>
     * <li>SQL実行時のRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecuteAndEnd() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorRuntimeException(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is(EXECUTE_RUNTIME_EXCEPTION_MESSAGE));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(1);
        assertWarnLog(END_RUNTIME_EXCEPTION_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生し、トランザクションの終了時にはErrorが発生する場合</li>
     * <li>呼び出し元には、トランザクションの終了時に発生した例外が送出されること。</li>
     * <li>SQL実行時のRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecute_ErrorAtEnd() {
        try {
            new SimpleDbTransactionExecutorRuntimeException(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_ERROR))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is(END_ERROR_MESSAGE));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(1);
        assertWarnLog(EXECUTE_RUNTIME_EXCEPTION_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生し、トランザクションの終了時にはRuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、SQL実行時に発生した例外が送出されること。</li>
     * <li>トランザクションの終了時のRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecute_ExceptionAtEnd() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorError(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is(EXECUTE_ERROR_MESSAGE));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(1);
        assertWarnLog(END_RUNTIME_EXCEPTION_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生し、トランザクションの終了時にはErrorが発生する場合</li>
     * <li>呼び出し元には、SQL実行時に発生した例外が送出されること。</li>
     * <li>SQL実行時のErrorは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecuteAndEnd() {
        try {
            new SimpleDbTransactionExecutorError(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_ERROR))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is(EXECUTE_ERROR_MESSAGE));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(1);
        assertWarnLog(END_ERROR_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生し、ロールバックとトランザクションの終了時にはRuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、ロールバック時に発生した例外が送出されること。</li>
     * <li>SQL実行時とトランザクションの終了時のRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecuteAndRollbackAndEnd() {
        try {
            new SimpleDbTransactionExecutorRuntimeException(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_RUNTIME,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is(ROLLBACK_RUNTIME_EXCEPTION_MESSAGE));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(2);
        assertWarnLog(EXECUTE_RUNTIME_EXCEPTION_MESSAGE);
        assertWarnLog(END_RUNTIME_EXCEPTION_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生し、ロールバック時にはRuntimeExceptionが発生し、トランザクション終了時にはErrorがする場合</li>
     * <li>呼び出し元には、トランザクション終了時に発生した例外が送出されること。</li>
     * <li>SQL実行時とロールバック時のRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecuteAndRollback_ErrorAtEnd() {
        try {
            new SimpleDbTransactionExecutorRuntimeException(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_RUNTIME,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_ERROR))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is(END_ERROR_MESSAGE));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(2);
        assertWarnLog(EXECUTE_RUNTIME_EXCEPTION_MESSAGE);
        assertWarnLog(ROLLBACK_RUNTIME_EXCEPTION_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生し、ロールバックとトランザクションの終了時にはRuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、SQL実行時に発生した例外が送出されること。</li>
     * <li>ロールバック時とトランザクションの終了時のRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecute_ExceptionAtRollbackAndEnd() {
        try {
            new SimpleDbTransactionExecutorError(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_RUNTIME,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is(EXECUTE_ERROR_MESSAGE));
        }


        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(2);
        assertWarnLog(ROLLBACK_RUNTIME_EXCEPTION_MESSAGE);
        assertWarnLog(END_RUNTIME_EXCEPTION_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生し、ロールバック時にはRuntimeExceptionが発生し、トランザクションの終了時にはErrorが発生する場合</li>
     * <li>呼び出し元には、SQL実行時に発生した例外が送出されること。</li>
     * <li>ロールバック時とトランザクションの終了時の例外は、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecute_ExceptionAtRollback_ErrorAndEnd() {
        try {
            new SimpleDbTransactionExecutorError(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_RUNTIME,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_ERROR))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is(EXECUTE_ERROR_MESSAGE));
        }


        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(2);
        assertWarnLog(ROLLBACK_RUNTIME_EXCEPTION_MESSAGE);
        assertWarnLog(END_ERROR_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生し、ロールバックとトランザクションの終了時にはErrorが発生する場合</li>
     * <li>呼び出し元には、ロールバック時に発生した例外が送出されること。</li>
     * <li>SQL実行時とトランザクションの終了時の例外は、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecute_ErrorAtRollbackAndEnd() {
        try {
            new SimpleDbTransactionExecutorRuntimeException(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_ERROR,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_ERROR))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is(ROLLBACK_ERROR_MESSAGE));
        }


        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLogCountIs(2);
        assertWarnLog(EXECUTE_RUNTIME_EXCEPTION_MESSAGE);
        assertWarnLog(END_ERROR_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>トランザクションの終了時にはRuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、例外が送出されないこと。</li>
     * <li>コミットは成功しているので、更新処理が反映されていること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtEnd() {
        new SimpleDbTransactionExecutorSub(
                new SimpleDbTransactionManagerError(
                        transaction,
                        SimpleDbTransactionManagerError.ERROR_STATEMENT.END_RUNTIME))
                .doTransaction();


        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("コミットは成功しているので、更新処理が正しく行われていること", entity.col2, is(UPDATED_VALUE));

        // ログのアサート
        assertWarnLogCountIs(1);
        assertWarnLog(END_RUNTIME_EXCEPTION_MESSAGE);
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>トランザクションの終了時にはErrorが発生する場合</li>
     * <li>呼び出し元には、トランザクションの終了時に発生した例外が送出されること。</li>
     * <li>コミットは成功しているので、更新処理が反映されていること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtEnd() {
        try {
            new SimpleDbTransactionExecutorSub(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_ERROR))
                    .doTransaction();
        } catch (Error e) {
            assertThat(e.getMessage(), is(END_ERROR_MESSAGE));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("コミットは成功しているので、更新処理が正しく行われていること", entity.col2, is(UPDATED_VALUE));

        // ログのアサート
        assertWarnLogCountIs(0);
    }

    /**
     * ワーニングログをアサートする。
     *
     * @param message ログのメッセージ
     */
    private static void assertWarnLog(String message) {
        List<String> log = OnMemoryLogWriter.getMessages("writer.memory");
        boolean writeLog = false;
        for (String logMessage : log) {
            String str = logMessage.replaceAll("[\\r\\n]", "");
            if (str.matches(
                    "^.*WARN.*failed in the "
                            + "application process\\..*" + message + ".*$")) {
                writeLog = true;
                break;
            }
        }
        assertThat("元例外がWARNレベルでログに出力されていること", writeLog, is(true));
    }

    /**
     * ワーニングログの件数をアサートする。
     *
     * @param count ログのカウント
     */
    private static void assertWarnLogCountIs(int count) {
        List<String> log = OnMemoryLogWriter.getMessages("writer.memory");
        int warnCount = 0;
        for (String logMessage : log) {
            String str = logMessage.replaceAll("[\\r\\n]", "");
            if (str.matches(
                    "^.*WARN.*failed in the "
                            + "application process\\..*$")) {
                warnCount++;
            }
        }
        assertThat(warnCount, is(count));
    }

    /**
     * create table sbt_test_table (
     *      col1 char(5),
     *      col2 varchar2(100)
     * )
     *
     */
    @Entity
    @Table(name = "SBT_TEST_TABLE")
    public static class TestEntity {
        @Id
        @Column(name = "col1", length = 5, nullable = false)
        public String col1;

        @Column(name = "col2", length = 100)
        public String col2;

        private static TestEntity create(String col1, String col2) {
            TestEntity entity = new TestEntity();
            entity.col1 = col1;
            entity.col2 = col2;
            return entity;
        }
    }

    /**
     * エラーを発生させる{@link SimpleDbTransactionManager}のサブクラス。
     */
    private static class SimpleDbTransactionManagerError extends SimpleDbTransactionManager {

        enum ERROR_STATEMENT {
            ROLLBACK_RUNTIME,
            ROLLBACK_ERROR,
            END_RUNTIME,
            END_ERROR
        }

        private final SimpleDbTransactionManager transactionManager;

        private final ERROR_STATEMENT[] errorState;

        private SimpleDbTransactionManagerError(
                SimpleDbTransactionManager transactionManager,
                ERROR_STATEMENT... errorState) {
            this.transactionManager = transactionManager;
            this.errorState = errorState;
        }

        private boolean is(ERROR_STATEMENT[] errors, ERROR_STATEMENT error) {
            for (ERROR_STATEMENT statement : errors) {
                if (statement == error) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void beginTransaction() {
            transactionManager.beginTransaction();
        }

        @Override
        public void commitTransaction() {
            transactionManager.commitTransaction();
        }

        @Override
        public void rollbackTransaction() {
            transactionManager.rollbackTransaction();
            if (is(errorState, ERROR_STATEMENT.ROLLBACK_RUNTIME)) {
                throw new RuntimeException(ROLLBACK_RUNTIME_EXCEPTION_MESSAGE);
            } else if (is(errorState, ERROR_STATEMENT.ROLLBACK_ERROR)) {
                throw new Error(ROLLBACK_ERROR_MESSAGE);
            }
        }

        @Override
        public void endTransaction() {
            transactionManager.endTransaction();
            if (is(errorState, ERROR_STATEMENT.END_RUNTIME)) {
                throw new RuntimeException(END_RUNTIME_EXCEPTION_MESSAGE);
            } else if (is(errorState, ERROR_STATEMENT.END_ERROR)) {
                throw new Error(END_ERROR_MESSAGE);
            }
        }

        @Override
        public void setConnectionFactory(ConnectionFactory connectionFactory) {
            transactionManager.setConnectionFactory(connectionFactory);
        }

        @Override
        public void setTransactionFactory(
                TransactionFactory transactionFactory) {
            transactionManager.setTransactionFactory(transactionFactory);
        }

        @Override
        public void setDbTransactionName(String dbTransactionName) {
            transactionManager.setDbTransactionName(dbTransactionName);
        }

        @Override
        public String getDbTransactionName() {
            return transactionManager.getDbTransactionName();
        }
    }

    /**
     * テスト用の{@link SimpleDbTransactionExecutor}実装クラス。
     */
    private static class SimpleDbTransactionExecutorSub extends SimpleDbTransactionExecutor<Integer> {

        /**
         * コンストラクタ。
         *
         * @param transactionManager トランザクションマネージャ
         */
        public SimpleDbTransactionExecutorSub(
                SimpleDbTransactionManager transactionManager) {
            super(transactionManager);
        }

        @Override
        public Integer execute(AppDbConnection connection) {
            SqlPStatement statement = connection.prepareStatement(
                    "update sbt_test_table set col2 = ? where col1 = ?");
            statement.setString(1, UPDATED_VALUE);
            statement.setString(2, ID);
            return statement.executeUpdate();
        }
    }

    /**
     * テスト用の{@link SimpleDbTransactionExecutor}実装クラス。
     * <br/>
     * 本クラスは、更新用SQL実行後に、RuntimeExceptionを送出する。
     */
    @SuppressWarnings("NonExceptionNameEndsWithException")
    private static class SimpleDbTransactionExecutorRuntimeException extends SimpleDbTransactionExecutorSub {

        /**
         * コンストラクタ。
         *
         * @param transactionManager トランザクションマネージャ
         */
        public SimpleDbTransactionExecutorRuntimeException(
                SimpleDbTransactionManager transactionManager) {
            super(transactionManager);
        }

        @Override
        public Integer execute(AppDbConnection connection) {
            super.execute(connection);
            // エラーを発生させる。
            throw new RuntimeException(EXECUTE_RUNTIME_EXCEPTION_MESSAGE);
        }
    }

    /**
     * テスト用の{@link SimpleDbTransactionExecutor}実装クラス。
     * <br/>
     * 本クラスは、更新用SQL実行後に、Errorを送出する。
     */
    private static class SimpleDbTransactionExecutorError extends SimpleDbTransactionExecutorSub {

        /**
         * コンストラクタ。
         *
         * @param transactionManager トランザクションマネージャ
         */
        public SimpleDbTransactionExecutorError(
                SimpleDbTransactionManager transactionManager) {
            super(transactionManager);
        }

        @Override
        public Integer execute(AppDbConnection connection) {
            super.execute(connection);
            // エラーを発生させる。
            throw new Error(EXECUTE_ERROR_MESSAGE);
        }
    }
}
