package nablarch.core.db.statement;

import nablarch.core.log.app.AppLogUtil;
import nablarch.core.log.app.JsonLogFormatterSupport;
import nablarch.core.log.basic.JsonLogObjectBuilder;
import nablarch.core.text.json.BasicJsonSerializationManager;
import nablarch.core.text.json.JsonSerializationManager;
import nablarch.core.text.json.JsonSerializationSettings;
import nablarch.core.util.StringUtil;
import nablarch.core.util.annotation.Published;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SQLログを出力するクラス。
 * @author Shuji Kitamura
 */
@Published(tag = "architect")
public class SqlJsonLogFormatter extends SqlLogFormatter {

    /** メソッド名の項目名 */
    private static final String TARGET_NAME_METHOD_NAME = "methodName";
    /** SQL文の項目名 */
    private static final String TARGET_NAME_SQL = "sql";
    /** 取得開始位置の項目名 */
    private static final String TARGET_NAME_START_POSITION = "startPosition";
    /** 最大取得件数の項目名 */
    private static final String TARGET_NAME_SIZE = "size";
    /** タイムアウト時間の項目名 */
    private static final String TARGET_NAME_QUERY_TIMEOUT = "queryTimeout";
    /** フェッチ件数の項目名 */
    private static final String TARGET_NAME_FETCH_SIZE = "fetchSize";
    /** 実行時間の項目名 */
    private static final String TARGET_NAME_EXECUTE_TIME = "executeTime";
    /** データ取得時間の項目名 */
    private static final String TARGET_NAME_RETRIEVE_TIME = "retrieveTime";
    /** 検索件数の項目名 */
    private static final String TARGET_NAME_COUNT = "count";
    /** 更新件数の項目名 */
    private static final String TARGET_NAME_UPDATE_COUNT = "updateCount";
    /** バッチ件数の項目名 */
    private static final String TARGET_NAME_BATCH_COUNT = "batchCount";
    /** 付加情報の項目名 */
    private static final String TARGET_NAME_ADDITIONAL_INFO = "additionalInfo";

    /** SqlPStatement#retrieveメソッドの検索開始時の出力項目を取得する際に使用するプロパティ名 */
    private static final String PROPS_START_RETRIEVE_TARGETS = PROPS_PREFIX + "startRetrieveTargets";
    /** SqlPStatement#retrieveメソッドの検索終了時の出力項目を取得する際に使用するプロパティ名 */
    private static final String PROPS_END_RETRIEVE_TARGETS = PROPS_PREFIX + "endRetrieveTargets";
    /** SqlPStatement#executeメソッドの実行開始時の出力項目を取得する際に使用するプロパティ名 */
    private static final String PROPS_START_EXECUTE_TARGETS = PROPS_PREFIX + "startExecuteTargets";
    /** SqlPStatement#executeメソッドの実行終了時の出力項目を取得する際に使用するプロパティ名 */
    private static final String PROPS_END_EXECUTE_TARGETS = PROPS_PREFIX + "endExecuteTargets";
    /** SqlPStatement#executeQueryメソッドの検索開始時の出力項目を取得する際に使用するプロパティ名 */
    private static final String PROPS_START_EXECUTE_QUERY_TARGETS = PROPS_PREFIX + "startExecuteQueryTargets";
    /** SqlPStatement#executeQueryメソッドの検索終了時の出力項目を取得する際に使用するプロパティ名 */
    private static final String PROPS_END_EXECUTE_QUERY_TARGETS = PROPS_PREFIX + "endExecuteQueryTargets";
    /** SqlPStatement#executeUpdateメソッドの更新開始時の出力項目を取得する際に使用するプロパティ名 */
    private static final String PROPS_START_EXECUTE_UPDATE_TARGETS = PROPS_PREFIX + "startExecuteUpdateTargets";
    /** SqlPStatement#executeUpdateメソッドの更新終了時の出力項目を取得する際に使用するプロパティ名 */
    private static final String PROPS_END_EXECUTE_UPDATE_TARGETS = PROPS_PREFIX + "endExecuteUpdateTargets";
    /** SqlPStatement#executeBatchメソッドの更新開始時の出力項目を取得する際に使用するプロパティ名 */
    private static final String PROPS_START_EXECUTE_BATCH_TARGETS = PROPS_PREFIX + "startExecuteBatchTargets";
    /** SqlPStatement#executeBatchメソッドの更新終了時の出力項目を取得する際に使用するプロパティ名 */
    private static final String PROPS_END_EXECUTE_BATCH_TARGETS = PROPS_PREFIX + "endExecuteBatchTargets";

    /** SqlPStatement#retrieveメソッドの検索開始時のデフォルトの出力項目 */
    private static final String DEFAULT_START_RETRIEVE_TARGETS
            = "methodName,sql,startPosition,size,queryTimeout,fetchSize,additionalInfo";
    /** SqlPStatement#retrieveメソッドの検索終了時のデフォルトの出力項目 */
    private static final String DEFAULT_END_RETRIEVE_TARGETS = "methodName,executeTime,retrieveTime,count";
    /** SqlPStatement#executeメソッドの実行開始時のデフォルトの出力項目 */
    private static final String DEFAULT_START_EXECUTE_TARGETS = "methodName,sql,additionalInfo";
    /** SqlPStatement#executeメソッドの実行終了時のデフォルトの出力項目 */
    private static final String DEFAULT_END_EXECUTE_TARGETS = "methodName,executeTime";
    /** SqlPStatement#executeQueryメソッドの検索開始時のデフォルトの出力項目 */
    @SuppressWarnings("squid:S1192") // 値が同じだけで定数としての意味は異なるため問題ない。
    private static final String DEFAULT_START_EXECUTE_QUERY_TARGETS = "methodName,sql,additionalInfo";
    /** SqlPStatement#executeQueryメソッドの検索終了時のデフォルトの出力項目 */
    @SuppressWarnings("squid:S1192") // 値が同じだけで定数としての意味は異なるため問題ない。
    private static final String DEFAULT_END_EXECUTE_QUERY_TARGETS = "methodName,executeTime";
    /** SqlPStatement#executeUpdateメソッドの更新開始時のデフォルトの出力項目 */
    private static final String DEFAULT_START_EXECUTE_UPDATE_TARGETS = "methodName,sql,additionalInfo";
    /** SqlPStatement#executeUpdateメソッドの更新終了時のデフォルトの出力項目 */
    private static final String DEFAULT_END_EXECUTE_UPDATE_TARGETS = "methodName,executeTime,updateCount";
    /** SqlPStatement#executeBatchメソッドの更新開始時のデフォルトの出力項目 */
    private static final String DEFAULT_START_EXECUTE_BATCH_TARGETS = "methodName,sql,additionalInfo";
    /** SqlPStatement#executeBatchメソッドの更新終了時のデフォルトの出力項目 */
    private static final String DEFAULT_END_EXECUTE_BATCH_TARGETS = "methodName,executeTime,batchCount";

    /** startRetrieve でサポートしている出力項目の一覧。 */
    private static final Set<String> SUPPORTED_TARGETS_BY_START_RETRIEVE = newUnmodifiableSet(
        TARGET_NAME_METHOD_NAME,
        TARGET_NAME_SQL,
        TARGET_NAME_START_POSITION,
        TARGET_NAME_SIZE,
        TARGET_NAME_QUERY_TIMEOUT,
        TARGET_NAME_FETCH_SIZE,
        TARGET_NAME_ADDITIONAL_INFO
    );
    /** endRetrieve でサポートしている出力項目の一覧。 */
    private static final Set<String> SUPPORTED_TARGETS_BY_END_RETRIEVE = newUnmodifiableSet(
        TARGET_NAME_METHOD_NAME,
        TARGET_NAME_EXECUTE_TIME,
        TARGET_NAME_RETRIEVE_TIME,
        TARGET_NAME_COUNT
    );
    /** startExecute でサポートしている出力項目の一覧。 */
    private static final Set<String> SUPPORTED_TARGETS_BY_START_EXECUTE = newUnmodifiableSet(
        TARGET_NAME_METHOD_NAME,
        TARGET_NAME_SQL,
        TARGET_NAME_ADDITIONAL_INFO
    );
    /** endExecute でサポートしている出力項目の一覧。 */
    private static final Set<String> SUPPORTED_TARGETS_BY_END_EXECUTE = newUnmodifiableSet(
        TARGET_NAME_METHOD_NAME,
        TARGET_NAME_EXECUTE_TIME
    );
    /** startExecuteQuery でサポートしている出力項目の一覧。 */
    private static final Set<String> SUPPORTED_TARGETS_BY_START_EXECUTE_QUERY = newUnmodifiableSet(
        TARGET_NAME_METHOD_NAME,
        TARGET_NAME_SQL,
        TARGET_NAME_ADDITIONAL_INFO
    );
    /** endExecuteQuery でサポートしている出力項目の一覧。 */
    private static final Set<String> SUPPORTED_TARGETS_BY_END_EXECUTE_QUERY = newUnmodifiableSet(
        TARGET_NAME_METHOD_NAME,
        TARGET_NAME_EXECUTE_TIME
    );
    /** startExecuteUpdate でサポートしている出力項目の一覧。 */
    private static final Set<String> SUPPORTED_TARGETS_BY_START_EXECUTE_UPDATE = newUnmodifiableSet(
        TARGET_NAME_METHOD_NAME,
        TARGET_NAME_SQL,
        TARGET_NAME_ADDITIONAL_INFO
    );
    /** endExecuteUpdate でサポートしている出力項目の一覧。 */
    private static final Set<String> SUPPORTED_TARGETS_BY_END_EXECUTE_UPDATE = newUnmodifiableSet(
        TARGET_NAME_METHOD_NAME,
        TARGET_NAME_EXECUTE_TIME,
        TARGET_NAME_UPDATE_COUNT
    );
    /** startExecuteBatch でサポートしている出力項目の一覧。 */
    private static final Set<String> SUPPORTED_TARGETS_BY_START_EXECUTE_BATCH = newUnmodifiableSet(
        TARGET_NAME_METHOD_NAME,
        TARGET_NAME_SQL,
        TARGET_NAME_ADDITIONAL_INFO
    );
    /** endExecuteBatch でサポートしている出力項目の一覧。 */
    private static final Set<String> SUPPORTED_TARGETS_BY_END_EXECUTE_BATCH = newUnmodifiableSet(
        TARGET_NAME_METHOD_NAME,
        TARGET_NAME_EXECUTE_TIME,
        TARGET_NAME_BATCH_COUNT
    );

    /**
     * 引数で指定した要素を持つ変更不可能な{@link Set}を生成する。
     * @param values {@link Set}の要素
     * @return 変更不可能な{@link Set}
     */
    private static Set<String> newUnmodifiableSet(String... values) {
        Set<String> set = new HashSet<String>();
        Collections.addAll(set, values);
        return Collections.unmodifiableSet(set);
    }

    /** SqlPStatement#retrieveメソッドの検索開始時のログ出力項目 */
    private List<JsonLogObjectBuilder<SqlLogContext>> startRetrieveStructuredTargets;
    /** SqlPStatement#retrieveメソッドの検索終了時のログ出力項目 */
    private List<JsonLogObjectBuilder<SqlLogContext>> endRetrieveStructuredTargets;
    /** SqlPStatement#executeメソッドの実行開始時のログ出力項目 */
    private List<JsonLogObjectBuilder<SqlLogContext>> startExecuteStructuredTargets;
    /** SqlPStatement#executeメソッドの実行終了時のログ出力項目 */
    private List<JsonLogObjectBuilder<SqlLogContext>> endExecuteStructuredTargets;
    /** SqlPStatement#executeQueryメソッドの検索開始時のログ出力項目 */
    private List<JsonLogObjectBuilder<SqlLogContext>> startExecuteQueryStructuredTargets;
    /** SqlPStatement#executeQueryメソッドの検索終了時のログ出力項目 */
    private List<JsonLogObjectBuilder<SqlLogContext>> endExecuteQueryStructuredTargets;
    /** SqlPStatement#executeUpdateメソッドの更新開始時のログ出力項目 */
    private List<JsonLogObjectBuilder<SqlLogContext>> startExecuteUpdateStructuredTargets;
    /** SqlPStatement#executeUpdateメソッドの更新終了時のログ出力項目 */
    private List<JsonLogObjectBuilder<SqlLogContext>> endExecuteUpdateStructuredTargets;
    /** SqlPStatement#executeBatchメソッドの更新開始時のログ出力項目 */
    private List<JsonLogObjectBuilder<SqlLogContext>> startExecuteBatchStructuredTargets;
    /** SqlPStatement#executeBatchメソッドの更新終了時のログ出力項目 */
    private List<JsonLogObjectBuilder<SqlLogContext>> endExecuteBatchStructuredTargets;

    /** 各種ログのJSONフォーマット支援オブジェクト */
    private JsonLogFormatterSupport support;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void initialize(Map<String, String> props) {
        JsonSerializationSettings settings = new JsonSerializationSettings(props, PROPS_PREFIX, AppLogUtil.getFilePath());
        JsonSerializationManager serializationManager = createSerializationManager(settings);
        support = new JsonLogFormatterSupport(serializationManager, settings);

        Map<String, JsonLogObjectBuilder<SqlLogContext>> objectBuilders = getObjectBuilders();

        startRetrieveStructuredTargets = getStructuredTargets(
                objectBuilders,
                props,
                PROPS_START_RETRIEVE_TARGETS,
                DEFAULT_START_RETRIEVE_TARGETS,
                SUPPORTED_TARGETS_BY_START_RETRIEVE);
        endRetrieveStructuredTargets = getStructuredTargets(
                objectBuilders,
                props,
                PROPS_END_RETRIEVE_TARGETS,
                DEFAULT_END_RETRIEVE_TARGETS,
                SUPPORTED_TARGETS_BY_END_RETRIEVE);
        startExecuteStructuredTargets = getStructuredTargets(
                objectBuilders,
                props,
                PROPS_START_EXECUTE_TARGETS,
                DEFAULT_START_EXECUTE_TARGETS,
                SUPPORTED_TARGETS_BY_START_EXECUTE);
        endExecuteStructuredTargets = getStructuredTargets(
                objectBuilders,
                props,
                PROPS_END_EXECUTE_TARGETS,
                DEFAULT_END_EXECUTE_TARGETS,
                SUPPORTED_TARGETS_BY_END_EXECUTE);
        startExecuteQueryStructuredTargets = getStructuredTargets(
                objectBuilders,
                props,
                PROPS_START_EXECUTE_QUERY_TARGETS,
                DEFAULT_START_EXECUTE_QUERY_TARGETS,
                SUPPORTED_TARGETS_BY_START_EXECUTE_QUERY);
        endExecuteQueryStructuredTargets = getStructuredTargets(
                objectBuilders,
                props,
                PROPS_END_EXECUTE_QUERY_TARGETS,
                DEFAULT_END_EXECUTE_QUERY_TARGETS,
                SUPPORTED_TARGETS_BY_END_EXECUTE_QUERY);
        startExecuteUpdateStructuredTargets = getStructuredTargets(
                objectBuilders,
                props,
                PROPS_START_EXECUTE_UPDATE_TARGETS,
                DEFAULT_START_EXECUTE_UPDATE_TARGETS,
                SUPPORTED_TARGETS_BY_START_EXECUTE_UPDATE);
        endExecuteUpdateStructuredTargets = getStructuredTargets(
                objectBuilders,
                props,
                PROPS_END_EXECUTE_UPDATE_TARGETS,
                DEFAULT_END_EXECUTE_UPDATE_TARGETS,
                SUPPORTED_TARGETS_BY_END_EXECUTE_UPDATE);
        startExecuteBatchStructuredTargets = getStructuredTargets(
                objectBuilders,
                props,
                PROPS_START_EXECUTE_BATCH_TARGETS,
                DEFAULT_START_EXECUTE_BATCH_TARGETS,
                SUPPORTED_TARGETS_BY_START_EXECUTE_BATCH);
        endExecuteBatchStructuredTargets = getStructuredTargets(
                objectBuilders,
                props,
                PROPS_END_EXECUTE_BATCH_TARGETS,
                DEFAULT_END_EXECUTE_BATCH_TARGETS,
                SUPPORTED_TARGETS_BY_END_EXECUTE_BATCH);
    }

    /**
     * 変換処理に使用する{@link JsonSerializationManager}を生成する。
     * @param settings 各種ログ出力の設定情報
     * @return {@link JsonSerializationManager}
     */
    protected JsonSerializationManager createSerializationManager(JsonSerializationSettings settings) {
        return new BasicJsonSerializationManager();
    }

    /**
     * フォーマット対象のログ出力項目を取得する。
     * @return フォーマット対象のログ出力項目
     */
    protected Map<String, JsonLogObjectBuilder<SqlLogContext>> getObjectBuilders() {

        Map<String, JsonLogObjectBuilder<SqlLogContext>> objectBuilders
                = new HashMap<String, JsonLogObjectBuilder<SqlLogContext>>();

        objectBuilders.put(TARGET_NAME_METHOD_NAME, new MethodNameBuilder());
        objectBuilders.put(TARGET_NAME_SQL, new SqlBuilder());
        objectBuilders.put(TARGET_NAME_START_POSITION, new StartPositionBuilder());
        objectBuilders.put(TARGET_NAME_SIZE, new SizeBuilder());
        objectBuilders.put(TARGET_NAME_QUERY_TIMEOUT, new QueryTimeoutBuilder());
        objectBuilders.put(TARGET_NAME_FETCH_SIZE, new FetchSizeBuilder());
        objectBuilders.put(TARGET_NAME_EXECUTE_TIME, new ExecuteTimeBuilder());
        objectBuilders.put(TARGET_NAME_RETRIEVE_TIME, new RetrieveTimeBuilder());
        objectBuilders.put(TARGET_NAME_COUNT, new CountBuilder());
        objectBuilders.put(TARGET_NAME_UPDATE_COUNT, new UpdateCountBuilder());
        objectBuilders.put(TARGET_NAME_BATCH_COUNT, new BatchCountBuilder());
        objectBuilders.put(TARGET_NAME_ADDITIONAL_INFO, new AdditionalInfoBuilder());

        return objectBuilders;
    }
    
    /**
     * フォーマット済みのログ出力項目を取得する。
     * @param objectBuilders オブジェクトビルダー
     * @param props 各種ログ出力の設定情報
     * @param targetsPropName 出力項目のプロパティ名
     * @param defaultTargets デフォルトの出力項目
     * @param supportedTargets サポートされている出力項目
     * @return フォーマット済みのログ出力項目
     */
    private List<JsonLogObjectBuilder<SqlLogContext>> getStructuredTargets(
            Map<String, JsonLogObjectBuilder<SqlLogContext>> objectBuilders,
            Map<String, String> props,
            String targetsPropName, String defaultTargets,
            Set<String> supportedTargets) {

        String targetsStr = props.get(targetsPropName);
        if (StringUtil.isNullOrEmpty(targetsStr)) {
            targetsStr = defaultTargets;
        }

        List<JsonLogObjectBuilder<SqlLogContext>> structuredTargets = new ArrayList<JsonLogObjectBuilder<SqlLogContext>>();

        String[] targets = targetsStr.split(",");
        Set<String> keys = new HashSet<String>(targets.length);
        for (String target: targets) {
            String key = target.trim();

            if (!StringUtil.isNullOrEmpty(key) && !keys.contains(key)) {
                if (!supportedTargets.contains(key)) {
                    throw new IllegalArgumentException(
                        String.format("[%s] is not supported target by [%s].", key, targetsPropName)
                    );
                }

                keys.add(key);
                structuredTargets.add(objectBuilders.get(key));
            }
        }

        return structuredTargets;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String startRetrieve(String methodName, String sql, int startPosition, int size, int queryTimeout, int fetchSize, String additionalInfo) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setSql(sql);
        context.setStartPosition(startPosition);
        context.setSize(size);
        context.setQueryTimeout(queryTimeout);
        context.setFetchSize(fetchSize);
        context.setAdditionalInfo(additionalInfo);
        return support.getStructuredMessage(startRetrieveStructuredTargets, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String endRetrieve(String methodName, long executeTime, long retrieveTime, int count) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setExecuteTime(executeTime);
        context.setRetrieveTime(retrieveTime);
        context.setCount(count);
        return support.getStructuredMessage(endRetrieveStructuredTargets, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String startExecuteQuery(String methodName, String sql, String additionalInfo) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setSql(sql);
        context.setAdditionalInfo(additionalInfo);
        return support.getStructuredMessage(startExecuteQueryStructuredTargets, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String endExecuteQuery(String methodName, long executeTime) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setExecuteTime(executeTime);
        return support.getStructuredMessage(endExecuteQueryStructuredTargets, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String startExecuteUpdate(String methodName, String sql, String additionalInfo) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setSql(sql);
        context.setAdditionalInfo(additionalInfo);
        return support.getStructuredMessage(startExecuteUpdateStructuredTargets, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String endExecuteUpdate(String methodName, long executeTime, int updateCount) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setExecuteTime(executeTime);
        context.setUpdateCount(updateCount);
        return support.getStructuredMessage(endExecuteUpdateStructuredTargets, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String startExecute(String methodName, String sql, String additionalInfo) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setSql(sql);
        context.setAdditionalInfo(additionalInfo);
        return support.getStructuredMessage(startExecuteStructuredTargets, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String endExecute(String methodName, long executeTime) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setExecuteTime(executeTime);
        return support.getStructuredMessage(endExecuteStructuredTargets, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String startExecuteBatch(String methodName, String sql, String additionalInfo) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setSql(sql);
        context.setAdditionalInfo(additionalInfo);
        return support.getStructuredMessage(startExecuteBatchStructuredTargets, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String endExecuteBatch(String methodName, long executeTime, int batchCount) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setExecuteTime(executeTime);
        context.setBatchCount(batchCount);
        return support.getStructuredMessage(endExecuteBatchStructuredTargets, context);
    }

    /**
     * メソッド名を処理するクラス。
     * @author Shuji Kitamura
     */
    public static class MethodNameBuilder implements JsonLogObjectBuilder<SqlLogContext> {

        /**
         * {@inheritDoc}
         */
        @Override
        public void build(Map<String, Object> structuredObject, SqlLogContext context) {
            structuredObject.put(TARGET_NAME_METHOD_NAME, context.getMethodName());
        }
    }

    /**
     * SQL文を処理するクラス。
     * @author Shuji Kitamura
     */
    public static class SqlBuilder implements JsonLogObjectBuilder<SqlLogContext> {

        /**
         * {@inheritDoc}
         */
        @Override
        public void build(Map<String, Object> structuredObject, SqlLogContext context) {
            structuredObject.put(TARGET_NAME_SQL, context.getSql());
        }
    }

    /**
     * 取得開始位置を処理するクラス。
     * @author Shuji Kitamura
     */
    public static class StartPositionBuilder implements JsonLogObjectBuilder<SqlLogContext> {

        /**
         * {@inheritDoc}
         */
        @Override
        public void build(Map<String, Object> structuredObject, SqlLogContext context) {
            structuredObject.put(TARGET_NAME_START_POSITION, context.getStartPosition());
        }
    }

    /**
     * 最大取得件数を処理するクラス。
     * @author Shuji Kitamura
     */
    public static class SizeBuilder implements JsonLogObjectBuilder<SqlLogContext> {

        /**
         * {@inheritDoc}
         */
        @Override
        public void build(Map<String, Object> structuredObject, SqlLogContext context) {
            structuredObject.put(TARGET_NAME_SIZE, context.getSize());
        }
    }

    /**
     * タイムアウト時間を処理するクラス。
     * @author Shuji Kitamura
     */
    public static class QueryTimeoutBuilder implements JsonLogObjectBuilder<SqlLogContext> {

        /**
         * {@inheritDoc}
         */
        @Override
        public void build(Map<String, Object> structuredObject, SqlLogContext context) {
            structuredObject.put(TARGET_NAME_QUERY_TIMEOUT, context.getQueryTimeout());
        }
    }

    /**
     * フェッチ件数を処理するクラス。
     * @author Shuji Kitamura
     */
    public static class FetchSizeBuilder implements JsonLogObjectBuilder<SqlLogContext> {

        /**
         * {@inheritDoc}
         */
        @Override
        public void build(Map<String, Object> structuredObject, SqlLogContext context) {
            structuredObject.put(TARGET_NAME_FETCH_SIZE, context.getFetchSize());
        }
    }

    /**
     * 実行時間を処理するクラス。
     * @author Shuji Kitamura
     */
    public static class ExecuteTimeBuilder implements JsonLogObjectBuilder<SqlLogContext> {

        /**
         * {@inheritDoc}
         */
        @Override
        public void build(Map<String, Object> structuredObject, SqlLogContext context) {
            structuredObject.put(TARGET_NAME_EXECUTE_TIME, context.getExecuteTime());
        }
    }

    /**
     * データ取得時間を処理するクラス。
     * @author Shuji Kitamura
     */
    public static class RetrieveTimeBuilder implements JsonLogObjectBuilder<SqlLogContext> {

        /**
         * {@inheritDoc}
         */
        @Override
        public void build(Map<String, Object> structuredObject, SqlLogContext context) {
            structuredObject.put(TARGET_NAME_RETRIEVE_TIME, context.getRetrieveTime());
        }
    }

    /**
     * 検索件数を処理するクラス。
     * @author Shuji Kitamura
     */
    public static class CountBuilder implements JsonLogObjectBuilder<SqlLogContext> {

        /**
         * {@inheritDoc}
         */
        @Override
        public void build(Map<String, Object> structuredObject, SqlLogContext context) {
            structuredObject.put(TARGET_NAME_COUNT, context.getCount());
        }
    }

    /**
     * 更新件数を処理するクラス。
     * @author Shuji Kitamura
     */
    public static class UpdateCountBuilder implements JsonLogObjectBuilder<SqlLogContext> {

        /**
         * {@inheritDoc}
         */
        @Override
        public void build(Map<String, Object> structuredObject, SqlLogContext context) {
            structuredObject.put(TARGET_NAME_UPDATE_COUNT, context.getUpdateCount());
        }
    }

    /**
     * バッチ件数を処理するクラス。
     * @author Shuji Kitamura
     */
    public static class BatchCountBuilder implements JsonLogObjectBuilder<SqlLogContext> {

        /**
         * {@inheritDoc}
         */
        @Override
        public void build(Map<String, Object> structuredObject, SqlLogContext context) {
            structuredObject.put(TARGET_NAME_BATCH_COUNT, context.getBatchCount());
        }
    }

    /**
     * 付加情報を処理するクラス。
     * @author Shuji Kitamura
     */
    public static class AdditionalInfoBuilder implements JsonLogObjectBuilder<SqlLogContext> {

        /**
         * {@inheritDoc}
         */
        @Override
        public void build(Map<String, Object> structuredObject, SqlLogContext context) {
            String info = context.getAdditionalInfo();
            if ("".equals(info)) {
                info = null;
            }
            structuredObject.put(TARGET_NAME_ADDITIONAL_INFO, info);
        }
    }
}
