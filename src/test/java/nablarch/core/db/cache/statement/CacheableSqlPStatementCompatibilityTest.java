package nablarch.core.db.cache.statement;


import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;

import nablarch.core.db.connection.BasicDbConnectionFactoryForDataSource;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.statement.BasicSqlPStatementTestLogic;
import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link CacheableSqlPStatement}が{@link SqlPStatement}と
 * 互換性があることを確認するテストクラス。
 *
 * @author T.Kawasaki
 * @see BasicSqlPStatementTestLogic
 */
@RunWith(DatabaseTestRunner.class)
public class CacheableSqlPStatementCompatibilityTest extends BasicSqlPStatementTestLogic {

    /**
     * SQLIDのプレフィックス
     */
    private static final String PREFIX = "nablarch/core/db/statement/CacheableSqlPStatementCompatibilityTest#";

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/core/db/statement/CacheableSqlPStatementCompatibilityTestConfiguration.xml");

    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(CacheTestEntity.class);
    }

    /**
     * {@inheritDoc}
     */
    protected ConnectionFactory createConnectionFactory() {
        return repositoryResource.getComponentByType(BasicDbConnectionFactoryForDataSource.class);
    }

    /**
     * テスト用のエンティティ。
     */
    @Entity
    @Table(name = "CACHE_STATEMENT_TEST_TABLE")
    public static class CacheTestEntity {
        @Id
        @Column(name = "COL_NAME_1")
        public String colName1;
        @Column(name = "COL_NAME_2")
        public String colName2;
        @Column(name = "COL_NAME_3", length = 10)
        public Integer colName3;
        @Column(name = "COL_NAME_4")
        public Date colName4;
        @Column(name = "COL_NAME_5")
        public Timestamp colName5;
        @Column(name = "COL_NAME_6", length = 18)
        public Long colName6;
        @Transient
        public Object[] array;
        @Transient
        public List<String> list;
        @Transient
        public Map<String, String[]> map;
        @Transient
        public String sortId;

        public String getColName1() {
            return colName1;
        }

        public String getColName2() {
            return colName2;
        }

        public Integer getColName3() {
            return colName3;
        }

        public Date getColName4() {
            return colName4;
        }

        public Timestamp getColName5() {
            return colName5;
        }

        public Long getColName6() {
            return colName6;
        }

        public Object[] getArray() {
            return array;
        }

        public List<String> getList() {
            return list;
        }

        public String getSortId() {
            return sortId;
        }

        public Map<String, String[]> getMap() {
            return map;
        }

        public static CacheTestEntity ofColName1(String colName1) {
            final CacheTestEntity entity = new CacheTestEntity();
            entity.colName1 = colName1;
            return entity;
        }
    }

    /**
     * IN句のパラメータをMapで指定した場合、キャッシュが書き換わっていること。
     */
    @Test
    public void testInConditionWithMap() throws Exception {
        
        VariousDbTestHelper.setUpTable(
                CacheTestEntity.ofColName1("10001"),
                CacheTestEntity.ofColName1("10002"),
                CacheTestEntity.ofColName1("10003"),
                CacheTestEntity.ofColName1("10004")
        );
        String sqlId = PREFIX + "TEST_IN_CONDITION_WITH_MAP";
        Map<String, Object[]> params = new HashMap<String, Object[]>();
        params.put("map", new Object[]{"10002", "10003"});

        ParameterizedSqlPStatement stmt
                = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, params);
        ImmutableSqlResultSet rs1
                = (ImmutableSqlResultSet) stmt.retrieve(params);
        assertThat(rs1.size(), is(2));
        assertThat(rs1.get(0).getString("colName1"), is("10002"));
        assertThat(rs1.get(1).getString("colName1"), is("10003"));

        params.put("map", new Object[]{"10001", "10003"});
        stmt = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, params);
        ImmutableSqlResultSet rs2 = (ImmutableSqlResultSet) stmt.retrieve(params);
        assertThat(rs2.size(), is(2));
        assertThat(rs2.get(0).getString("colName1"), is("10001"));
        assertThat(rs2.get(1).getString("colName1"), is("10003"));
    }

    /**
     * IN句のパラメータをBeanが持つ配列で指定した場合、キャッシュが書き換わっていること。
     */
    @Test
    public void testInConditionWithArrayInEntity() throws Exception {
        String sqlId = PREFIX + "TEST_IN_CONDITION_WITH_ARRAY";

        CacheTestEntity entity = new CacheTestEntity();
        entity.array = new Object[]{"10002", "10003"};

        ParameterizedSqlPStatement stmt
                = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, entity);
        ImmutableSqlResultSet rs1
                = (ImmutableSqlResultSet) stmt.retrieve(entity);
        assertThat(rs1.size(), is(2));
        assertThat(rs1.get(0).getString("colName1"), is("10002"));
        assertThat(rs1.get(1).getString("colName1"), is("10003"));

        entity.array = new Object[]{"10001", "10003"};
        stmt = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, entity);
        ImmutableSqlResultSet rs2 = (ImmutableSqlResultSet) stmt.retrieve(entity);
        assertThat(rs2.size(), is(2));
        assertThat(rs2.get(0).getString("colName1"), is("10001"));
        assertThat(rs2.get(1).getString("colName1"), is("10003"));
    }

    /**
     * IN句のパラメータをBeanが持つListで指定した場合、キャッシュが書き換わっていること。
     */
    @Ignore("変換できない不具合のためIgnoreとしている")
    @Test
    public void testInConditionWithListInEntity() throws Exception {
        String sqlId = PREFIX + "TEST_IN_CONDITION_WITH_LIST";

        CacheTestEntity entity = new CacheTestEntity();
        entity.list = Arrays.asList("10002", "10003");

        ParameterizedSqlPStatement stmt
                = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, entity);
        ImmutableSqlResultSet rs1
                = (ImmutableSqlResultSet) stmt.retrieve(entity);
        assertThat(rs1.size(), is(2));
        assertThat(rs1.get(0).getString("colName1"), is("10002"));
        assertThat(rs1.get(1).getString("colName1"), is("10003"));

        entity.list = Arrays.asList("10001", "10003");
        stmt = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, entity);
        ImmutableSqlResultSet rs2 = (ImmutableSqlResultSet) stmt.retrieve(entity);
        assertThat(rs2.size(), is(2));
        assertThat(rs2.get(0).getString("colName1"), is("10001"));
        assertThat(rs2.get(1).getString("colName1"), is("10003"));
    }

    /**
     * IN句のパラメータに添え字指定した配列が書かれている場合、キャッシュを書き換えられること。(パラメータにMapを使用)
     */
    @Test
    public void testInConditionWithMapAndWriteArrayIndexOnSQL() throws Exception {
        String sqlId = PREFIX + "TEST_IN_CONDITION_WRITE_ARRAY_INDEX";
        Map<String, String[]> params = new HashMap<String, String[]>();
        params.put("array", new String[]{"10002", "10003"});

        ParameterizedSqlPStatement stmt
                = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, params);
        ImmutableSqlResultSet rs1
                = (ImmutableSqlResultSet) stmt.retrieve(params);
        assertThat(rs1.size(), is(2));
        assertThat(rs1.get(0).getString("colName1"), is("10002"));
        assertThat(rs1.get(1).getString("colName1"), is("10003"));

        params.put("array", new String[]{"10001", "10003"});
        stmt = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, params);
        ImmutableSqlResultSet rs2 = (ImmutableSqlResultSet) stmt.retrieve(params);
        assertThat(rs2.size(), is(2));
        assertThat(rs2.get(0).getString("colName1"), is("10001"));
        assertThat(rs2.get(1).getString("colName1"), is("10003"));
    }

    /**
     * IN句のパラメータに添え字指定した配列が書かれている場合、キャッシュが書き換わっていること。(パラメータにBeanを使用)
     */
    @Test
    public void testInConditionWithObjectAndWriteArrayIndexOnSQL() throws Exception {
        String sqlId = PREFIX + "TEST_IN_CONDITION_WRITE_ARRAY_INDEX";
        CacheTestEntity entity = new CacheTestEntity();
        entity.array = new String[]{"10002", "10003"};

        ParameterizedSqlPStatement stmt
                = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, entity);
        ImmutableSqlResultSet rs1
                = (ImmutableSqlResultSet) stmt.retrieve(entity);
        assertThat(rs1.size(), is(2));
        assertThat(rs1.get(0).getString("colName1"), is("10002"));
        assertThat(rs1.get(1).getString("colName1"), is("10003"));

        entity.array = new String[]{"10001", "10003"};
        stmt = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, entity);
        ImmutableSqlResultSet rs2 = (ImmutableSqlResultSet) stmt.retrieve(entity);
        assertThat(rs2.size(), is(2));
        assertThat(rs2.get(0).getString("colName1"), is("10001"));
        assertThat(rs2.get(1).getString("colName1"), is("10003"));
    }

    /**
     * 結果をソートする場合でもキャッシュが書き換わっていること。
     */
    @Test
    public void testInConditionWithSortId() throws Exception {
        String sqlId = PREFIX + "TEST_IN_CONDITION_DO_SORT";

        CacheTestEntity entity = new CacheTestEntity();
        entity.array = new String[]{"10002", "10003"};
        entity.sortId = "col_name1_desc";

        ParameterizedSqlPStatement stmt
                = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, entity);
        ImmutableSqlResultSet rs1
                = (ImmutableSqlResultSet) stmt.retrieve(entity);
        assertThat(rs1.size(), is(2));
        assertThat(rs1.get(0).getString("colName1"), is("10003"));
        assertThat(rs1.get(1).getString("colName1"), is("10002"));

        entity.array = new String[]{"10001", "10003"};
        entity.sortId = "col_name1_asc";
        stmt = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, entity);
        ImmutableSqlResultSet rs2 = (ImmutableSqlResultSet) stmt.retrieve(entity);
        assertThat(rs2.size(), is(2));
        assertThat(rs2.get(0).getString("colName1"), is("10001"));
        assertThat(rs2.get(1).getString("colName1"), is("10003"));
    }

    /**
     * 検索範囲を指定してSQLを実行した場合でも、キャッシュが書き換わっていること。(パラメータ指定にMapを使用)
     */
    @Test
    public void testInConditionWithMapAndDoPaging() throws Exception {
        String sqlId = PREFIX + "TEST_IN_CONDITION_WITH_MAP";

        Map<String, Object[]> params = new HashMap<String, Object[]>();
        params.put("map", new Object[]{"10002", "10003"});

        ParameterizedSqlPStatement stmt
                = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, params);
        ImmutableSqlResultSet rs1
                = (ImmutableSqlResultSet) stmt.retrieve(2, 1, params);
        assertThat(rs1.size(), is(1));
        assertThat(rs1.get(0).getString("colName1"), is("10003"));

        params.put("map", new Object[]{"10002", "10003"});
        stmt = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, params);
        ImmutableSqlResultSet rs2 = (ImmutableSqlResultSet) stmt.retrieve(1, 1, params);
        assertThat(rs2.size(), is(1));
        assertThat(rs2.get(0).getString("colName1"), is("10002"));
    }

    /**
     * 検索範囲を指定してSQLを実行した場合でも、キャッシュが書き換わっていること。(パラメータ指定にBeanを使用)
     */
    @Test
    public void testInConditionWithObjectAndDoPaging() throws Exception {
        String sqlId = PREFIX + "TEST_IN_CONDITION_WITH_ARRAY";

        CacheTestEntity entity = new CacheTestEntity();
        entity.array = new Object[]{"10002", "10003"};

        ParameterizedSqlPStatement stmt
                = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, entity);
        ImmutableSqlResultSet rs1
                = (ImmutableSqlResultSet) stmt.retrieve(2, 1, entity);
        assertThat(rs1.size(), is(1));
        assertThat(rs1.get(0).getString("colName1"), is("10003"));

        entity.array = new Object[]{"10002", "10003"};
        stmt = dbCon.prepareParameterizedSqlStatementBySqlId(sqlId, entity);
        ImmutableSqlResultSet rs2 = (ImmutableSqlResultSet) stmt.retrieve(1, 1, entity);
        assertThat(rs2.size(), is(1));
        assertThat(rs2.get(0).getString("colName1"), is("10002"));
    }
}
