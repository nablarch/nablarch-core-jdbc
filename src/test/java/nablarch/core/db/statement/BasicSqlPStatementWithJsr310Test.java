package nablarch.core.db.statement;

import nablarch.core.db.connection.BasicDbConnectionFactoryForDataSource;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.statement.entity.Jsr310Column;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

/**
 * {@link BasicSqlPStatement}のテストクラス。
 * <p> 
 * Date and Time API（JSR-310）型の登録、検索の動作確認テストを実施する。
 */
@RunWith(DatabaseTestRunner.class)
public class BasicSqlPStatementWithJsr310Test extends BasicSqlPStatementTestLogic {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/core/db/statement/BasicSqlPStatementWithJsr310TestConfiguration.xml");

    @Override
    protected ConnectionFactory createConnectionFactory() {
        return repositoryResource.getComponentByType(BasicDbConnectionFactoryForDataSource.class);
    }

    /**
     * {@link BasicSqlPStatement#executeUpdateByMap(Map)}を利用して Date and Time API（JSR-310）型
     * （{@link LocalDate}, {@link LocalDateTime}）のオブジェクトの登録確認テスト。
     */
    @Test
    public void executeUpdateByMap_with_jsr310() throws Exception {
        VariousDbTestHelper.createTable(Jsr310Column.class);

        LocalDate localDate = LocalDate.parse("2015-04-01");
        LocalDateTime localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");

        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO JSR310_COLUMN (ID, LOCAL_DATE, LOCAL_DATE_TIME) VALUES (:id, :localDate, :localDateTime)");
        Map<String, Object> insertData = new HashMap<>();
        insertData.put("id", 1);
        insertData.put("localDate", localDate);
        insertData.put("localDateTime", localDateTime);
        final int result = sut.executeUpdateByMap(insertData);

        assertThat("1レコード登録されること", result, is(1));
        dbCon.commit();

        final Jsr310Column actual = VariousDbTestHelper.findById(Jsr310Column.class, 1);
        assertThat("idが登録用Mapに設定した値であること", actual.id, is(1L));
        assertThat("localDateが登録用Mapに設定した値であること",actual.localDate, is(localDate));
        assertThat("localDateTimeが登録用Mapに設定した値であること",actual.localDateTime, is(localDateTime));
    }

    /**
     * {@link BasicSqlPStatement#executeQueryByObject(Object)}を利用して Date and Time API（JSR-310）型
     * （{@link LocalDate}, {@link LocalDateTime}）のフィールドを持つEntityのデータの登録確認テスト。
     */
    @Test
    public void executeUpdateByObject_with_jsr310() throws Exception {
        VariousDbTestHelper.createTable(Jsr310Column.class);

        LocalDate localDate = LocalDate.parse("2015-04-01");
        LocalDateTime localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");

        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO JSR310_COLUMN (ID, LOCAL_DATE, LOCAL_DATE_TIME) VALUES (:id, :localDate, :localDateTime)");

        final Jsr310Column entity = new Jsr310Column();
        entity.id = 1L;
        entity.localDate = localDate;
        entity.localDateTime = localDateTime;
        
        int result = sut.executeUpdateByObject(entity);
        assertThat("1レコード登録されること", result, is(1));
        dbCon.commit();

        dbCon.commit();

        final Jsr310Column actual = VariousDbTestHelper.findById(Jsr310Column.class, 1);
        assertThat("idが登録用オブジェクトに設定した値であること", actual.id, is(1L));
        assertThat("localDateが登録用オブジェクトに設定した値であること",actual.localDate, is(localDate));
        assertThat("localDateTimeが登録用オブジェクトに設定した値であること",actual.localDateTime, is(localDateTime));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Map)}で{@link LocalDate}を検索条件とした場合のテスト。
     */
    @Test
    public void retrieve_map_with_localDate_condition() throws Exception {
        VariousDbTestHelper.createTable(Jsr310Column.class);
        LocalDate localDate = LocalDate.parse("2015-04-01");
        Jsr310Column entity =  new Jsr310Column();
        entity.id = 12345L;
        entity.localDate = localDate;
        VariousDbTestHelper.insert(entity);

        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM JSR310_COLUMN WHERE LOCAL_DATE = :localDate");

        Map<String, Object> condition = new HashMap<>();
        condition.put("localDate", localDate);

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("1レコード取得できること", actual.size(), is(1));
        assertThat("localDateが検索条件Mapに設定した値であること",
                ((java.sql.Date)actual.get(0).get("LOCAL_DATE")).toLocalDate(), is(localDate));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Map)}で{@link LocalDateTime}を検索条件とした場合のテスト。
     */
    @Test
    public void retrieve_map_with_localDateTime_condition() throws Exception {
        VariousDbTestHelper.createTable(Jsr310Column.class);
        LocalDateTime localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        Jsr310Column entity =  new Jsr310Column();
        entity.id = 12345L;
        entity.localDateTime = localDateTime;
        VariousDbTestHelper.insert(entity);

        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM JSR310_COLUMN WHERE LOCAL_DATE_TIME = :localDateTime");

        Map<String, Object> condition = new HashMap<>();
        condition.put("localDateTime", localDateTime);

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("1レコード取得できること", actual.size(), is(1));
        assertThat("localDateTimeが検索条件Mapに設定した値であること",
                ((java.sql.Timestamp)actual.get(0).get("LOCAL_DATE_TIME")).toLocalDateTime(), is(localDateTime));
    }
    
    /**
     * {@link BasicSqlPStatement#retrieve(Object)}で{@link LocalDate}を検索条件とした場合のテスト。
     */
    @Test
    public void retrieve_object_with_localDate_condition() throws Exception {
        VariousDbTestHelper.createTable(Jsr310Column.class);
        LocalDate localDate = LocalDate.parse("2015-04-01");
        Jsr310Column entity =  new Jsr310Column();
        entity.id = 12345L;
        entity.localDate = localDate;
        VariousDbTestHelper.insert(entity);

        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM JSR310_COLUMN WHERE LOCAL_DATE = :localDate");
        final Jsr310Column condition =  new Jsr310Column();
        condition.localDate = localDate;

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("1レコード取得できること", actual.size(), is(1));
        assertThat("localDateが検索条件オブジェクトに設定した値であること",
                ((java.sql.Date)actual.get(0).get("LOCAL_DATE")).toLocalDate(), is(localDate));

    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}で{@link LocalDateTime}を検索条件とした場合のテスト。
     */
    @Test
    public void retrieve_object_with_localDateTime_condition() throws Exception {
        VariousDbTestHelper.createTable(Jsr310Column.class);
        LocalDateTime localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        Jsr310Column entity =  new Jsr310Column();
        entity.id = 12345L;
        entity.localDateTime = localDateTime;
        VariousDbTestHelper.insert(entity);

        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM JSR310_COLUMN WHERE LOCAL_DATE_TIME = :localDateTime");
        final Jsr310Column condition =  new Jsr310Column();
        condition.localDateTime = localDateTime;

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("1レコード取得できること", actual.size(), is(1));
        assertThat("localDateTimeが検索条件オブジェクトに設定した値であること",
                ((java.sql.Timestamp)actual.get(0).get("LOCAL_DATE_TIME")).toLocalDateTime(), is(localDateTime));
    }
}
