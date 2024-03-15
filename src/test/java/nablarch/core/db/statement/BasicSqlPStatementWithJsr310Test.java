package nablarch.core.db.statement;

import nablarch.core.db.connection.BasicDbConnectionFactoryForDataSource;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.statement.entity.Jsr310Column;
import nablarch.core.db.statement.entity.Jsr310ColumnForSqlServer;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.TargetDb.Db;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Timestamp;
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
 * Entityクラスの日時型の定義方法がSQLServerとそれ以外のDBで異なるため、分けてテストを実施する。
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
     * （{@link LocalDate}, {@link LocalDateTime}）のオブジェクトの登録確認テスト（SQLServer以外）。
     */
    @Test
    @TargetDb(exclude = Db.SQL_SERVER)
    public void executeUpdateByMap_with_jsr310() {
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

        assertThat(result, is(1));
        dbCon.commit();

        final Jsr310Column actual = VariousDbTestHelper.findById(Jsr310Column.class, 1);
        assertThat(actual.id, is(1L));
        assertThat(actual.localDate, is(localDate));
        assertThat(actual.localDateTime, is(localDateTime));
    }

    /**
     * {@link BasicSqlPStatement#executeUpdateByMap(Map)}を利用して Date and Time API（JSR-310）型
     * （{@link LocalDate}, {@link LocalDateTime}）のオブジェクトの登録確認テスト（SQLServer用）。
     */
    @Test
    @TargetDb(include = Db.SQL_SERVER)
    public void executeUpdateByMap_with_jsr310_SQLServer() {
        VariousDbTestHelper.createTable(Jsr310ColumnForSqlServer.class);

        LocalDate localDate = LocalDate.parse("2015-04-01");
        LocalDateTime localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");

        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
            "INSERT INTO JSR310_COLUMN_SQLSERVER (ID, LOCAL_DATE, LOCAL_DATE_TIME) VALUES (:id, :localDate, :localDateTime)");
        Map<String, Object> insertData = new HashMap<>();
        insertData.put("id", 1);
        insertData.put("localDate", localDate);
        insertData.put("localDateTime", localDateTime);
        final int result = sut.executeUpdateByMap(insertData);

        assertThat(result, is(1));
        dbCon.commit();

        final Jsr310ColumnForSqlServer actual = VariousDbTestHelper.findById(Jsr310ColumnForSqlServer.class, 1);
        assertThat(actual.id, is(1L));
        assertThat(actual.localDate, is(localDate));
        assertThat(actual.localDateTime, is(localDateTime));
    }

    /**
     * {@link BasicSqlPStatement#executeQueryByObject(Object)}を利用して Date and Time API（JSR-310）型
     * （{@link LocalDate}, {@link LocalDateTime}）のフィールドを持つEntityのデータの登録確認テスト（SQLServer以外）。
     * 
     */
    @Test
    @TargetDb(exclude = Db.SQL_SERVER)
    public void executeUpdateByObject_with_jsr310() {
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
        assertThat(result, is(1));
        dbCon.commit();

        dbCon.commit();

        final Jsr310Column actual = VariousDbTestHelper.findById(Jsr310Column.class, 1);
        assertThat(actual.id, is(1L));
        assertThat(actual.localDate, is(localDate));
        assertThat(actual.localDateTime, is(localDateTime));
    }

    /**
     * {@link BasicSqlPStatement#executeQueryByObject(Object)}を利用して Date and Time API（JSR-310）型
     * （{@link LocalDate}, {@link LocalDateTime}）のフィールドを持つEntityのデータの登録確認テスト（SQLServer用）。
     */
    @Test
    @TargetDb(include = Db.SQL_SERVER)
    public void executeUpdateByObject_with_jsr310_SQLServer() {
        VariousDbTestHelper.createTable(Jsr310ColumnForSqlServer.class);

        LocalDate localDate = LocalDate.parse("2015-04-01");
        LocalDateTime localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");

        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
            "INSERT INTO JSR310_COLUMN_SQLSERVER (ID, LOCAL_DATE, LOCAL_DATE_TIME) VALUES (:id, :localDate, :localDateTime)");

        final Jsr310ColumnForSqlServer entity = new Jsr310ColumnForSqlServer();
        entity.id = 1L;
        entity.localDate = localDate;
        entity.localDateTime = localDateTime;

        int result = sut.executeUpdateByObject(entity);
        assertThat(result, is(1));
        dbCon.commit();

        dbCon.commit();

        final Jsr310ColumnForSqlServer actual = VariousDbTestHelper.findById(Jsr310ColumnForSqlServer.class, 1);
        assertThat(actual.id, is(1L));
        assertThat(actual.localDate, is(localDate));
        assertThat(actual.localDateTime, is(localDateTime));
    }
    
    /**
     * {@link BasicSqlPStatement#retrieve(Map)}で{@link LocalDate}を検索条件とした場合のテスト（SQLServer以外）。
     */
    @Test
    @TargetDb(exclude = Db.SQL_SERVER)
    public void retrieve_map_with_localDate_condition() {
        VariousDbTestHelper.createTable(Jsr310Column.class);
        LocalDate localDate = LocalDate.parse("2015-04-01");
        Jsr310Column entity =  new Jsr310Column();
        entity.id = 12345L;
        entity.localDate = localDate;
        VariousDbTestHelper.insert(entity);

        // DBによってDateを保持する精度が異なるため、「指定したlocalDate以降であること」を検索条件とする
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM JSR310_COLUMN WHERE LOCAL_DATE > :localDate");

        Map<String, Object> condition = new HashMap<>();
        condition.put("localDate", localDate.minusDays(1));

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));
        assertThat(new Timestamp(actual.get(0).getDate("LOCAL_DATE").getTime()).toLocalDateTime().toLocalDate(), is(localDate));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Map)}で{@link LocalDate}を検索条件とした場合のテスト（SQLServer用）。
     */
    @Test
    @TargetDb(include = Db.SQL_SERVER)
    public void retrieve_map_with_localDate_condition_SQLServer() {
        VariousDbTestHelper.createTable(Jsr310ColumnForSqlServer.class);
        LocalDate localDate = LocalDate.parse("2015-04-01");
        Jsr310ColumnForSqlServer entity =  new Jsr310ColumnForSqlServer();
        entity.id = 12345L;
        entity.localDate = localDate;
        VariousDbTestHelper.insert(entity);

        // DBによってDateを保持する精度が異なるため、「指定したlocalDate以降であること」を検索条件とする
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
            "SELECT * FROM JSR310_COLUMN_SQLSERVER WHERE LOCAL_DATE > :localDate");

        Map<String, Object> condition = new HashMap<>();
        condition.put("localDate", localDate.minusDays(1));

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));
        assertThat(new Timestamp(actual.get(0).getDate("LOCAL_DATE").getTime()).toLocalDateTime().toLocalDate(), is(localDate));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Map)}で{@link LocalDateTime}を検索条件とした場合のテスト（SQLServer以外）。
     */
    @Test
    @TargetDb(exclude = Db.SQL_SERVER)
    public void retrieve_map_with_localDateTime_condition() {
        VariousDbTestHelper.createTable(Jsr310Column.class);
        LocalDateTime localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        Jsr310Column entity =  new Jsr310Column();
        entity.id = 12345L;
        entity.localDateTime = localDateTime;
        VariousDbTestHelper.insert(entity);

        // DBによってTimestampを保持する精度が異なるため、「指定したlocalDateTime以降であること」を検索条件とする
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM JSR310_COLUMN WHERE LOCAL_DATE_TIME > :localDateTime");

        Map<String, Object> condition = new HashMap<>();
        condition.put("localDateTime", localDateTime.minusSeconds(1));

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));
        assertThat(new Timestamp(actual.get(0).getDate("LOCAL_DATE_TIME").getTime()).toLocalDateTime(), is(localDateTime));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Map)}で{@link LocalDateTime}を検索条件とした場合のテスト（SQLServer用）。
     */
    @Test
    @TargetDb(include = Db.SQL_SERVER)
    public void retrieve_map_with_localDateTime_condition_SQLServer() {
        VariousDbTestHelper.createTable(Jsr310ColumnForSqlServer.class);
        LocalDateTime localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        Jsr310ColumnForSqlServer entity =  new Jsr310ColumnForSqlServer();
        entity.id = 12345L;
        entity.localDateTime = localDateTime;
        VariousDbTestHelper.insert(entity);

        // DBによってTimestampを保持する精度が異なるため、「指定したlocalDateTime以降であること」を検索条件とする
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
            "SELECT * FROM JSR310_COLUMN_SQLSERVER WHERE LOCAL_DATE_TIME > :localDateTime");

        Map<String, Object> condition = new HashMap<>();
        condition.put("localDateTime", localDateTime.minusSeconds(1));

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));
        assertThat(new Timestamp(actual.get(0).getDate("LOCAL_DATE_TIME").getTime()).toLocalDateTime(), is(localDateTime));
    }
    
    /**
     * {@link BasicSqlPStatement#retrieve(Object)}で{@link LocalDate}を検索条件とした場合のテスト（SQLServer以外）。
     */
    @Test
    @TargetDb(exclude = Db.SQL_SERVER)
    public void retrieve_object_with_localDate_condition() {
        VariousDbTestHelper.createTable(Jsr310Column.class);
        LocalDate localDate = LocalDate.parse("2015-04-01");
        Jsr310Column entity =  new Jsr310Column();
        entity.id = 12345L;
        entity.localDate = localDate;
        VariousDbTestHelper.insert(entity);

        // DBによってDateを保持する精度が異なるため、「指定したlocalDate以降であること」を検索条件とする
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM JSR310_COLUMN WHERE LOCAL_DATE > :localDate");
        final Jsr310Column condition =  new Jsr310Column();
        condition.localDate = localDate.minusDays(1);

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));
        assertThat(new Timestamp(actual.get(0).getDate("LOCAL_DATE").getTime()).toLocalDateTime().toLocalDate(), is(localDate));

    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}で{@link LocalDate}を検索条件とした場合のテスト（SQLServer用）。
     */
    @Test
    @TargetDb(include = Db.SQL_SERVER)
    public void retrieve_object_with_localDate_condition_SQLServer() {
        VariousDbTestHelper.createTable(Jsr310ColumnForSqlServer.class);
        LocalDate localDate = LocalDate.parse("2015-04-01");
        Jsr310ColumnForSqlServer entity =  new Jsr310ColumnForSqlServer();
        entity.id = 12345L;
        entity.localDate = localDate;
        VariousDbTestHelper.insert(entity);

        // DBによってDateを保持する精度が異なるため、「指定したlocalDate以降であること」を検索条件とする
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
            "SELECT * FROM JSR310_COLUMN_SQLSERVER WHERE LOCAL_DATE > :localDate");
        final Jsr310ColumnForSqlServer condition =  new Jsr310ColumnForSqlServer();
        condition.localDate = localDate.minusDays(1);

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));
        assertThat(new Timestamp(actual.get(0).getDate("LOCAL_DATE").getTime()).toLocalDateTime().toLocalDate(), is(localDate));

    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}で{@link LocalDateTime}を検索条件とした場合のテスト（SQLServer以外）。
     */
    @Test
    @TargetDb(exclude = Db.SQL_SERVER)
    public void retrieve_object_with_localDateTime_condition() {
        VariousDbTestHelper.createTable(Jsr310Column.class);
        LocalDateTime localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        Jsr310Column entity =  new Jsr310Column();
        entity.id = 12345L;
        entity.localDateTime = localDateTime;
        VariousDbTestHelper.insert(entity);

        // DBによってTimestampを保持する精度が異なるため、「指定したlocalDateTime以降であること」を検索条件とする
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
            "SELECT * FROM JSR310_COLUMN WHERE LOCAL_DATE_TIME > :localDateTime");
        final Jsr310Column condition =  new Jsr310Column();
        condition.localDateTime = localDateTime.minusSeconds(1);

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));
        assertThat( new Timestamp(actual.get(0).getDate("LOCAL_DATE_TIME").getTime()).toLocalDateTime(), is(localDateTime));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}で{@link LocalDateTime}を検索条件とした場合のテスト（SQLServer用）。
     */
    @Test
    @TargetDb(include = Db.SQL_SERVER)
    public void retrieve_object_with_localDateTime_condition_SQLServer() {
        VariousDbTestHelper.createTable(Jsr310ColumnForSqlServer.class);
        LocalDateTime localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        Jsr310ColumnForSqlServer entity =  new Jsr310ColumnForSqlServer();
        entity.id = 12345L;
        entity.localDateTime = localDateTime;
        VariousDbTestHelper.insert(entity);

        // DBによってTimestampを保持する精度が異なるため、「指定したlocalDateTime以降であること」を検索条件とする
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
            "SELECT * FROM JSR310_COLUMN_SQLSERVER WHERE LOCAL_DATE_TIME > :localDateTime");
        final Jsr310ColumnForSqlServer condition =  new Jsr310ColumnForSqlServer();
        condition.localDateTime = localDateTime.minusSeconds(1);

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));
        assertThat( new Timestamp(actual.get(0).getDate("LOCAL_DATE_TIME").getTime()).toLocalDateTime(), is(localDateTime));
    }
}