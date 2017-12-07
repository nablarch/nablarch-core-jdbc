package nablarch.core.db.statement.sqlloader;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * {@link SchemaReplacer}のテスト。
 */
public class SchemaReplacerTest {
    private SchemaReplacer sut = new SchemaReplacer();

    @Test
    public void スキーマのプレースホルダーが置換されること() {
        sut.setSchemaName("AAA");
        // 2箇所のプレースホルダー
        String sql = "SELECT EMP.NAME, DEPT.NAME FROM #SCHEMA#.EMPLOYEE EMP INNER JOIN #SCHEMA#.DEPARTMENT DEPT ON EMP.DEPT_ID = DEPT.DEPT_ID";
        String actual = sut.processOnAfterLoad(sql, "SQL_ID");
        assertThat(actual, is("SELECT EMP.NAME, DEPT.NAME FROM AAA.EMPLOYEE EMP INNER JOIN AAA.DEPARTMENT DEPT ON EMP.DEPT_ID = DEPT.DEPT_ID"));
    }

    @Test
    public void プレースホルダーがない場合は元のSQLと同じであること() {
        sut.setSchemaName("AAA");
        String sql = "SELECT * FROM EMPLOYEE";
        String actual = sut.processOnAfterLoad(sql, "SQL_ID");
        assertThat(actual, is("SELECT * FROM EMPLOYEE"));
    }

    @Test(expected = IllegalStateException.class)
    public void スキーマ名が設定されていない場合_前処理実行時に例外が発生すること() {
        sut.processOnAfterLoad("SELECT * FROM DUAL", "SQL_ID");
    }

}