package nablarch.core.db.statement.sqlloader;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class SqlPlaceHolderReplacerTest {

    private SqlPlaceHolderReplacer sut = new SqlPlaceHolderReplacer();

    @Before
    public void setUp() {
        Map<String, String>  m = new HashMap<String, String>();
        m.put("#SCHEMA_A#", "AAA");
        m.put("#SCHEMA_B#", "BBB");
        sut.setPlaceHolderValuePair(m);
    }

    @Test
    public void 複数種類のプレースホルダーの置き換えができること() {
        // 2種類のプレースホルダー
        String sql = "SELECT EMP.NAME, DEPT.NAME FROM #SCHEMA_A#.EMPLOYEE EMP INNER JOIN #SCHEMA_B#.DEPARTMENT DEPT ON EMP.DEPT_ID = DEPT.DEPT_ID";
        String actual = sut.processOnAfterLoad(sql, "SQL_ID");
        assertThat(actual, is("SELECT EMP.NAME, DEPT.NAME FROM AAA.EMPLOYEE EMP INNER JOIN BBB.DEPARTMENT DEPT ON EMP.DEPT_ID = DEPT.DEPT_ID"));
    }

    @Test
    public void 同一プレースホルダーが複数回置換されること() {
        // 2箇所のプレースホルダー
        String sql = "SELECT EMP.NAME, DEPT.NAME FROM #SCHEMA_A#.EMPLOYEE EMP INNER JOIN #SCHEMA_A#.DEPARTMENT DEPT ON EMP.DEPT_ID = DEPT.DEPT_ID";
        String actual = sut.processOnAfterLoad(sql, "SQL_ID");
        assertThat(actual, is("SELECT EMP.NAME, DEPT.NAME FROM AAA.EMPLOYEE EMP INNER JOIN AAA.DEPARTMENT DEPT ON EMP.DEPT_ID = DEPT.DEPT_ID"));
    }

    @Test
    public void プレースホルダーがない場合は元のSQLと同じであること() {
        String sql = "SELECT * FROM EMPLOYEE";
        String actual = sut.processOnAfterLoad(sql, "SQL_ID");
        assertThat(actual, is("SELECT * FROM EMPLOYEE"));
    }

    @Test(expected = IllegalStateException.class)
    public void プレースホルダーの設定がされていない場合_処理実行時に例外が発生すること() {
        // プレースホルダーの設定がされていない初期状態で処理実行
        sut = new SqlPlaceHolderReplacer();
        sut.processOnAfterLoad("SELECT * FROM DUAL", "SQL_ID");
    }
}