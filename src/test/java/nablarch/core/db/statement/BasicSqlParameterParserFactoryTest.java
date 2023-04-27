package nablarch.core.db.statement;

import nablarch.core.db.statement.sqlconvertor.VariableInSyntaxConvertor;
import nablarch.test.support.reflection.ReflectionUtil;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * {@link BasicSqlParameterParserFactory}のテストクラス。
 */
public class BasicSqlParameterParserFactoryTest {

    private BasicSqlParameterParserFactory sut = new BasicSqlParameterParserFactory();

    /**
     * デフォルト設定の場合
     */
    @Test
    public void createSqlParameterParser() throws Exception {
        SqlParameterParser parser = sut.createSqlParameterParser();
        assertThat(parser, is(instanceOf(BasicSqlParameterParser.class)));
    }

    /**
     * {@link SqlConvertor}を設定した場合
     */
    @Test
    public void createSqlParameterParserFromCustomSetting() throws Exception {
        sut.setSqlConvertors(Collections.<SqlConvertor>singletonList(new VariableInSyntaxConvertor()));
        final SqlParameterParser parser = sut.createSqlParameterParser();
        final SqlConvertor[] convertors = ReflectionUtil.getFieldValue(parser, "sqlConvertors");
        assertThat(convertors.length, is(1));
        assertThat(convertors[0], is(instanceOf(VariableInSyntaxConvertor.class)));
    }
}