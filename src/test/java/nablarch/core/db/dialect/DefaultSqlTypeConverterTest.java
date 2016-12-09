package nablarch.core.db.dialect;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Types;

import static org.junit.Assert.assertTrue;

/**
 * {@link DefaultSqlTypeConverter}のテスト
 * @author ryo asato
 */
public class DefaultSqlTypeConverterTest {

    private DefaultSqlTypeConverter sut = new DefaultSqlTypeConverter();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void convert() {
        assertTrue("CHARは変換可能", sut.convertToJavaClass(Types.CHAR).isAssignableFrom(String.class));
    }

    @Test
    public void convertAbnormalSqlType() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("unsupported sqlType: 12345");
        sut.convertToJavaClass(12345);
    }
}
