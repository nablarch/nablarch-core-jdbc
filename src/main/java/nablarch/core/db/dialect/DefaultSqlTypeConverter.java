package nablarch.core.db.dialect;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link SqlTypeConverter}のデフォルト実装
 * @author ryo asato
 */
public class DefaultSqlTypeConverter implements SqlTypeConverter {

    /**
     * SQL型に対応するJavaクラスのマッピング定義。
     */
    private static final Map<Integer, Class> CLASS_CONVERTER_MAP;

    static {
        final Map<Integer, Class> classConverterMap = new HashMap<Integer, Class>();
        // TODO マッピング
        classConverterMap.put(Types.CHAR, String.class);

        CLASS_CONVERTER_MAP = Collections.unmodifiableMap(classConverterMap);
    }

    @Override
    public Class convertToJavaClass(int sqlType) {
        Class converted = CLASS_CONVERTER_MAP.get(sqlType);
        if (converted == null) {
            throw new IllegalArgumentException("unsupported sqlType: " + sqlType);
        }
        return converted;
    }
}
