package nablarch.core.db.dialect;

import java.util.HashMap;
import java.util.Map;

import nablarch.core.db.dialect.converter.AttributeConverter;
import nablarch.core.db.dialect.converter.OracleStringAttributeConverter;

/**
 * {@link AttributeConverterFactory}のOracle用実装クラス。
 *
 * @author siosio
 */
public class OracleAttributeConverterFactory implements AttributeConverterFactory {

    /** 型変換を行う{@link AttributeConverter}定義。 */
    private final Map<Class<?>, AttributeConverter<?>> attributeConverterMap;
    
    /** 委譲先 */
    private final AttributeConverterFactory delegatee = new BasicAttributeConverterFactory();

    /**
     * コンストラクタ。
     */
    public OracleAttributeConverterFactory() {
        final Map<Class<?>, AttributeConverter<?>> attributeConverterMap = new HashMap<Class<?>, AttributeConverter<?>>();
        attributeConverterMap.put(String.class, new OracleStringAttributeConverter());
        this.attributeConverterMap = attributeConverterMap;
    }

    @Override
    public <T> AttributeConverter<T> factory(final Class<T> type) {
        @SuppressWarnings("unchecked") 
        final AttributeConverter<T> converter = (AttributeConverter<T>) attributeConverterMap.get(type);
        if (converter == null) {
            return delegatee.factory(type);
        }
        return converter;
    }
}
