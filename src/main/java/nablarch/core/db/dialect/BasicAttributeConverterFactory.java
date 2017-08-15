package nablarch.core.db.dialect;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import nablarch.core.db.dialect.converter.AttributeConverter;
import nablarch.core.db.dialect.converter.BigDecimalAttributeConverter;
import nablarch.core.db.dialect.converter.BooleanAttributeConverter;
import nablarch.core.db.dialect.converter.ByteArrayAttributeConverter;
import nablarch.core.db.dialect.converter.IntegerAttributeConverter;
import nablarch.core.db.dialect.converter.LongAttributeConverter;
import nablarch.core.db.dialect.converter.ShortAttributeConverter;
import nablarch.core.db.dialect.converter.SqlDateAttributeConverter;
import nablarch.core.db.dialect.converter.StringAttributeConverter;
import nablarch.core.db.dialect.converter.TimestampAttributeConverter;
import nablarch.core.db.dialect.converter.UtilDateAttributeConverter;

/**
 * {@link AttributeConverterFactory}の基本実装クラス。
 *
 * @author siosio
 */
public class BasicAttributeConverterFactory implements AttributeConverterFactory {

    /** 型変換を行う{@link AttributeConverter}定義。 */
    private final Map<Class<?>, AttributeConverter<?>> attributeConverterMap;
    
    /**
     * コンストラクタ。
     */
    public BasicAttributeConverterFactory() {
        final Map<Class<?>, AttributeConverter<?>> attributeConverterMap = new HashMap<Class<?>, AttributeConverter<?>>();
        attributeConverterMap.put(String.class, new StringAttributeConverter());
        attributeConverterMap.put(Short.class, new ShortAttributeConverter());
        attributeConverterMap.put(short.class, new ShortAttributeConverter.Primitive());
        attributeConverterMap.put(Integer.class, new IntegerAttributeConverter());
        attributeConverterMap.put(int.class, new IntegerAttributeConverter.Primitive());
        attributeConverterMap.put(Long.class, new LongAttributeConverter());
        attributeConverterMap.put(long.class, new LongAttributeConverter.Primitive());
        attributeConverterMap.put(BigDecimal.class, new BigDecimalAttributeConverter());
        attributeConverterMap.put(java.sql.Date.class, new SqlDateAttributeConverter());
        attributeConverterMap.put(java.util.Date.class, new UtilDateAttributeConverter());
        attributeConverterMap.put(Timestamp.class, new TimestampAttributeConverter());
        attributeConverterMap.put(byte[].class, new ByteArrayAttributeConverter());
        attributeConverterMap.put(Boolean.class, new BooleanAttributeConverter());
        attributeConverterMap.put(boolean.class, new BooleanAttributeConverter.Primitive());
        
        this.attributeConverterMap = attributeConverterMap;
    }

    @Override
    public <T> AttributeConverter<T> factory(final Class<T> type) {
        @SuppressWarnings("unchecked") 
        final AttributeConverter<T> converter = (AttributeConverter<T>) attributeConverterMap.get(type);
        if (converter == null) {
            throw new IllegalStateException("This dialect does not support [" + type.getSimpleName() + "] type.");
        }
        return converter;
    }
}
