package nablarch.core.db.dialect;

import nablarch.core.db.dialect.converter.AttributeConverter;
import nablarch.core.util.annotation.Published;

/**
 * データベースとの入出力時に型変換を行うConverterを生成する。
 *
 * @author siosio
 */
@Published(tag = "architect")
public interface AttributeConverterFactory {

    /**
     * 指定された型に対応した{@link AttributeConverter}を返す。
     *
     * @param type 型
     * @return 生成したコンバータ
     */
    <T> AttributeConverter<T> factory(Class<T> type);
}
