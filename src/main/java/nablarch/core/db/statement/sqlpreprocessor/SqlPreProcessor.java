package nablarch.core.db.statement.sqlpreprocessor;

import nablarch.core.util.annotation.Published;

/**
 * SQL文の前処理を行うインタフェース。
 *
 * @author Tsuyoshi Kawasaki
 * @see nablarch.core.db.statement.BasicSqlLoader
 */
@Published(tag = "architect")
public interface SqlPreProcessor {

    /**
     * 前処理を行う。
     *
     * @param before 元のSQL文
     * @param sqlId 元SQLのSQL_ID
     * @return 前処理実行後のSQL文
     */
    String preProcess(String before, String sqlId);
}
