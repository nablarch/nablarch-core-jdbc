package nablarch.core.db.statement.sqlloader;

import nablarch.core.util.annotation.Published;

/**
 * SQL文ロード時のコールバック処理を行うインタフェース。
 *
 * @author Tsuyoshi Kawasaki
 * @see nablarch.core.db.statement.BasicSqlLoader
 */
@Published(tag = "architect")
public interface SqlLoaderCallback {

    /**
     * SQL文ロード後の加工処理を行う。
     * 引数で与えられたSQL文に対して任意の処理を行うことができる。
     * 本メソッドの戻り値で加工後のSQL文を返却しなければならない。
     *
     * @param sql ロードされたSQL文
     * @param sqlId 元SQLのSQL_ID
     * @return 処理実行後のSQL文
     */
    String processOnAfterLoad(String sql, String sqlId);
}
