package nablarch.core.db.statement.sqlpreprocessor;

/**
 * SQL文の前処理を行うインタフェース。
 *
 * @author Tsuyoshi Kawasaki
 * @see nablarch.core.db.statement.BasicSqlLoader
 */
public interface SqlPreProcessor {

    /**
     * 前処理を行う。
     * @param original 元のSQL文
     * @return 前処理実行後のSQL文
     */
    String preProcess(String original);
}
