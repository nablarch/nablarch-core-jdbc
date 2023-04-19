package nablarch.core.db.util;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import nablarch.core.repository.SystemRepository;

/**
 * データベースアクセス機能で利用するユーティリティクラス。
 *
 * @author hisaaki sioiri
 */
public final class DbUtil {

    /** コンストラクタ。 */
    private DbUtil() {
    }

    /**
     * オブジェクトが配列または、{@link Collection}か。
     *
     * @param object チェック対象のオブジェクト
     * @return 配列の場合は、true
     */
    public static boolean isArrayObject(Object object) {
        if (object == null) {
            // nullの場合は、配列要素として判断する。
            return true;
        }
        return isCollection(object) || isArray(object);
    }

    /**
     * 指定されたオブジェクトが配列か否か。
     * @param object オブジェクト
     * @return 配列の場合は、true
     */
    private static boolean isArray(Object object) {
        return object.getClass().isArray();
    }

    /**
     * 指定されたオブジェクトが{@link Collection}か否か。
     * @param object オブジェクト
     * @return {@link Collection}の場合は、true
     */
    private static boolean isCollection(Object object) {
        return (object instanceof Collection<?>);
    }

    /**
     * オブジェクトの配列サイズを取得する。<br/>
     * <p/>
     * オブジェクトが配列または、{@link java.util.Collection}以外の場合は、{@link IllegalArgumentException}。<br/>
     * オブジェクトがnullの場合は、0を返却する。
     *
     * @param object オブジェクト
     * @return 配列のサイズ
     */
    public static int getArraySize(Object object) {
        if (!isArrayObject(object)) {
            throw new IllegalArgumentException(String.format(
                    "object type is invalid. valid object type is Array or Collection. object class = [%s]",
                    object.getClass().getName()));
        }
        if (object == null) {
            return 0;
        }
        if (isCollection(object)) {
            return ((Collection<?>) object).size();
        } else {
            return Array.getLength(object);
        }
    }

    /**
     * 配列または、{@link java.util.Collection}オブジェクトから指定された要素の値を取得する。<br/>
     * <p/>
     * オブジェクトが配列または、Collection以外の場合は、{@link IllegalArgumentException}。<br/>
     * オブジェクトがnullの場合は、nullを返却する。
     *
     * @param object オブジェクト(配列または、Collection)
     * @param pos 要素
     * @return 取得した値。(オブジェクトがnullの場合は、null)
     */
    public static Object getArrayValue(Object object, int pos) {
        if (!isArrayObject(object)) {
            throw new IllegalArgumentException(
                    "object type is invalid. valid object type is Array or Collection.");
        }
        if (object == null) {
            return null;
        }
        int size = getArraySize(object);
        if (pos < 0 || pos >= size) {
            throw new IllegalArgumentException(String.format(
                    "specified position is out of range. actual size = [%d], specified position = [%d]",
                    size, pos));
        }
        if (isCollection(object)) {
            Object[] objects = ((Collection<?>) object).toArray();
            return objects[pos];
        } else {
            return Array.get(object, pos);
        }
    }

    /**
     * データベースのBeanに対してフィールドでアクセスするかどうかを返却する。
     * @return フィールドでアクセスする場合true、getterでアクセスする場合false
     */
    public static boolean isFieldAccess() {
        return SystemRepository.getBoolean("nablarch.dbAccess.isFieldAccess");
    }

    /**
     * 指定されたオブジェクトの特定のフィールドの値を返却する。
     * @param data フィールドの値を取得したいオブジェクト
     * @param fieldName フィールド名
     * @return フィールドの値
     * @throws IllegalArgumentException filedNameに対応するプロパティが定義されていない場合
     * @throws RuntimeException フィールドの値の取得に失敗した場合
     */
    public static Object getField(final Object data, final String fieldName) {
        try {
            final Field field = findDeclaredField(data.getClass(), fieldName);
            if (field == null) {
                throw new IllegalArgumentException(String.format(
                        "specified filed [%s] is not declared in the class [%s].",
                        fieldName, data.getClass().getName()));
            }
            field.setAccessible(true);
            return field.get(data);
        } catch (IllegalAccessException e) {
            // setAccessible(true) でアクセス可能にしているので、この例外がスローされることはない
            throw new RuntimeException(String.format(
                    "failed to access the filed [%s]  of the class [%s].",
                    fieldName, data.getClass().getName()) ,e);
        }
    }

    /**
     * 指定されたフィールドがクラスから取得する。親クラスも再帰的に検索する。
     * @param clazz 対象クラス
     * @param fieldName フィールド名
     * @return 見つかったフィールド、存在しなければnull
     */
    private static Field findDeclaredField(final Class<?> clazz, final String fieldName) {
        final Field[] fields = clazz.getDeclaredFields();
        for (final Field field : fields) {
            if (field.getName().equals(fieldName)) {
                return field;
            }
        }
        final Class<?> supperClass = clazz.getSuperclass();
        if (supperClass != null) {
            return findDeclaredField(supperClass, fieldName);
        }
        return null;
    }

    /**
     * オブジェクトの親クラスも含めたすべてのフィールドをMapにコピーする。
     *
     * @param data コピー元のオブジェクト
     * @return オブジェクトのフィールドをコピーしたMap
     * @throws RuntimeException フィールドの値の取得に失敗した場合
     */
    public static Map<String,Object> createMapAndCopy(final Object data) {
        return createMapInner(data.getClass(), data, new HashSet<String>());
    }

    /**
     * オブジェクトのフィールドをMapにコピーする。<br/>
     * 親のクラスも再帰的に呼び出しMapに格納する。
     *
     * @param clazz 対象のクラス
     * @param data コピー元のオブジェクト
     * @param addedFieldNames 既にMapに設定したfield名
     * @return オブジェクトのフィールドをコピーしたMap
     * @throws RuntimeException フィールドから値の取得に失敗した場合
     */
    private static Map<String, Object> createMapInner(final Class<?> clazz, final Object data, final Set<String> addedFieldNames) {
        final Map<String, Object> result = new HashMap<String, Object>();
        final Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            final String fieldName = field.getName();
            if (addedFieldNames.contains(fieldName)) {
                continue;
            }
            addedFieldNames.add(fieldName);
            final Object value;
            try {
                field.setAccessible(true);
                value = field.get(data);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(String.format(
                        "failed to access the filed [%s] of the class [%s].",
                        field.getName(), clazz.getName()), e);
            }
            result.put(fieldName, value);
        }
        final Class<?> supperClass = clazz.getSuperclass();
        if (supperClass != null) {
            result.putAll(createMapInner(supperClass, data, addedFieldNames));//親のクラスのフィールドも取得
        }
        return result;
    }
}
