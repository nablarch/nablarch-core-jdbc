package nablarch.core.db.util;

import nablarch.core.repository.SystemRepository;
import org.hamcrest.collection.IsMapContaining;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link DbUtil}のテストクラス。
 *
 * @author hisaaki sioiri
 */
public class DbUtilTest {
    @Before
    public void setUp() throws Exception {
        SystemRepository.clear();
    }

    @After
    public void tearDown() throws Exception {
        SystemRepository.clear();
    }

    /**
     * {@link DbUtil#isArrayObject(Object)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testIsArrayObject() throws Exception {

        assertTrue("nullはOK", DbUtil.isArrayObject(null));
        assertTrue("String[]は、OK", DbUtil.isArrayObject(new String[0]));
        assertTrue("String[5]は、OK", DbUtil.isArrayObject(new String[5]));
        assertTrue("int[5]はOK", DbUtil.isArrayObject(new int[5]));
        assertTrue("ArrayListは、OK", DbUtil.isArrayObject(new ArrayList()));
        assertTrue("Vectorは、OK", DbUtil.isArrayObject(new Vector()));
        assertTrue("Collectionは、OK", DbUtil.isArrayObject(new StringCollection()));

        assertFalse("StringはNG", DbUtil.isArrayObject(""));
        assertFalse("intはNG", DbUtil.isArrayObject(500));

    }

    /**
     * {@link DbUtil#getArraySize(Object)} のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testGetArraySize() throws Exception {

        assertThat("nullは、0", DbUtil.getArraySize(null), is(0));

        // 配列の指定
        assertThat("サイズ0の配列", DbUtil.getArraySize(new String[0]), is(0));
        assertThat("指定した配列のサイズが返却される", DbUtil.getArraySize(new int[10]), is(10));

        // Collection
        assertThat("空のVector", DbUtil.getArraySize(new Vector()), is(0));
        assertThat("サイズありのArrayList", DbUtil.getArraySize(new ArrayList<String>() {
            {
                add("1");
            }
        }), is(1));
        assertThat("サイズありのCollection", DbUtil.getArraySize(new StringCollection() {
            {
                add("");
                add("");
            }
        }), is(2));
    }

    /**
     * {@link DbUtil#getArraySize(Object)} の異常テスト。
     *
     * @throws Exception
     */
    @Test
    public void testGetArraySizeError() throws Exception {
        // Stringを指定した場合
        try {
            DbUtil.getArraySize("aaa");
            fail("do not run");
        } catch (Exception e) {
            assertThat(e.getMessage(),
                    is("object type is invalid. valid object type is Array or Collection."
                            + " object class = [java.lang.String]"));
        }

        // 自分自身を指定
        try {
            DbUtil.getArraySize(this);
            fail("do not run");
        } catch (Exception e) {
            assertThat(e.getMessage(), is(String
                    .format("object type is invalid. valid object type is Array or Collection."
                            + " object class = [%s]", this.getClass().getName())));
        }
    }

    /**
     * {@link DbUtil#getArrayValue(Object, int)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testGetArrayValue() throws Exception {

        // 配列指定
        assertThat("nullは、null", DbUtil.getArrayValue(null, 0), nullValue());
        String[] strings = {"1", "2", "3"};
        assertThat("配列のrangeない指定", (String) DbUtil.getArrayValue(strings, 0), is("1"));
        assertThat("配列のrangeない指定", (String) DbUtil.getArrayValue(strings, 1), is("2"));
        assertThat("配列のrangeない指定", (String) DbUtil.getArrayValue(strings, 2), is("3"));

        // List指定
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        assertThat("配列のrangeない指定", (String) DbUtil.getArrayValue(list, 0), is("a"));
        assertThat("配列のrangeない指定", (String) DbUtil.getArrayValue(list, 1), is("b"));
    }

    /**
     * {@link DbUtil#getArrayValue(Object, int)}の異常系テスト。
     */
    @Test
    public void testGetArrayValueError() {

        // 配列以外を指定
        try {
            // String
            DbUtil.getArrayValue("", 0);
            fail("do not run.");
        } catch (Exception e) {
            assertThat(e.getMessage(),
                    is("object type is invalid. valid object type is Array or Collection."));
        }

        // 配列のrange外を指定
        String[] strings = {"1", "2", "3"};
        try {
            DbUtil.getArrayValue(strings, 3);
            fail("do not run");
        } catch (Exception e) {
            assertThat(e.getMessage(),
                    is("specified position is out of range. actual size = [3], specified position = [3]"));
        }
        try {
            DbUtil.getArrayValue(strings, -1);
            fail("do not run");
        } catch (Exception e) {
            assertThat(e.getMessage(),
                    is("specified position is out of range. actual size = [3], specified position = [-1]"));
        }
    }

    /**
     * {@link DbUtil#getField(Object, String)} のテスト
     * @throws Exception
     */
    @Test
    public void testGetFiled() throws Exception {
        Object grandObjectValue = new Object();
        Object parentObjectValue = new Object();
        Object objectValue = new Object();
        DbUtilTestEntity bean = new DbUtilTestEntity(
                "grandPrivateStringValue", -3, -2L, grandObjectValue,
                "parentPrivateStringValue", -1, 0L, parentObjectValue,
                "privateStringValue", 1, 2L, objectValue);
        assertThat("親の親のprivateフィールドへのアクセス", (String)DbUtil.getField(bean, "grandPrivateString"), is("grandPrivateStringValue"));
        assertThat("親の親のprotectedフィールドへのアクセス", (Integer)DbUtil.getField(bean, "grandProtectedInt"), is(-3));
        assertThat("親の親のdefaultフィールドへのアクセス", (Long)DbUtil.getField(bean,"grandDefaultLong"), is(-2L));
        assertThat("親の親のpublicフィールドへのアクセス", DbUtil.getField(bean, "grandPublicObject"), is(grandObjectValue));
        assertThat("親の親のprivateフィールドへのアクセス", (String)DbUtil.getField(bean, "parentPrivateString"), is("parentPrivateStringValue"));
        assertThat("親のprotectedフィールドへのアクセス", (Integer)DbUtil.getField(bean, "parentProtectedInt"), is(-1));
        assertThat("親のdefaultフィールドへのアクセス", (Long)DbUtil.getField(bean,"parentDefaultLong"), is(0L));
        assertThat("親のpublicフィールドへのアクセス", DbUtil.getField(bean, "parentPublicObject"), is(parentObjectValue));
        assertThat("privateフィールドへのアクセス", (String)DbUtil.getField(bean, "privateString"), is("privateStringValue"));
        assertThat("protectedフィールドへのアクセス", (Integer)DbUtil.getField(bean, "protectedInt"), is(1));
        assertThat("defaultフィールドへのアクセス", (Long)DbUtil.getField(bean,"defaultLong"), is(2L));
        assertThat("publicフィールドへのアクセス", DbUtil.getField(bean, "publicObject"), is(objectValue));
    }

    /**
     * {@link DbUtil#getField(Object, String)} の異常系テスト
     * @throws Exception
     */
    @Test
    public void testGetFiledError() throws Exception {
        Object grandObjectValue = new Object();
        Object parentObjectValue = new Object();
        Object objectValue = new Object();
        DbUtilTestEntity bean = new DbUtilTestEntity(
                "grandPrivateStringValue", -3, -2L, grandObjectValue,
                "parentPrivateStringValue", -1, 0L, parentObjectValue,
                "privateStringValue", 1, 2L, objectValue);
        try {
            DbUtil.getField(bean, "notExistingField");
            fail("Should not reach here.");
        } catch (IllegalArgumentException e) {
            assertThat("存在しないフィールドへのアクセスでは例外を送出", e.getMessage(),
                    is("specified filed [notExistingField] is not declared in the class [nablarch.core.db.util.DbUtilTest$DbUtilTestEntity]."));
        }
    }

    /**
     * {@link DbUtil#createMapAndCopy(Object)} のテスト
     * @throws Exception
     */
    @Test
    public void testCreateMapAndCopy() throws Exception {
        Object grandObjectValue = new Object();
        Object parentObjectValue = new Object();
        Object objectValue = new Object();
        DbUtilTestEntity bean = new DbUtilTestEntity(
                "grandPrivateStringValue", -3, -2L, grandObjectValue,
                "parentPrivateStringValue", -1, 0L, parentObjectValue,
                "privateStringValue", 1, 2L, objectValue);
        Map<String, Object> actual = DbUtil.createMapAndCopy(bean);
        
        assertThat(actual, IsMapContaining.<String, Object>hasEntry("grandPrivateString", "grandPrivateStringValue"));
        assertThat(actual, IsMapContaining.<String, Object>hasEntry("grandProtectedInt", -3));
        assertThat(actual, IsMapContaining.<String, Object>hasEntry("grandDefaultLong", -2L));
        assertThat(actual, IsMapContaining.<String, Object>hasEntry("grandPublicObject", grandObjectValue));
        assertThat(actual, IsMapContaining.<String, Object>hasEntry("parentPrivateString", "parentPrivateStringValue"));
        assertThat(actual, IsMapContaining.<String, Object>hasEntry("parentProtectedInt", -1));
        assertThat(actual, IsMapContaining.<String, Object>hasEntry("parentDefaultLong", 0L));
        assertThat(actual, IsMapContaining.<String, Object>hasEntry("parentPublicObject", parentObjectValue));
        assertThat(actual, IsMapContaining.<String, Object>hasEntry("privateString", "privateStringValue"));
        assertThat(actual, IsMapContaining.<String, Object>hasEntry("protectedInt", 1));
        assertThat(actual, IsMapContaining.<String, Object>hasEntry("defaultLong", 2L));
        assertThat(actual, IsMapContaining.<String, Object>hasEntry("publicObject", objectValue));
        assertThat(actual, IsMapContaining.<String, Object>hasEntry("duplicateFieldName", "ChildField"));
    }

    private static class StringCollection implements Collection<String> {

        private int size = 0;

        public int size() {
            return size;
        }

        public boolean isEmpty() {
            return false;
        }

        public boolean contains(Object o) {
            return false;
        }

        public Iterator<String> iterator() {
            return null;
        }

        public Object[] toArray() {
            return new Object[0];
        }

        public <T> T[] toArray(T[] a) {
            return null;
        }

        public boolean add(String s) {
            size++;
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public boolean remove(Object o) {
            size--;
            return false;
        }

        public boolean containsAll(Collection<?> c) {
            return false;
        }

        public boolean addAll(Collection<? extends String> c) {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public boolean removeAll(Collection<?> c) {
            return false;
        }

        public boolean retainAll(Collection<?> c) {
            return false;
        }

        public void clear() {
        }
    }

    public static class DbUtilTestEntity extends DbUtilTestEntityParent {
        private String privateString;
        protected int protectedInt;
        long defaultLong;
        public Object publicObject;
        private String duplicateFieldName = "ChildField";

        public DbUtilTestEntity(
                String grandPrivateString, int grandProtectedInt, long grandDefaultLong, Object grandPublicObject,
                String parentPrivateString, int parentPrtotectedInt, long parentDefaultLong, Object parentPublicObject,
                String privateString, int protectedInt, long defaultLong, Object publicObject) {
            super( grandPrivateString, grandProtectedInt,grandDefaultLong, grandPublicObject,
                    parentPrivateString, parentPrtotectedInt, parentDefaultLong, parentPublicObject);
            this.privateString = privateString;
            this.protectedInt = protectedInt;
            this.defaultLong = defaultLong;
            this.publicObject = publicObject;
        }
    }
    public static class DbUtilTestEntityParent extends DbUtilTestEntityGrandParent{
        //親のフィールドを取るテストのためのEntity
        private String parentPrivateString;
        protected int parentProtectedInt;
        long parentDefaultLong;
        public Object parentPublicObject;
        private String duplicateFieldName =  "ParentField";;

        public DbUtilTestEntityParent(
                String grandPrivateString, int grandProtectedInt, long grandDefaultLong, Object grandPublicObject,
                String parentPrivateString, int parentProtectedInt, long parentDefaultLong, Object parentPublicObject) {
            super(grandPrivateString, grandProtectedInt,grandDefaultLong, grandPublicObject);
            this.parentPrivateString = parentPrivateString;
            this.parentProtectedInt = parentProtectedInt;
            this.parentDefaultLong = parentDefaultLong;
            this.parentPublicObject = parentPublicObject;
        }
    }
    public static class DbUtilTestEntityGrandParent {
        //親の親のフィールドを取るテストのためのEntity
        private String grandPrivateString;
        protected int grandProtectedInt;
        long grandDefaultLong;
        public Object grandPublicObject;
        private String duplicateFieldName = "GrandParentField";;

        public DbUtilTestEntityGrandParent(String grandPrivateString, int grandProtectedInt, long grandDefaultLong, Object grandPublicObject) {
            this.grandPrivateString = grandPrivateString;
            this.grandProtectedInt = grandProtectedInt;
            this.grandDefaultLong = grandDefaultLong;
            this.grandPublicObject = grandPublicObject;
        }
    }

}
