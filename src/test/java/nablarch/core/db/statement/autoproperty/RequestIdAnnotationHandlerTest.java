package nablarch.core.db.statement.autoproperty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.lang.reflect.Field;
import java.sql.Timestamp;

import nablarch.core.ThreadContext;

import org.junit.Test;

/**
 * {@link RequestIdAnnotationHandler}のテストクラス。
 * @author Kiyohito Itoh
 */
public class RequestIdAnnotationHandlerTest {

    @Test
    public void handle() throws Exception {
        
        RequestIdAnnotationHandler handler = new RequestIdAnnotationHandler();
        ThreadContext.setRequestId("req_id");

        Entity entity = new Entity();
        handler.handle(entity);

        Field exeId1 = entity.getClass().getSuperclass().getSuperclass().getDeclaredField("reqId1");
        exeId1.setAccessible(true);
        Field ymdLng = entity.getClass().getSuperclass().getDeclaredField("ymdLng");
        ymdLng.setAccessible(true);

        assertEquals("親の親クラスでもOK", "req_id", exeId1.get(entity));
        assertEquals("req_id", entity.reqId2);
        assertEquals(null, entity.reqId3);

        assertNull("ExecutionIdアノテーション以外は何も設定されない。", entity.date);

        assertEquals("アノテーションが設定されていないフィールドは変更されない。", 100, entity.cnt);

        // アノテーションのついていないクラスでもOK
        NoAnnotationClass noAnnotationClass = new NoAnnotationClass();
        handler.handle(noAnnotationClass);
        assertNull(noAnnotationClass.reqId);
    }

    private static class Base {
        @UserId
        private String userId1;
        @CurrentDateTime(format = "yyyyMM")
        private Integer ymInt;
        @ExecutionId
        private String exeId1;
        @RequestId
        private String reqId1;
    }

    private static class Base2 extends Base {
        @CurrentDateTime(format = "yyyyMMdd")
        private Long ymdLng;
    }

    private static class Entity extends Base2 {
        @UserId
        private String userId2;
        private String userId3;

        @CurrentDateTime
        private java.sql.Date date;
        @CurrentDateTime
        private java.sql.Time time;
        @CurrentDateTime
        private Timestamp timestamp;
        @CurrentDateTime
        private String defaultFormatDate;
        @CurrentDateTime(format = "yyyy")
        private String year;
        @CurrentDateTime(format = "yyyyMMdd")
        private String dateStr;
        @CurrentDateTime(format = "hhmmss")
        private String timeStr;
        @CurrentDateTime(format = "yyyyMMddhhmmssSSS")
        private String timestampStr;

        @CurrentDateTime(format = "yyyy")
        private int yearInt;
        @CurrentDateTime(format = "yyyyMM")
        private long ymLng;

        @ExecutionId
        private String exeId2;
        private String exeId3;

        @RequestId
        private String reqId2;
        private String reqId3;
        
        private int cnt = 100;
    }

    private static class NoAnnotationClass {
        private String reqId;
    }
}
