package nablarch.core.db.statement.entity;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Date and Time API（JSR-310）の型（{@link LocalDate}, {@link LocalDateTime}）の動作検証用エンティティ（SQLServer用）
 */
@SuppressWarnings("JpaDataSourceORMInspection")
@Entity
@Table(name = "jsr310_column_sqlserver")
public class Jsr310ColumnForSqlServer {

    @Id
    @Column(name = "id", length = 18)
    public Long id;

    @Column(name = "local_date", columnDefinition = "date")
    public LocalDate localDate;
    
    // "timestamp"で生成されるTIMESTAMP型はSQLSererだと日時型ではないため、DATETIME2型にする
    @Column(name = "local_date_time", columnDefinition = "datetime2")
    public LocalDateTime localDateTime;

    @Id
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public LocalDate getLocalDate() {
        return localDate;
    }

    public void setLocalDate(LocalDate localDate) {
        this.localDate = localDate;
    }

    public LocalDateTime getLocalDateTime() {
        return localDateTime;
    }

    public void setLocalDateTime(LocalDateTime localDateTime) {
        this.localDateTime = localDateTime;
    }
}