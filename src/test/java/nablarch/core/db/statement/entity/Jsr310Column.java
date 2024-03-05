package nablarch.core.db.statement.entity;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Date and Time API（JSR-310）の型（{@link LocalDate}, {@link LocalDateTime}）の動作検証用エンティティ
 */
@SuppressWarnings("JpaDataSourceORMInspection")
@Entity
@Table(name = "jsr310_column")
public class Jsr310Column {

    @Id
    @Column(name = "id", length = 18)
    public Long id;

    @Column(name = "local_date", columnDefinition = "date")
    public LocalDate localDate;

    @Column(name = "local_date_time", columnDefinition = "timestamp")
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