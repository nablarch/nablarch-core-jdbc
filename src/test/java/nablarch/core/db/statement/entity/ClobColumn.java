package nablarch.core.db.statement.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "clob_table")
public class ClobColumn {

    @Column(name = "id", length = 10)
    @Id
    public Integer id;

    @Column(name = "clob_col", columnDefinition = "clob")
    public String clob;

    public Integer getId() {
        return id;
    }

    public void setId(final Integer id) {
        this.id = id;
    }

    public String getClob() {
        return clob;
    }

    public void setClob(final String clob) {
        this.clob = clob;
    }
}
