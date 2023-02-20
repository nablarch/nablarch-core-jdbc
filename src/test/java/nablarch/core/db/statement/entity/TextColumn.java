package nablarch.core.db.statement.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "text_table")
public class TextColumn {

    @Column(name = "id", length = 10)
    @Id
    public Integer id;

    @Column(name = "text_col", columnDefinition = "text")
    public String text;

    public Integer getId() {
        return id;
    }

    public void setId(final Integer id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(final String text) {
        this.text = text;
    }
}
