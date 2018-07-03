package it.cnr.isti.hpc.wikipedia.common;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Maxwell.Lee
 * @date 2018-07-02 10:47
 * @since   1.0.0
 */
public class TableMetadata {

    private String tableName;

    private List<FieldMetadata> fields;

    private AtomicLong idGen;

    public TableMetadata(String tableName, List<FieldMetadata> fields, Long beginId) {
        this.tableName = tableName;
        this.fields = fields;
        this.idGen = new AtomicLong(beginId);
    }

    public String getTableName() {
        return tableName;
    }

    public List<FieldMetadata> getFields() {
        return fields;
    }

    public Long nextId() {
        return idGen.addAndGet(1);
    }
}
