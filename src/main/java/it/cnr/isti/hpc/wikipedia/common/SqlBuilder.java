package it.cnr.isti.hpc.wikipedia.common;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Maxwell.Lee
 * @date 2018-07-02 10:46
 * @since   0.0.1
 */
public class SqlBuilder {

    private Map<String, String> insertSqlHeadPartCache = new HashMap<>();

    public InsertSql buildInsertSql(TableMetadata tableMeta, DataRecord record) {
        if (tableMeta == null || record == null) {
            throw new IllegalArgumentException("参数不全。[0x00SB2463]");
        }

        String headPart = buildInsertSqlHeadPart(tableMeta);

        StringBuilder buf = new StringBuilder();
        buf.append(" ( ");

        boolean isFirst = true;
        Long id = null;
        for (FieldMetadata field : tableMeta.getFields()) {
            String val = record.getValue(field.getName());

            if (!isFirst) buf.append(',');
            isFirst = false;

            if (field.isIdKey()) {
                id = tableMeta.nextId();
                buf.append(val != null ? val : id);
                continue;
            }
            if (val == null || field.isValueOfNumberType()) {
                buf.append(val != null ? val : field.getDefaultValue());
                continue;
            }

            //为了性能考虑，最好不要预生成字符串；
            buf.append('"');
            appendByMaskDoubleQuota(buf, val);
            buf.append('"');
        }
        buf.append(" ) ");

        return new InsertSql(tableMeta.getTableName(), id, headPart, buf.toString());
    }

    private static void appendByMaskDoubleQuota(StringBuilder buf, String val) {
        if (!val.contains("\\") && !val.contains("\"")) {
            buf.append(val);
            return;
        }

        for (char c : val.toCharArray()) {
            if (c == '\\') {
                buf.append("\\\\");
            } else if (c == '"') {
                buf.append("\\\"");
            } else {
                buf.append(c);
            }
        }
    }

    private String buildInsertSqlHeadPart(TableMetadata tableMeta) {
        if (insertSqlHeadPartCache.containsKey(tableMeta.getTableName())) {
            return insertSqlHeadPartCache.get(tableMeta.getTableName());
        }

        StringBuilder buf = new StringBuilder();

        buf.append("INSERT INTO ").append(tableMeta.getTableName()).append(" (");

        boolean isFirst = true;
        for (FieldMetadata field : tableMeta.getFields()) {
            if (!isFirst) buf.append(',');

            buf.append(field.getName());

            isFirst = false;
        }
        buf.append(") VALUES ");

        String tmp = buf.toString();

        insertSqlHeadPartCache.put(tableMeta.getTableName(), tmp);
        return tmp;
    }

}
