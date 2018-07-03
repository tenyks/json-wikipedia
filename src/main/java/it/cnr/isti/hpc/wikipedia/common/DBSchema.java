package it.cnr.isti.hpc.wikipedia.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Maxwell.Lee
 * @date 2018-07-02 14:19
 * @since   1.0.0
 */
public class DBSchema {

    public static final String TB_MP = "wiki_article";
    public static final String TB_HIGH_LIGHT = "wiki_article_highlight";
    public static final String TB_LINK = "wiki_article_link";
    public static final String TB_INVENTORY = "wiki_article_inventory";
    public static final String TB_SECTION = "wiki_article_section";
    public static final String TB_PARAGRAPH = "wiki_article_paragraph";
    public static final String TB_CATEGORY = "wiki_article_category";
    public static final String TB_INFOBOX = "wiki_article_infobox";
    public static final String TB_IB_FIELD = "wiki_infobox_field";
    public static final String TB_INFO_TPL = "wiki_article_info_template";
    public static final String TB_INFO_TPL_PARAM = "wiki_info_template_param";

    private Long    idOffset;

    private Map<String, TableMetadata>  tableMetaCache;

    private SqlBuilder  sqlBuilder;

    public DBSchema(long idOffset) {
        this.idOffset = idOffset;
        this.tableMetaCache = new HashMap<>();
        this.sqlBuilder = new SqlBuilder();
    }

    public TableMetadata addTable(String tableName, List<FieldMetadata> fields, Long maxId) {
        TableMetadata rst = new TableMetadata(tableName, fields, maxId + idOffset);

        tableMetaCache.put(tableName, rst);
        return rst;
    }

    public TableMetadata getTableMeta(String tableName) {
        return tableMetaCache.get(tableName);
    }

    public InsertSql buildInsertSql(String tableName, DataRecord record) {
        TableMetadata tm = tableMetaCache.get(tableName);
        if (tm == null) {
            throw new IllegalArgumentException("无效的参数。[0x09DBS4065]");
        }
        return sqlBuilder.buildInsertSql(tm, record);
    }

}
