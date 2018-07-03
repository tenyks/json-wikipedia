package it.cnr.isti.hpc.wikipedia.reader.converter;

import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import it.cnr.isti.hpc.benchmark.Stopwatch;
import it.cnr.isti.hpc.log.ProgressLogger;
import it.cnr.isti.hpc.wikipedia.article.Article;
import it.cnr.isti.hpc.wikipedia.article.Link;
import it.cnr.isti.hpc.wikipedia.article.ParagraphLite;
import it.cnr.isti.hpc.wikipedia.article.Template;
import it.cnr.isti.hpc.wikipedia.common.DBSchema;
import it.cnr.isti.hpc.wikipedia.common.DataRecord;
import it.cnr.isti.hpc.wikipedia.common.InsertSql;
import it.cnr.isti.hpc.wikipedia.parser.ArticleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

/**
 * @author Maxwell.Lee
 * @date 2018-07-02 10:16
 * @since   0.0.1
 */
public class SqlConverter implements IArticleFilter {

    private static final Logger logger = LoggerFactory.getLogger(SqlConverter.class);

    private static final int BatchSize = 100;

    private Stopwatch sw;

    private ProgressLogger pl;

    private ArticleParser parser;

    private BufferedWriter out;

    private DBSchema dbSchema;

    private List<InsertSql>  sqlCache;

    private int count = 0;

    public SqlConverter(DBSchema dbSchema, ArticleParser parser, BufferedWriter out,
                        Stopwatch sw, ProgressLogger pl) {
        this.parser = parser;
        this.out = out;
        this.sw = sw;
        this.pl = pl;
        this.dbSchema = dbSchema;

        this.sqlCache = new ArrayList<>();
    }

    public void process(WikiArticle page, Siteinfo si) {
        pl.up();
        sw.start("articles");

        Article.Type type = Article.Type.UNKNOWN;
        if (page.isTemplate()) {
            type = Article.Type.TEMPLATE;
            // FIXME just to go fast;
            sw.stop("articles");
            return;
        }
        if (page.isProject()) {
            type = Article.Type.PROJECT;
            // FIXME just to go fast;
            sw.stop("articles");
            return;
        }
        if (page.isFile()) {
            type = Article.Type.FILE;
            // FIXME just to go fast;
            sw.stop("articles");
            return;
        }

        if (page.isCategory()) {
            type = Article.Type.CATEGORY;
        }
        if (page.isMain()) {
            type = Article.Type.ARTICLE;
        }

        Article article = new Article();
        article.setTitle(page.getTitle());
        article.setWikiId(Integer.parseInt(page.getId()));
        article.setNamespace(page.getNamespace());
        article.setIntegerNamespace(page.getIntegerNamespace());
        article.setTimestamp(page.getTimeStamp());
        article.setType(type);

        parser.parse(article, page.getText());
        List<InsertSql> sqls = toSql(article);
        sqlCache.addAll(sqls);

        count ++;
        if (count >= BatchSize) {
            flush();
        }

        sw.stop("articles");

        return ;
    }

    public void flush() {
        writeOutAsBatchInsertSql(out, sqlCache);

        sqlCache.clear();
        count = 0;
    }

    private static void writeOutAsBatchInsertSql(BufferedWriter out, List<InsertSql> sqls) {
        Map<String, List<InsertSql>> batchGroups = new HashMap<>();

        for (InsertSql sql : sqls) {
            List<InsertSql> grp = batchGroups.computeIfAbsent(sql.getBatchKey(), k -> new ArrayList<>());
            grp.add(sql);
        }

        try {
            for (String key : batchGroups.keySet()) {
                List<InsertSql> grp = batchGroups.get(key);

                if (grp.isEmpty()) continue;

                String headPart = grp.get(0).getHeadPart();
                out.write(headPart);

                boolean isFirst = true;
                for (InsertSql sql : grp) {
                    if (!isFirst) out.write("\n,");
                    isFirst = false;
                    out.write(sql.getTailPart());
                }

                out.write(";\n");
            }
        } catch (IOException e) {
            logger.error("writing the output file {}", e.toString());
            System.exit(-1);
        }
    }

    private List<InsertSql> toSql(Article article) {
        List<InsertSql> rst = new ArrayList<>();

        InsertSql mp = buildSqlOfMainPart(article);
        rst.add(mp);

        List<InsertSql> tmp;

        tmp = buildSqlOfHighlight(mp.getId(), article);
        if (tmp != null) rst.addAll(tmp);

        tmp = buildSqlOfLink(mp.getId(), article);
        if (tmp != null) rst.addAll(tmp);

        tmp = buildSqlOfInventory(mp.getId(), article);
        if (tmp != null) rst.addAll(tmp);

        tmp = buildSqlOfSection(mp.getId(), article);
        if (tmp != null) rst.addAll(tmp);

        tmp = buildSqlOfParagraph(mp.getId(), article);
        if (tmp != null) rst.addAll(tmp);

        tmp = buildSqlOfCategory(mp.getId(), article);
        if (tmp != null) rst.addAll(tmp);

        tmp = buildSqlOfInfobox(mp.getId(), article);
        if (tmp != null) rst.addAll(tmp);

        tmp = buildSqlOfTemplate(mp.getId(), article);
        if (tmp != null) rst.addAll(tmp);

        return rst;
    }

    private InsertSql buildSqlOfMainPart(Article article) {
        DataRecord record = new DataRecord();
        record.setValue("uuid", article.getWid() + "-WIKI-ID");
        record.setValue("name", article.getTitle());
        record.setValue("title", article.getTitle());
        record.setValue("en_title", article.getEnWikiTitle());
        record.setValue("namespace", article.getNamespace());
        record.setValue("lang_code", article.getLang());
        record.setValue("type_code", article.getType());
        record.setValue("last_update_time", normalizeDateTime(article.getTimestamp()));

        return dbSchema.buildInsertSql(DBSchema.TB_MP, record);
    }

    private static String normalizeDateTime(String str) {
        return str.substring(0, 10) + " " + str.substring(11, 19);
    }

    private List<InsertSql> buildSqlOfHighlight(Long mpId, Article article) {
        List<String>    tags = article.getHighlights();

        if (tags == null || tags.isEmpty()) return null;

        List<InsertSql> rst = new ArrayList<>();
        for (String tag : tags) {
            tag = tag.trim();
            if (tag.length() == 0) continue;

            DataRecord record = new DataRecord();
            record.setValue("article_id", mpId);
            record.setValue("name", tag);

            rst.add(dbSchema.buildInsertSql(DBSchema.TB_HIGH_LIGHT, record));
        }

        return rst;
    }

    private List<InsertSql> buildSqlOfLink(Long mpId, Article article) {
        List<InsertSql> rst = new ArrayList<>();

        List<Link>    links = article.getLinks();
        if (links != null && links.size() > 0) {
            for (Link link : links) {
                rst.add(buildSqlOfLink(mpId, "INTL", link));
            }
        }

        links = article.getExternalLinks();
        if (links != null && links.size() > 0) {
            for (Link link : links) {
                rst.add(buildSqlOfLink(mpId, "EXTL", link));
            }
        }

        return rst;
    }

    private InsertSql buildSqlOfLink(Long mpId, String classCode, Link link) {
        DataRecord record = new DataRecord();
        record.setValue("article_id", mpId);
        record.setValue("class_code", classCode);
        record.setValue("type_code", link.getType());
        record.setValue("title", link.getAnchor());
        record.setValue("uri", link.getId());

        String lc;
        if (link.getTableId() != null) {
            lc = String.format("TABLE:%d:%d:%d:%d", link.getTableId(), link.getRowId(), link.getColumnId(), link.getStart());
        } else if (link.getListId() != null) {
            lc = String.format("LIST:%d:%d:%d", link.getListId(), link.getListItem(), link.getStart());
        } else if (link.getParagraphId() != null) {
            lc = String.format("PG:%d:%d", link.getParagraphId(), link.getStart());
        } else {
            lc = String.format("POS:%d", link.getStart());
        }
        record.setValue("location_coordinate", lc);

        return dbSchema.buildInsertSql(DBSchema.TB_LINK, record);
    }

    private List<InsertSql> buildSqlOfInventory(Long mpId, Article article) {
        List<List<String>>    groups = article.getLists();

        if (groups == null || groups.isEmpty()) return null;

        List<InsertSql> rst = new ArrayList<>();
        int grpNo = 0;
        for (List<String> group : groups) {
            if (group.isEmpty()) continue;

            int seqNo = 0;
            for (String item : group) {
                item = item.trim();
                if (item.isEmpty()) continue;

                DataRecord record = new DataRecord();
                record.setValue("article_id", mpId);
                record.setValue("group_no", grpNo);
                record.setValue("seq_no", seqNo++);
                record.setValue("content", item);

                rst.add(dbSchema.buildInsertSql(DBSchema.TB_INVENTORY, record));
            }

            grpNo++;
        }

        return rst;
    }

    private List<InsertSql> buildSqlOfSection(Long mpId, Article article) {
        List<String>    titles = article.getSections();

        if (titles == null || titles.isEmpty()) return null;

        List<InsertSql> rst = new ArrayList<>();
        int seqNo = 0;
        for (String title : titles) {
            title = title.trim();
            if (title.length() == 0) continue;

            DataRecord record = new DataRecord();
            record.setValue("article_id", mpId);
            record.setValue("seq_no", seqNo++);
            record.setValue("title", title);

            rst.add(dbSchema.buildInsertSql(DBSchema.TB_SECTION, record));
        }

        return rst;
    }

    private List<InsertSql> buildSqlOfParagraph(Long mpId, Article article) {
        List<ParagraphLite>    paragraphs = article.getParagraphLites();

        if (paragraphs == null || paragraphs.isEmpty()) return null;

        List<InsertSql> rst = new ArrayList<>();
        int seqNo = 0;
        for (ParagraphLite p : paragraphs) {

            DataRecord record = new DataRecord();
            record.setValue("article_id", mpId);
            record.setValue("seq_no", seqNo++);
            record.setValue("section_title", p.getSectionTitle());
            record.setValue("content", p.getText());

            rst.add(dbSchema.buildInsertSql(DBSchema.TB_PARAGRAPH, record));
        }

        return rst;
    }

    private List<InsertSql> buildSqlOfCategory(Long mpId, Article article) {
        List<Link>    categories = article.getCategories();

        if (categories == null || categories.isEmpty()) return null;

        List<InsertSql> rst = new ArrayList<>();
        for (Link cat : categories) {
            DataRecord record = new DataRecord();
            record.setValue("article_id", mpId);
            record.setValue("name", cat.getId());

            rst.add(dbSchema.buildInsertSql(DBSchema.TB_CATEGORY, record));
        }

        return rst;
    }

    private List<InsertSql> buildSqlOfInfobox(Long mpId, Article article) {
        Template infobox = article.getInfobox();

        if (infobox == null || infobox.getDescription() == null || infobox.getDescription().isEmpty()) return null;

        List<InsertSql> rst = new ArrayList<>();

        DataRecord record = new DataRecord();
        record.setValue("article_id", mpId);
        record.setValue("seq_no", 0);
        record.setValue("name", infobox.getName());

        InsertSql mp = dbSchema.buildInsertSql(DBSchema.TB_INFOBOX, record);
        rst.add(mp);

        Map<String, String> fields = infobox.getAsMap();
        if (fields != null && fields.size() > 0) {
            for (String key : fields.keySet()) {
                DataRecord r = new DataRecord();
                r.setValue("infobox_id", mpId);
                r.setValue("name", key);
                r.setValue("value", fields.get(key));

                rst.add(dbSchema.buildInsertSql(DBSchema.TB_IB_FIELD, r));
            }
        }

        return rst;
    }

    private List<InsertSql> buildSqlOfTemplate(Long mpId, Article article) {
        List<Template>  templates = article.getTemplates();

        if (templates == null || templates.isEmpty()) return null;

        List<InsertSql> rst = new ArrayList<>();

        int seqNo = 0;
        for (Template tpl : templates) {
            DataRecord record = new DataRecord();
            record.setValue("article_id", mpId);
            record.setValue("seq_no", seqNo++);
            record.setValue("name", tpl.getName());

            InsertSql mp = dbSchema.buildInsertSql(DBSchema.TB_INFO_TPL, record);
            rst.add(mp);

            Map<String, String> fields = tpl.getAsMap();
            if (fields != null && fields.size() > 0) {
                for (String key : fields.keySet()) {
                    if (key.startsWith("Harwood, Dane (1976).")) {
                        logger.info("===============");
                    }

                    DataRecord r = new DataRecord();
                    r.setValue("template_id", mpId);

                    if (key.length() < 128) {
                        r.setValue("name", key);
                        r.setValue("value", fields.get(key));
                    } else {
                        r.setValue("name", key.substring(0, 128));
                        r.setValue("value", key + fields.get(key));
                    }

                    rst.add(dbSchema.buildInsertSql(DBSchema.TB_INFO_TPL_PARAM, r));
                }
            }
        }

        return rst;
    }
}
