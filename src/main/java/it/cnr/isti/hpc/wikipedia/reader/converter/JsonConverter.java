package it.cnr.isti.hpc.wikipedia.reader.converter;

import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import it.cnr.isti.hpc.benchmark.Stopwatch;
import it.cnr.isti.hpc.log.ProgressLogger;
import it.cnr.isti.hpc.wikipedia.article.Article;
import it.cnr.isti.hpc.wikipedia.parser.ArticleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * @author Maxwell.Lee
 * @date 2018-07-02 10:06
 * @since 0.0.1
 */
public class JsonConverter implements IArticleFilter {

    private static final Logger logger = LoggerFactory.getLogger(JsonConverter.class);

    private Stopwatch sw;

    private ProgressLogger pl;

    private ArticleParser parser;

    private BufferedWriter out;

    public JsonConverter(ArticleParser parser, BufferedWriter out, Stopwatch sw, ProgressLogger pl) {
        this.parser = parser;
        this.out = out;
        this.sw = sw;
        this.pl = pl;
    }

    public void process(WikiArticle page, Siteinfo si) {
        pl.up();
        sw.start("articles");
        String title = page.getTitle();
        String id = page.getId();
        String namespace = page.getNamespace();
        Integer integerNamespace = page.getIntegerNamespace();
        String timestamp = page.getTimeStamp();

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
        article.setTitle(title);
        article.setWikiId(Integer.parseInt(id));
        article.setNamespace(namespace);
        article.setIntegerNamespace(integerNamespace);
        article.setTimestamp(timestamp);
        article.setType(type);
        parser.parse(article, page.getText());

        try {
            out.write(article.toJson());
            out.write("\n");
        } catch (IOException e) {
            logger.error("writing the output file {}", e.toString());
            System.exit(-1);
        }

        sw.stop("articles");

        return ;
    }
}
