/**
 *  Copyright 2011 Diego Ceccarelli
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package it.cnr.isti.hpc.wikipedia.reader;

import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.WikiXMLParser;
import it.cnr.isti.hpc.benchmark.Stopwatch;
import it.cnr.isti.hpc.io.IOUtils;
import it.cnr.isti.hpc.log.ProgressLogger;
import it.cnr.isti.hpc.wikipedia.article.Article;
import it.cnr.isti.hpc.wikipedia.common.DBSchema;
import it.cnr.isti.hpc.wikipedia.parser.ArticleParser;
import it.cnr.isti.hpc.wikipedia.reader.converter.JsonConverter;
import it.cnr.isti.hpc.wikipedia.reader.converter.SqlConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

/**
 * A reader that converts a Wikipedia dump in its json dump. The json dump will
 * contain all the article in the XML dump, one article per line. Each line will
 * be compose by the json serialization of the object Article.
 * 
 * @see Article
 * 
 * @author Diego Ceccarelli, diego.ceccarelli@isti.cnr.it created on 18/nov/2011
 */
public class WikipediaArticleReader {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = LoggerFactory.getLogger(WikipediaArticleReader.class);

	private WikiXMLParser wxp;
	private BufferedWriter out;
    private IArticleFilter  handler;
	private ArticleParser parser;
	// private JsonRecordParser<Article> encoder;

	private static ProgressLogger pl = new ProgressLogger("parsed {} articles", 10000);
	private static Stopwatch sw = new Stopwatch();

	/**
	 * Generates a converter from the xml to json dump.
	 * 
	 * @param inputFile
	 *            - the xml file (compressed)
	 * @param outputFile
	 *            - the json output file, containing one article per line (if
	 *            the filename ends with <tt>.gz </tt> the output will be
	 *            compressed).
	 * 
	 * @param lang
	 *            - the language of the dump
	 * 
	 * 
	 */
	public WikipediaArticleReader(String inputFile, String outputFile, String lang) {
		this(new File(inputFile), new File(outputFile), lang);
	}

    public WikipediaArticleReader(String inputFile, String outputFile, String lang, DBSchema dbSchema) {
        this(new File(inputFile), new File(outputFile), lang, dbSchema);
    }

	/**
	 * Generates a converter from the xml to json dump.
	 * 
	 * @param inputFile
	 *            - the xml file (compressed)
	 * @param outputFile
	 *            - the json output file, containing one article per line (if
	 *            the filename ends with <tt>.gz </tt> the output will be
	 *            compressed).
	 * 
	 * @param lang
	 *            - the language of the dump
	 * 
	 * 
	 */
	public WikipediaArticleReader(File inputFile, File outputFile, String lang) {
		parser = new ArticleParser(lang);
        out = IOUtils.getPlainOrCompressedUTF8Writer(outputFile.getAbsolutePath());

        handler = new JsonConverter(parser, out, sw, pl);
		// encoder = new JsonRecordParser<Article>(Article.class);

		try {
			wxp = new WikiXMLParser(new File(inputFile.getAbsolutePath()), handler);
		} catch (Exception e) {
			logger.error("creating the parser {}", e.toString());
			System.exit(-1);
		}
	}

    public WikipediaArticleReader(File inputFile, File outputFile, String lang, DBSchema dbSchema) {
        parser = new ArticleParser(lang);
        out = IOUtils.getPlainOrCompressedUTF8Writer(outputFile.getAbsolutePath());

        handler = new SqlConverter(dbSchema, parser, out, sw, pl);
        // encoder = new JsonRecordParser<Article>(Article.class);

        try {
            wxp = new WikiXMLParser(new File(inputFile.getAbsolutePath()), handler);
        } catch (Exception e) {
            logger.error("creating the parser {}", e.toString());
            System.exit(-1);
        }
    }

	/**
	 * Starts the parsing
	 */
	public void start() throws IOException, SAXException {
		wxp.parse();

        if (handler instanceof SqlConverter) {
            ((SqlConverter) handler).flush();
        }

		out.close();
		logger.info(sw.stat("articles"));
	}

}
