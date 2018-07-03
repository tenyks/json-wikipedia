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
package it.cnr.isti.hpc.wikipedia.cli;

import it.cnr.isti.hpc.cli.AbstractCommandLineInterface;
import it.cnr.isti.hpc.wikipedia.article.Article;
import it.cnr.isti.hpc.wikipedia.common.DBSchema;
import it.cnr.isti.hpc.wikipedia.common.FieldMetadata;
import it.cnr.isti.hpc.wikipedia.common.TableMetadata;
import it.cnr.isti.hpc.wikipedia.reader.WikipediaArticleReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * MediawikiToJsonCLI converts a Wikipedia Dump in Sql.
 * <br/>
 * <br/>
 * <code>MediawikiToSqlCLI  wikipedia-dump.xml.bz -output wikipedia-dump.Sql[.gz] -lang [en|it] </code>
 * <br/>
 * <br/>
 * produces in wikipedia-dump.Sql the Sql version of the dump. Each line of the file contains an article
 * of dump encoded in Sql. Each Sql line can be deserialized in an Article object, which represents an
 * <b> enriched </b> version of the wikitext page. The Article object contains: 
 * 
 * <ul>
 * <li> the title (e.g., Leonardo Da Vinci);</li>
 * <li> the wikititle (used in Wikipedia as key, e.g., Leonardo_Da_Vinci);</li>
 * <li> the namespace and the integer namespace in the dump;</li>
 * <li> the timestamp of the article;</li>
 * <li> the type, if it is a standard article, a redirection, a category and so on;</li>
 * <li> if it is not in English the title of the corrispondent English Article;</li>
 * <li> a list of  tables that appear in the article ;</li>
 * <li> a list of lists that  that appear in the article ;</li>
 * <li> a list  of internal links that appear in the article;</li>
 * <li> if the article  is a redirect, the pointed article;</li>
 * <li> a list of section titles in the article;</li>
 * <li> the text of the article, divided in paragraphs;</li>
 * <li> the categories and the templates of the articles;</li>
 * <li> the list of attributes found in the templates;</li>
 * <li> a list of terms highlighted in the article;</li>
 * <li> if present the infobox.</li>
 * </ul>
 * 
 * Once you have created (or downloaded) the Sql dump (say <code>wikipedia.Sql</code>), you can iterate over the articles of the collection
 * easily using this snippet: 
 * <br/>
 * <br/>
 * <br/>
 * <pre>
 * {@code
 * RecordReader<Article> reader = new RecordReader<Article>(
 * 			"wikipedia.Sql",new SqlRecordParser<Article>(Article.class)
 * ).filter(TypeFilter.STD_FILTER);
 * 
 * for (Article a : reader) {
 * 	 // do what you want with your articles	
 * }
 * 
 * }
 * </pre>
 * <br/>
 * <br/>
 * 
 * You can also add some filters in order to iterate on only certain articles (in the example 
 * we used only the standard type filter, which excludes meta pages e.g., Portal: or User: pages. 
 * 
 * @see Article
 * @author Diego Ceccarelli, diego.ceccarelli@isti.cnr.it created on 21/nov/2011
 */
public class MediawikiToSqlCLI extends AbstractCommandLineInterface {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = LoggerFactory
			.getLogger(MediawikiToSqlCLI.class);

	private static String[] params = new String[] { INPUT, OUTPUT, "lang" };

	private static final String USAGE = "java -cp $jar "
			+ MediawikiToSqlCLI.class
			+ " -input wikipedia-dump.xml.bz -output wikipedia-dump.Sql -lang [en|it|zh]";

	public MediawikiToSqlCLI(String[] args) {
		super(args, params, USAGE);
	}

	public static void main(String[] args) {
		args = new String [] {"-input", "D:\\Share\\kb\\Wiki\\zhwiki-20180620-pages-articles-cn.xml",
				"-output", "D:\\Share\\kb\\Wiki\\zhwiki-20180620-pages-articles-cn.sql",
				"-lang", "zh"};

		MediawikiToSqlCLI cli = new MediawikiToSqlCLI(args);
		String input = cli.getInput();
		String output = cli.getOutput();
		String lang = cli.getParam("lang");
		WikipediaArticleReader wap = new WikipediaArticleReader(input, output, lang, buildDBSchema(0L));
		try {
			wap.start();
		} catch (Exception e) {
			logger.error("parsing the mediawiki {}", e.toString());
			System.exit(-1);
		}
	}

	private static DBSchema buildDBSchema(Long idOffset) {
		DBSchema	schema = new DBSchema(idOffset);
		List<FieldMetadata>	fields;

		fields = new ArrayList<>();//wiki_article
		fields.add(new FieldMetadata("id", "bigint", null));//wiki_article
		fields.add(new FieldMetadata("uuid", "varchar", null));//wiki_article
		fields.add(new FieldMetadata("name", "varchar", null));//wiki_article
		fields.add(new FieldMetadata("title", "varchar", null));//wiki_article
		fields.add(new FieldMetadata("en_title", "varchar", null));//wiki_article
		fields.add(new FieldMetadata("namespace", "varchar", null));//wiki_article
		fields.add(new FieldMetadata("lang_code", "varchar", null));//wiki_article
		fields.add(new FieldMetadata("type_code", "varchar", null));//wiki_article
		fields.add(new FieldMetadata("last_update_time", "datetime", null));//wiki_article
		fields.add(new FieldMetadata("create_time", "datetime", "NOW()"));//wiki_article
		fields.add(new FieldMetadata("state", "smallint", "1"));//wiki_article
		fields.add(new FieldMetadata("mod_time", "datetime", null));//wiki_article
		fields.add(new FieldMetadata("remark", "varchar", null));//wiki_article
		fields.add(new FieldMetadata("summary", "varchar", null));//wiki_article
		schema.addTable("wiki_article", fields, 0L);


		fields = new ArrayList<>();//wiki_article_category
		fields.add(new FieldMetadata("id", "bigint", null));//wiki_article_category
		fields.add(new FieldMetadata("article_id", "bigint", null));//wiki_article_category
		fields.add(new FieldMetadata("name", "varchar", null));//wiki_article_category
		schema.addTable("wiki_article_category", fields, 0L);


		fields = new ArrayList<>();//wiki_article_highlight
		fields.add(new FieldMetadata("id", "bigint", null));//wiki_article_highlight
		fields.add(new FieldMetadata("article_id", "bigint", null));//wiki_article_highlight
		fields.add(new FieldMetadata("name", "varchar", null));//wiki_article_highlight
		schema.addTable("wiki_article_highlight", fields, 0L);


		fields = new ArrayList<>();//wiki_article_info_template
		fields.add(new FieldMetadata("id", "bigint", null));//wiki_article_info_template
		fields.add(new FieldMetadata("article_id", "bigint", null));//wiki_article_info_template
		fields.add(new FieldMetadata("seq_no", "smallint", null));//wiki_article_info_template
		fields.add(new FieldMetadata("name", "varchar", null));//wiki_article_info_template
		schema.addTable("wiki_article_info_template", fields, 0L);


		fields = new ArrayList<>();//wiki_article_infobox
		fields.add(new FieldMetadata("id", "bigint", null));//wiki_article_infobox
		fields.add(new FieldMetadata("article_id", "bigint", null));//wiki_article_infobox
		fields.add(new FieldMetadata("seq_no", "smallint", null));//wiki_article_infobox
		fields.add(new FieldMetadata("name", "varchar", null));//wiki_article_infobox
		schema.addTable("wiki_article_infobox", fields, 0L);


		fields = new ArrayList<>();//wiki_article_inventory
		fields.add(new FieldMetadata("id", "bigint", null));//wiki_article_inventory
		fields.add(new FieldMetadata("article_id", "bigint", null));//wiki_article_inventory
		fields.add(new FieldMetadata("group_no", "smallint", null));//wiki_article_inventory
		fields.add(new FieldMetadata("seq_no", "smallint", null));//wiki_article_inventory
		fields.add(new FieldMetadata("content", "varchar", null));//wiki_article_inventory
		schema.addTable("wiki_article_inventory", fields, 0L);


		fields = new ArrayList<>();//wiki_article_link
		fields.add(new FieldMetadata("id", "bigint", null));//wiki_article_link
		fields.add(new FieldMetadata("article_id", "bigint", null));//wiki_article_link
		fields.add(new FieldMetadata("class_code", "char", null));//wiki_article_link
		fields.add(new FieldMetadata("type_code", "varchar", null));//wiki_article_link
		fields.add(new FieldMetadata("title", "varchar", null));//wiki_article_link
		fields.add(new FieldMetadata("location_coordinate", "varchar", null));//wiki_article_link
		fields.add(new FieldMetadata("uri", "varchar", null));//wiki_article_link
		schema.addTable("wiki_article_link", fields, 0L);


		fields = new ArrayList<>();//wiki_article_paragraph
		fields.add(new FieldMetadata("id", "bigint", null));//wiki_article_paragraph
		fields.add(new FieldMetadata("article_id", "bigint", null));//wiki_article_paragraph
		fields.add(new FieldMetadata("seq_no", "smallint", null));//wiki_article_paragraph
		fields.add(new FieldMetadata("section_title", "varchar", null));//wiki_article_paragraph
		fields.add(new FieldMetadata("content", "varchar", null));//wiki_article_paragraph
		schema.addTable("wiki_article_paragraph", fields, 0L);


		fields = new ArrayList<>();//wiki_article_section
		fields.add(new FieldMetadata("id", "bigint", null));//wiki_article_section
		fields.add(new FieldMetadata("article_id", "bigint", null));//wiki_article_section
		fields.add(new FieldMetadata("seq_no", "smallint", null));//wiki_article_section
		fields.add(new FieldMetadata("title", "varchar", null));//wiki_article_section
		schema.addTable("wiki_article_section", fields, 0L);


		fields = new ArrayList<>();//wiki_info_template_param
		fields.add(new FieldMetadata("id", "bigint", null));//wiki_info_template_param
		fields.add(new FieldMetadata("template_id", "bigint", null));//wiki_info_template_param
		fields.add(new FieldMetadata("name", "varchar", null));//wiki_info_template_param
		fields.add(new FieldMetadata("value", "varchar", null));//wiki_info_template_param
		schema.addTable("wiki_info_template_param", fields, 0L);


		fields = new ArrayList<>();//wiki_infobox_field
		fields.add(new FieldMetadata("id", "bigint", null));//wiki_infobox_field
		fields.add(new FieldMetadata("infobox_id", "bigint", null));//wiki_infobox_field
		fields.add(new FieldMetadata("name", "varchar", null));//wiki_infobox_field
		fields.add(new FieldMetadata("value", "varchar", null));//wiki_infobox_field
		schema.addTable("wiki_infobox_field", fields, 0L);


		return schema;
	}

}
