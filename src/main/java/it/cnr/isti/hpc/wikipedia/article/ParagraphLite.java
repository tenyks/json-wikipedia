package it.cnr.isti.hpc.wikipedia.article;

/**
 * 段落
 * @author Maxwell.Lee
 * @date 2018-06-29 19:05
 * @since   1.0.0
 */
public class ParagraphLite {

    private String  sectionTitle;

    private String  text;

    public ParagraphLite() {

    }

    public ParagraphLite(String sectionTitle, String text) {
        this.sectionTitle = sectionTitle;
        this.text = text;
    }

    public String getSectionTitle() {
        return sectionTitle;
    }

    public void setSectionTitle(String sectionTitle) {
        this.sectionTitle = sectionTitle;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return "ParagraphLite{" +
                "sectionTitle='" + sectionTitle + '\'' +
                ", text='" + text + '\'' +
                '}';
    }
}
