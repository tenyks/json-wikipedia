package it.cnr.isti.hpc.wikipedia.common;

/**
 * @author Maxwell.Lee
 * @date 2018-07-02 13:39
 * @since   0.0.1
 */
public class InsertSql {

    private String  batchKey;

    private Long    id;

    private String  headPart;

    private String  tailPart;

    public InsertSql(String batchKey, Long id, String headPart, String tailPart) {
        this.batchKey = batchKey;
        this.id = id;
        this.headPart = headPart;
        this.tailPart = tailPart;
    }

    public String getBatchKey() {
        return batchKey;
    }

    public Long getId() {
        return id;
    }

    public String getHeadPart() {
        return headPart;
    }

    public String getTailPart() {
        return tailPart;
    }

    @Override
    public String toString() {
        return "InsertSql{" +
                "batchKey='" + batchKey + '\'' +
                ", id=" + id +
                ", headPart='" + headPart + '\'' +
                ", tailPart='" + tailPart + '\'' +
                '}';
    }
}
