package it.cnr.isti.hpc.wikipedia.common;

/**
 * 字段元数据；
 * @author Maxwell.Lee
 * @date 2018-07-02 10:48
 * @since   0.0.1
 */
public class FieldMetadata {

    private String      name;

    private String dataType;

    private String      defaultValue;

    public FieldMetadata(String name, String dataType, String defaultValue) {
        this.name = name;
        this.dataType = dataType.toLowerCase();
        this.defaultValue = defaultValue;
    }

    public boolean isIdKey() {
        return (name.equals("id"));
    }

    public String getName() {
        return name;
    }

    public String getDataType() {
        return dataType;
    }

    public boolean isValueOfNumberType() {
        return (dataType.equals("bigint") || dataType.equals("smallint") || dataType.equals("int") || dataType.equals("decimal") ||
                dataType.equals("tinyint") || dataType.equals("float") || dataType.equals("double") || dataType.equals("integer"));
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String toString() {
        return "FieldMetadata{" +
                "name='" + name + '\'' +
                ", dataType=" + dataType +
                ", defaultValue=" + defaultValue +
                '}';
    }
}
