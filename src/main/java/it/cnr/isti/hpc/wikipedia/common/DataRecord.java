package it.cnr.isti.hpc.wikipedia.common;

import java.util.HashMap;
import java.util.Map;

/**
 * 数据记录；
 * @author Maxwell.Lee
 * @date 2018-07-02 10:54
 * @since   0.0.1
 */
public class DataRecord {

    private Map<String, Object> values;

    public DataRecord() {
        this.values = new HashMap<>();
    }

    public String getValue(String name) {
        Object val = values.get(name);

        return (val != null ? val.toString() : null);
    }

    public <T> void setValue(String name, T value) {
        this.values.put(name, value);
    }
}
