package com.yss.flink.table.sorceFactory;

import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyStreamSorceFactory implements StreamTableSourceFactory<Row> {

    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        Boolean flag = Boolean.valueOf(properties.get("connector.debug"));

        return new MySystemAppendTableSource(flag);
    }

    public Map<String, String> requiredContext() {
        Map<String,String> context = new HashMap<String, String>();
        context.put("update-mode","append");
        context.put("connector.type","my-system");
        return context;
    }

    public List<String> supportedProperties() {
        List<String> list = new ArrayList<String>();
        list.add("connector.debug");
        return list;
    }
}
