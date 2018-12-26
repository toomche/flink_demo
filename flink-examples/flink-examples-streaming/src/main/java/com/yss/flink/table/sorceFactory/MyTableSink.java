package com.yss.flink.table.sorceFactory;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

public class MyTableSink implements UpsertStreamTableSink<Row> {

    public void setKeyFields(String[] keys) {

    }

    public void setIsAppendOnly(Boolean isAppendOnly) {

    }

    public TypeInformation<Row> getRecordType() {
        return null;
    }

    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {

    }

    public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
        return null;
    }

    public String[] getFieldNames() {
        return new String[0];
    }

    public TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation[0];
    }

    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return null;
    }
}
