package com.yss.flink.table.sorceFactory;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

public class MySystemAppendTableSource implements StreamTableSource<Row> {

    @Setter
    @Getter
    private boolean isDebug;

    public MySystemAppendTableSource(Boolean flag) {
        this.isDebug = flag;
    }

    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(new SourceFunction<Row>() {
            public void run(SourceContext<Row> ctx) throws Exception {
                ctx.collect(null);
            }

            public void cancel() {
                //do nothing
            }
        });
    }

    public TypeInformation<Row> getReturnType() {
        return null;
    }

    public TableSchema getTableSchema() {
        return null;
    }

    public String explainSource() {
        return null;
    }
}
