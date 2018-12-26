package com.yss.flink.datastream.datasources;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 手动实现flink数据源
 * 需要实现SourceFunction<>接口
 * 另外两个接口CheckpointedFunction，StoppableFunction可选
 * CheckpointedFunction可以实现输入流中的水印和数据的timestamp
 * StoppableFunction可以实现优雅地关闭source
 */

public class MySource implements SourceFunction<Long>, CheckpointedFunction, StoppableFunction {

    private boolean isRunning = true;
    private ListState<Long> checkpointedCount;
    private Long count = 0l;

    public void stop() {

    }

    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        this.checkpointedCount.clear();
        this.checkpointedCount.add(count);
    }

    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        this.checkpointedCount = ctx.getOperatorStateStore()
                .getListState(new ListStateDescriptor<Long>("count", Long.class));
        if (ctx.isRestored()){
            for (Long count: this.checkpointedCount.get()) {
                this.count = count;
            }
        }
    }

    public void run(SourceContext<Long> sourceContext) throws Exception {
        if (isRunning){
            synchronized (sourceContext.getClass()){
                for (int i = 0; i < 10000*10000; i++) {
                    sourceContext.collect(i+0l);
                }
            }
        }
    }

    public void cancel() {
        this.isRunning = false;
    }
}
