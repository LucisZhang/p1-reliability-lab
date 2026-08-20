package com.p1.reliability.cdc;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Emits only the latest CDC change for a key after a replay backlog becomes quiet. */
public final class LatestPerKeyReplayCoalescer
    extends KeyedProcessFunction<Long, OrderChange, OrderChange> {
  private static final long serialVersionUID = 1L;

  private final long quietPeriodMs;
  private transient ValueState<OrderChange> pendingChange;
  private transient ValueState<Long> pendingTimer;

  public LatestPerKeyReplayCoalescer(long quietPeriodMs) {
    if (quietPeriodMs <= 0L) {
      throw new IllegalArgumentException("quietPeriodMs must be positive");
    }
    this.quietPeriodMs = quietPeriodMs;
  }

  @Override
  public void open(OpenContext openContext) {
    pendingChange =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("replay-pending-change", OrderChange.class));
    pendingTimer =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("replay-pending-timer", Long.class));
  }

  @Override
  public void processElement(OrderChange change, Context context, Collector<OrderChange> out)
      throws Exception {
    Long previousTimer = pendingTimer.value();
    if (previousTimer != null) {
      context.timerService().deleteProcessingTimeTimer(previousTimer);
    }
    pendingChange.update(change);
    long nextTimer = context.timerService().currentProcessingTime() + quietPeriodMs;
    pendingTimer.update(nextTimer);
    context.timerService().registerProcessingTimeTimer(nextTimer);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext context, Collector<OrderChange> out)
      throws Exception {
    Long expectedTimer = pendingTimer.value();
    if (expectedTimer == null || expectedTimer.longValue() != timestamp) {
      return;
    }
    OrderChange latest = pendingChange.value();
    if (latest != null) {
      out.collect(latest);
    }
    pendingChange.clear();
    pendingTimer.clear();
  }
}
