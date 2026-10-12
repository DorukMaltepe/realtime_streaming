package com.wikistream;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flags windows whose edit count is a statistical outlier for that wiki.
 *
 * <p>Keeps an exponentially weighted moving mean and variance per wiki in keyed state, so the
 * baseline adapts to each wiki's normal activity (enwiki vs. a small wiki) without storing history.
 */
public class SpikeDetector extends KeyedProcessFunction<String, WindowStats, Anomaly> {

    static final double ALPHA = 0.2;          // weight of the newest window in the baseline
    static final double Z_THRESHOLD = 3.0;
    static final int WARMUP_WINDOWS = 5;      // no alerts until the baseline has seen this many windows
    static final long MIN_EDITS = 20;         // ignore "spikes" on tiny wikis (2 -> 6 edits)

    // {windowsSeen, mean, variance}
    private transient ValueState<double[]> baseline;

    @Override
    public void open(OpenContext ctx) {
        baseline = getRuntimeContext().getState(
                new ValueStateDescriptor<>("baseline", PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));
    }

    // shortcut: windows with zero edits are never emitted, so quiet minutes don't pull the mean down;
    // fine for busy wikis, upgrade to timer-driven zero windows if small-wiki accuracy matters.
    @Override
    public void processElement(WindowStats s, Context ctx, Collector<Anomaly> out) throws Exception {
        double[] b = baseline.value();
        if (b == null) {
            baseline.update(new double[] {1, s.edits(), 0});
            return;
        }

        double std = Math.max(Math.sqrt(b[2]), 1.0); // floor avoids divide-by-zero on flat baselines
        double z = (s.edits() - b[1]) / std;
        if (b[0] >= WARMUP_WINDOWS && s.edits() >= MIN_EDITS && z >= Z_THRESHOLD) {
            out.collect(new Anomaly(s.windowStartMs(), s.wiki(), s.edits(), b[1], z));
        }

        double diff = s.edits() - b[1];
        double incr = ALPHA * diff;
        b[0] += 1;
        b[1] += incr;
        b[2] = (1 - ALPHA) * (b[2] + diff * incr);
        baseline.update(b);
    }
}
