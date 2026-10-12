package com.wikistream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Wikipedia recent changes (Kafka) -> per-wiki 1-minute edit counts + spike alerts (Postgres).
 */
public class WikiPipeline {

    static final Duration WINDOW = Duration.ofMinutes(1);
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000); // recovers state + offsets consistently after task failures
        build(env,
                env("KAFKA_BOOTSTRAP", "kafka:9092"),
                env("KAFKA_TOPIC", "wiki-changes"),
                env("JDBC_URL", "jdbc:postgresql://postgres:5432/wiki"),
                env("JDBC_USER", "wiki"),
                env("JDBC_PASSWORD", "wiki"));
        env.execute("wikipedia-edit-pipeline");
    }

    /** Wires the whole topology onto {@code env}; shared by {@link #main} and the integration test. */
    public static void build(StreamExecutionEnvironment env, String bootstrap, String topic,
                             String jdbcUrl, String jdbcUser, String jdbcPassword) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("wiki-pipeline")
                // A fresh job has no window state, so resuming from committed offsets would overwrite
                // half-finished windows with partial counts. Replaying the retained topic rebuilds windows
                // and detector baselines exactly; the upsert sinks make the replay idempotent.
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<WikiEdit> edits = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "wikimedia-recentchange")
                .flatMap((String json, Collector<WikiEdit> out) -> {
                    WikiEdit e = parse(json);
                    if (e != null) out.collect(e);
                })
                .returns(WikiEdit.class)
                .name("parse")
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WikiEdit>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((e, ts) -> e.timestampMs())
                        .withIdleness(Duration.ofMinutes(1)));

        DataStream<WindowStats> stats = edits
                .keyBy(WikiEdit::wiki, Types.STRING)
                .window(TumblingEventTimeWindows.of(WINDOW))
                .aggregate(new CountEdits(), new EmitWindow())
                .name("1-min edit counts");

        DataStream<Anomaly> anomalies = stats
                .keyBy(WindowStats::wiki, Types.STRING)
                .process(new SpikeDetector())
                .name("spike detector");

        JdbcConnectionOptions db = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(jdbcUser)
                .withPassword(jdbcPassword)
                .build();

        // Upserts keep the sinks idempotent, so replays after a restart don't double count.
        stats.sinkTo(jdbcSink(db, """
                INSERT INTO wiki_stats (window_start, wiki, edits, bot_edits) VALUES (?, ?, ?, ?)
                ON CONFLICT (window_start, wiki)
                DO UPDATE SET edits = EXCLUDED.edits, bot_edits = EXCLUDED.bot_edits""",
                (JdbcStatementBuilder<WindowStats>) (ps, s) -> {
                    ps.setObject(1, utc(s.windowStartMs()));
                    ps.setString(2, s.wiki());
                    ps.setLong(3, s.edits());
                    ps.setLong(4, s.botEdits());
                })).name("postgres: wiki_stats");

        anomalies.sinkTo(jdbcSink(db, """
                INSERT INTO anomalies (window_start, wiki, edits, expected, z_score) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (window_start, wiki)
                DO UPDATE SET edits = EXCLUDED.edits, expected = EXCLUDED.expected, z_score = EXCLUDED.z_score""",
                (JdbcStatementBuilder<Anomaly>) (ps, a) -> {
                    ps.setObject(1, utc(a.windowStartMs()));
                    ps.setString(2, a.wiki());
                    ps.setLong(3, a.edits());
                    ps.setDouble(4, a.expected());
                    ps.setDouble(5, a.zScore());
                })).name("postgres: anomalies");
    }

    /** Returns the edit, or null for events we don't count (logs, categorize, canaries, malformed). */
    static WikiEdit parse(String json) {
        try {
            JsonNode n = JSON.readTree(json);
            String type = n.path("type").asText();
            String wiki = n.path("wiki").asText();
            long ts = n.path("timestamp").asLong();
            boolean isEdit = type.equals("edit") || type.equals("new");
            boolean isCanary = n.path("meta").path("domain").asText().equals("canary");
            if (!isEdit || isCanary || wiki.isEmpty() || ts <= 0) return null;
            return new WikiEdit(wiki, n.path("bot").asBoolean(), ts * 1000);
        } catch (Exception malformed) {
            return null;
        }
    }

    private static <T> JdbcSink<T> jdbcSink(JdbcConnectionOptions db, String sql, JdbcStatementBuilder<T> bind) {
        return JdbcSink.<T>builder()
                .withQueryStatement(sql, bind)
                .withExecutionOptions(JdbcExecutionOptions.builder().withBatchIntervalMs(1000).build())
                .buildAtLeastOnce(db);
    }

    private static OffsetDateTime utc(long epochMs) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMs), ZoneOffset.UTC);
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? fallback : v;
    }

    /** Accumulator: {edits, botEdits}. */
    static class CountEdits implements AggregateFunction<WikiEdit, long[], long[]> {
        public long[] createAccumulator() { return new long[2]; }
        public long[] add(WikiEdit e, long[] acc) { acc[0]++; if (e.bot()) acc[1]++; return acc; }
        public long[] getResult(long[] acc) { return acc; }
        public long[] merge(long[] a, long[] b) { return new long[] {a[0] + b[0], a[1] + b[1]}; }
    }

    static class EmitWindow extends ProcessWindowFunction<long[], WindowStats, String, TimeWindow> {
        @Override
        public void process(String wiki, Context ctx, Iterable<long[]> counts, Collector<WindowStats> out) {
            long[] c = counts.iterator().next();
            out.collect(new WindowStats(ctx.window().getStart(), wiki, c[0], c[1]));
        }
    }
}
