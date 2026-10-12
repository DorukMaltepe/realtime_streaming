package com.wikistream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/** Runs the real topology on an embedded Flink cluster against real Kafka and Postgres containers. */
@Testcontainers
class WikiPipelineIntegrationTest {

    static final String TOPIC = "wiki-changes";
    static final long T0 = Instant.parse("2026-01-01T00:00:00Z").getEpochSecond();

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.2.2");

    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17")
            // same schema file docker compose uses
            .withCopyFileToContainer(MountableFile.forHostPath("../postgres/init.sql"),
                    "/docker-entrypoint-initdb.d/init.sql");

    @Test
    void countsEditsPerWindowAndFlagsSpikes() throws Exception {
        try (var producer = new KafkaProducer<>(
                Map.<String, Object>of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
                new StringSerializer(), new StringSerializer())) {
            // six quiet minutes: 5 edits each, one of them by a bot
            for (int minute = 0; minute < 6; minute++) {
                for (int i = 0; i < 5; i++) {
                    send(producer, event("edit", "testwiki", i == 0, T0 + minute * 60 + i, "en.wikipedia.org"));
                }
            }
            // minute 6: a burst of 50 edits
            for (int i = 0; i < 50; i++) {
                send(producer, event("new", "testwiki", false, T0 + 6 * 60 + i, "en.wikipedia.org"));
            }
            // must all be ignored
            send(producer, event("edit", "testwiki", false, T0 + 1, "canary"));
            send(producer, event("log", "testwiki", false, T0 + 2, "en.wikipedia.org"));
            send(producer, "not json");
            // later event on another wiki pushes the watermark past minute 6, closing every window above
            send(producer, event("edit", "otherwiki", false, T0 + 10 * 60, "de.wikipedia.org"));
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        WikiPipeline.build(env, kafka.getBootstrapServers(), TOPIC,
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        JobClient job = env.executeAsync("wiki-pipeline-it");

        try (Connection db = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())) {
            long deadline = System.currentTimeMillis() + 120_000;
            while (rows(db, "SELECT count(*) FROM wiki_stats WHERE wiki = 'testwiki'").get(0)[0] < 7
                    || rows(db, "SELECT count(*) FROM anomalies").get(0)[0] < 1) {
                if (System.currentTimeMillis() > deadline) throw new AssertionError("pipeline produced no output in time");
                Thread.sleep(500);
            }

            List<long[]> stats = rows(db, """
                    SELECT extract(epoch FROM window_start)::bigint, edits, bot_edits
                    FROM wiki_stats WHERE wiki = 'testwiki' ORDER BY window_start""");
            for (int minute = 0; minute < 6; minute++) {
                assertArrayEquals(new long[] {T0 + minute * 60, 5, 1}, stats.get(minute), "minute " + minute);
            }
            assertArrayEquals(new long[] {T0 + 6 * 60, 50, 0}, stats.get(6));

            List<long[]> anomalies = rows(db, """
                    SELECT extract(epoch FROM window_start)::bigint, edits FROM anomalies WHERE wiki = 'testwiki'""");
            assertEquals(1, anomalies.size(), "only the burst minute is anomalous");
            assertArrayEquals(new long[] {T0 + 6 * 60, 50}, anomalies.get(0));
        } finally {
            job.cancel().get();
        }
    }

    private static void send(KafkaProducer<String, String> producer, String value) throws Exception {
        producer.send(new ProducerRecord<>(TOPIC, value)).get();
    }

    private static String event(String type, String wiki, boolean bot, long epochSeconds, String domain) {
        return """
                {"type":"%s","wiki":"%s","bot":%b,"timestamp":%d,"meta":{"domain":"%s"}}"""
                .formatted(type, wiki, bot, epochSeconds, domain);
    }

    private static List<long[]> rows(Connection db, String sql) throws Exception {
        List<long[]> out = new ArrayList<>();
        try (ResultSet rs = db.createStatement().executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                long[] row = new long[cols];
                for (int i = 0; i < cols; i++) row[i] = rs.getLong(i + 1);
                out.add(row);
            }
        }
        return out;
    }
}
