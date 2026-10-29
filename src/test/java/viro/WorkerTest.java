package viro;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class WorkerTest {
    @TempDir Path directory;
    private ConsumerRecord<String,String> record(long offset, String activity) {
        return new ConsumerRecord<>("observations", 0, offset, "site-key", "{\"state_territory\":\"California\",\"site\":\"A\",\"pathogen_target\":\"SARS-CoV-2\",\"week_end\":\"2026-01-03\",\"site_wval_category\":\"" + activity + "\",\"site_wval\":null}");
    }
    @Test void replayIsIdempotentAndOffsetSurvivesRestart() throws Exception {
        try (Worker worker = new Worker(directory, 0)) {
            worker.ingest(List.of(record(0, "High")));
            worker.ingest(List.of(record(0, "High")));
            assertEquals(1L, worker.query(Map.of()).get("total"));
            assertEquals(1L, worker.storedOffset());
        }
        try (Worker worker = new Worker(directory, 0)) {
            assertEquals(1L, worker.storedOffset());
            assertEquals(1L, worker.query(Map.of("state", "California", "week_from", "2026-01-01")).get("total"));
            assertEquals(0L, worker.query(Map.of("state", "Oregon")).get("total"));
        }
    }
    @Test void invalidBatchRollsBackBothRowsAndOffset() throws Exception {
        try (Worker worker = new Worker(directory, 0)) {
            ConsumerRecord<String,String> invalid = new ConsumerRecord<>("observations", 0, 1, "bad", "{}");
            assertThrows(IllegalArgumentException.class, () -> worker.ingest(List.of(record(0, "High"), invalid)));
            assertEquals(0L, worker.query(Map.of()).get("total"));
            assertEquals(-1L, worker.storedOffset());
        }
    }
    @Test void parquetRetainsCompletedSnapshotAndSurvivesRestart() throws Exception {
        try (Worker worker = new Worker(directory, 0)) {
            worker.ingest(List.of(record(0, "High")));
            worker.flush();
            worker.ingest(List.of(record(1, "Low")));
            assertEquals(1L, worker.query(Map.of("mode", "parquet", "activity", "High")).get("total"));
            assertEquals(0L, worker.query(Map.of("activity", "High")).get("total"));
        }
        try (Worker worker = new Worker(directory, 0)) {
            assertEquals(1L, worker.query(Map.of("mode", "parquet")).get("total"));
            worker.flush();
            assertEquals(worker.query(Map.of("activity", "Low")).get("rows"), worker.query(Map.of("mode", "parquet", "activity", "Low")).get("rows"));
        }
    }
    @Test void filtersRejectUnknownSqlAndBadDates() {
        assertThrows(IllegalArgumentException.class, () -> Main.filters("sql=DROP+TABLE+observations"));
        assertThrows(Exception.class, () -> Main.filters("week_from=not-a-date"));
        assertThrows(IllegalArgumentException.class, () -> Main.filters("mode=/tmp/foreign.parquet"));
        assertEquals("California", Main.filters("state=California").get("state"));
    }
    @Test void gatewaySumsGroupsAcrossEveryPartition() throws Exception {
        var a = Main.JSON.readTree("{\"rows\":[{\"state\":\"CA\",\"pathogen\":\"COVID\",\"activity\":\"High\",\"count\":2}]}");
        var b = Main.JSON.readTree("{\"rows\":[{\"state\":\"CA\",\"pathogen\":\"COVID\",\"activity\":\"High\",\"count\":3}]}");
        Map<String,Object> result = Gateway.merge(List.of(a,b), "database");
        assertEquals(5L, result.get("total"));
        assertEquals(1, ((List<?>) result.get("rows")).size());
        assertEquals(2, result.get("workers"));
    }
}
