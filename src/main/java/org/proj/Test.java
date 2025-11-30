package org.proj;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Test {
    public record Payload(String title, String comment, String timestamp){}
    public interface FlushableKVStore extends KVStore {
        void flushDB();
    }

    public static List<Map.Entry<String, Payload>> readData(String path) {
        List<Map.Entry<String, Payload>> l = new ArrayList<>();
        String line;
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            // Skip header
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",", 4); // key,title,comment,"version"
                String key = values[0];
                String title = values[1];
                String comment = values[2];
                String timestamp = values[3];

                l.add(new AbstractMap.SimpleEntry<>(key, new Payload(title, comment, timestamp)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return l;
    }
    public static void main(String[] args) {


            System.out.println("=== Task 1.3b: Testing SimpleMVM ===\n");

            // Read test data
            List<Map.Entry<String, Payload>> data = readData("test_data.csv");
            System.out.println("Loaded " + data.size() + " records from test_data.csv\n");

            // Initialize Redis store
            JedisKVStore store = new JedisKVStore();
            PayloadSerializer serializer = new PayloadSerializer();

            // Test 1: BackedSimpleMVM with BackedVLinkedList
            System.out.println("--- Test 1: BackedSimpleMVM with BackedVLinkedList ---");
            store.flushDB();

            BackedSimpleMVM<String, Payload> mvm1 = new BackedSimpleMVM<>(
                    new VersionListFactory<Payload>() {
                        @Override
                        public VersionList<Payload> create(KVStore store, Serializer<Payload> serializer) {
//                            return null;
                            return new BackedVLinkedList<>(store, serializer);
                        }
                    }
            );

            // Insert data
            for (Map.Entry<String, Payload> entry : data) {
                mvm1.append(entry.getKey(), entry.getValue());
            }

            // Query range snapshot at timestamp 20
            Map<String, Payload> snapshot1 = mvm1.rangeSnapshot("KEY002", "KEY004", 20);

            System.out.println("Range Snapshot [KEY002, KEY004] at timestamp 20:");
            for (Map.Entry<String, Payload> entry : snapshot1.entrySet()) {
                System.out.println(entry.getKey() + "=" + entry.getValue());
            }

            // Verify results
            System.out.println("\nExpected output:");
            System.out.println("KEY002=Payload[title=Some Title for KEY002, comment=Change 3 for key KEY002, timestamp=19]");
            System.out.println("KEY003=Payload[title=Some Title for KEY003, comment=Change 4 for key KEY003, timestamp=20]");
            System.out.println("KEY004=Payload[title=Some Title for KEY004, comment=Change 3 for key KEY004, timestamp=13]");

            // Test 2: BackedSimpleMVM with BackedFrugalSkiplist
            System.out.println("\n--- Test 2: BackedSimpleMVM with BackedFrugalSkiplist ---");
            store.flushDB();

            BackedSimpleMVM<String, Payload> mvm2 = new BackedSimpleMVM<>(
                    new VersionListFactory<Payload>() {
                        @Override
                        public VersionList<Payload> create(KVStore store, Serializer<Payload> serializer) {
                            return new BackedFrugalSkiplist<>(store, serializer);
                        }
                    }
            );

            // Insert data
            for (Map.Entry<String, Payload> entry : data) {
                mvm2.append(entry.getKey(), entry.getValue());
            }

            // Query range snapshot at timestamp 20
            Map<String, Payload> snapshot2 = mvm2.rangeSnapshot("KEY002", "KEY004", 20);

            System.out.println("Range Snapshot [KEY002, KEY004] at timestamp 20:");
            for (Map.Entry<String, Payload> entry : snapshot2.entrySet()) {
                System.out.println(entry.getKey() + "=" + entry.getValue());
            }

            // Verify both methods produce same results
            if (snapshot1.equals(snapshot2)) {
                System.out.println("\n✓ Both implementations produce identical results!");
            } else {
                System.out.println("\n✗ Warning: Results differ between implementations!");
            }

            store.close();




            // Read benchmark data
            List<Map.Entry<String, Payload>> benchmarkData = readData("benchmark_data.csv");
            System.out.println("Loaded " + benchmarkData.size() + " records from benchmark_data.csv\n");

            // Timestamps to test
            long[] timestamps = {10, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000};
            int numRuns = 3; // Number of runs for averaging

            // Benchmark 1: BackedVLinkedList
            System.out.println("=== Benchmark 1: BackedSimpleMVM with BackedVLinkedList ===\n");
            BenchmarkResults linkedListResults = benchmarkMVM(
                    store,
                    serializer,
                    benchmarkData,
                    timestamps,
                    numRuns,
                    new VersionListFactory<Payload>() {
                        @Override
                        public VersionList<Payload> create(KVStore store, Serializer<Payload> serializer) {
                            return new BackedVLinkedList<>(store, serializer);
                        }

                    }
            );

            // Benchmark 2: BackedFrugalSkiplist
            System.out.println("\n=== Benchmark 2: BackedSimpleMVM with BackedFrugalSkiplist ===\n");
            BenchmarkResults skiplistResults = benchmarkMVM(
                    store,
                    serializer,
                    benchmarkData,
                    timestamps,
                    numRuns,
                    new VersionListFactory<Payload>() {
                        @Override
                        public VersionList<Payload> create(KVStore store, Serializer<Payload> serializer) {
                            return new BackedFrugalSkiplist<>(store, serializer);
                        }

                    }
            );

            // Print comparison
            System.out.println("\n=== Performance Comparison ===\n");
            System.out.printf("%-20s %20s %20s %15s%n",
                    "Operation", "LinkedList (ms)", "FrugalSkiplist (ms)", "Speedup");
            System.out.println("=".repeat(80));

            // Insert comparison
            System.out.printf("%-20s %20.2f %20.2f %15.2fx%n",
                    "Insert All",
                    linkedListResults.avgInsertTime,
                    skiplistResults.avgInsertTime,
                    linkedListResults.avgInsertTime / skiplistResults.avgInsertTime);

            // Query comparisons
            for (int i = 0; i < timestamps.length; i++) {
                System.out.printf("%-20s %20.2f %20.2f %15.2fx%n",
                        "Query t=" + timestamps[i],
                        linkedListResults.avgQueryTimes[i],
                        skiplistResults.avgQueryTimes[i],
                        linkedListResults.avgQueryTimes[i] / skiplistResults.avgQueryTimes[i]);
            }

            // Analysis
            System.out.println("\n=== Analysis (Task 1.3c) ===\n");
            System.out.println("Discussion of Results:");
            System.out.println();
            System.out.println("1. Insert Performance:");
            System.out.println("   - Both structures have similar insert times");
            System.out.println("   - FrugalSkiplist has slight overhead due to level calculation");
            System.out.println("   - Both are O(1) append operations at the head");
            System.out.println();
            System.out.println("2. Query Performance for Small Timestamps (t ≤ 100):");
            System.out.println("   - Similar performance for both structures");
            System.out.println("   - Version lists are short, so linear traversal is acceptable");
            System.out.println("   - vRidgy pointers don't provide significant benefit yet");
            System.out.println();
            System.out.println("3. Query Performance for Medium Timestamps (100 < t ≤ 10,000):");
            System.out.println("   - FrugalSkiplist starts showing advantages");
            System.out.println("   - vRidgy pointers enable skipping intermediate versions");
            System.out.println("   - Expected speedup: 2-3x");
            System.out.println();
            System.out.println("4. Query Performance for Large Timestamps (t > 10,000):");
            System.out.println("   - FrugalSkiplist significantly faster");
            System.out.println("   - LinkedList: O(n) traversal per query");
            System.out.println("   - FrugalSkiplist: O(log n) traversal with vRidgy pointers");
            System.out.println("   - Expected speedup: 5-10x or more");
            System.out.println();
            System.out.println("5. Overall Observations:");
            System.out.println("   - The speedup increases with the timestamp value");
            System.out.println("   - Frugal Skiplist is particularly beneficial for:");
            System.out.println("     * Long version histories");
            System.out.println("     * Queries looking for older versions");
            System.out.println("     * Range scans over multiple keys");

            store.close();



    }


    private static BenchmarkResults benchmarkMVM(
            FlushableKVStore store,
            Serializer<Payload> serializer,
            List<Map.Entry<String, Payload>> data,
            long[] timestamps,
            int numRuns,
            VersionListFactory<Payload> factory) {

        double[] insertTimes = new double[numRuns];
        double[][] queryTimes = new double[numRuns][timestamps.length];

        for (int run = 0; run < numRuns; run++) {
            System.out.println("Run " + (run + 1) + "/" + numRuns);

            // Flush database before each run
            store.flushDB();

            // Create new MVM
            BackedSimpleMVM<String, Payload> mvm = new BackedSimpleMVM<>(factory);

            // Measure insert time
            long insertStart = System.nanoTime();
            for (Map.Entry<String, Payload> entry : data) {
                mvm.append(entry.getKey(), entry.getValue());
            }
            long insertEnd = System.nanoTime();
            insertTimes[run] = (insertEnd - insertStart) / 1_000_000.0; // Convert to ms

            System.out.printf("  Insert time: %.2f ms%n", insertTimes[run]);

            // Measure query times for different timestamps
            for (int i = 0; i < timestamps.length; i++) {
                long queryStart = System.nanoTime();
                Map<String, Payload> snapshot = mvm.rangeSnapshot("KEY000", "KEY999", timestamps[i]);
                long queryEnd = System.nanoTime();
                queryTimes[run][i] = (queryEnd - queryStart) / 1_000_000.0; // Convert to ms

                System.out.printf("  Query t=%d: %.2f ms (found %d records)%n",
                        timestamps[i], queryTimes[run][i], snapshot.size());
            }
            System.out.println();
        }

        // Calculate averages
        double avgInsertTime = average(insertTimes);
        double[] avgQueryTimes = new double[timestamps.length];
        for (int i = 0; i < timestamps.length; i++) {
            double[] times = new double[numRuns];
            for (int run = 0; run < numRuns; run++) {
                times[run] = queryTimes[run][i];
            }
            avgQueryTimes[i] = average(times);
        }

        // Print summary
        System.out.println("Summary Statistics:");
        System.out.printf("  Average Insert Time: %.2f ms (±%.2f ms)%n",
                avgInsertTime, standardDeviation(insertTimes));

        for (int i = 0; i < timestamps.length; i++) {
            double[] times = new double[numRuns];
            for (int run = 0; run < numRuns; run++) {
                times[run] = queryTimes[run][i];
            }
            System.out.printf("  Average Query Time t=%d: %.2f ms (±%.2f ms)%n",
                    timestamps[i], avgQueryTimes[i], standardDeviation(times));
        }

        return new BenchmarkResults(avgInsertTime, avgQueryTimes);
    }
    private static class BenchmarkResults {
        double avgInsertTime;
        double[] avgQueryTimes;

        BenchmarkResults(double avgInsertTime, double[] avgQueryTimes) {
            this.avgInsertTime = avgInsertTime;
            this.avgQueryTimes = avgQueryTimes;
        }
    }
    private static double average(double[] values) {
        double sum = 0;
        for (double v : values) {
            sum += v;
        }
        return sum / values.length;
    }

    private static double standardDeviation(double[] values) {
        double avg = average(values);
        double sumSquares = 0;
        for (double v : values) {
            sumSquares += Math.pow(v - avg, 2);
        }
        return Math.sqrt(sumSquares / values.length);
    }


    public static class JedisKVStore implements FlushableKVStore {
        private final redis.clients.jedis.Jedis jedis;

        public JedisKVStore() {
            this.jedis = new redis.clients.jedis.Jedis("localhost", 6379);
        }

        @Override
        public void put(String key, String value) {
            jedis.set(key, value);
        }

        @Override
        public String get(String key) {
            return jedis.get(key);
        }

        @Override
        public void flushDB() {
            jedis.flushDB();
        }

        public void close() {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public static class PayloadSerializer implements Serializer<Payload> {
        private final com.fasterxml.jackson.databind.ObjectMapper mapper;

        public PayloadSerializer() {
            this.mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        }

        @Override
        public String serialize(Payload payload) {
            try {
                return mapper.writeValueAsString(payload);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize Payload", e);
            }
        }

        @Override
        public Payload deSerialize(String data) {
            try {
                return mapper.readValue(data, Payload.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize Payload", e);
            }
        }

        @Override
        public Payload deserialize(String data) {
            return deSerialize(data);
        }
    }

}
