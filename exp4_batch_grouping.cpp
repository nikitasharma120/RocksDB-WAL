/*
 * EXPERIMENT 4: Write Group Batching — The WAL's Core Throughput Mechanism
 *
 * DESIGN DECISION BEING TESTED:
 *   RocksDB uses "write group" batching (db_impl_write.cc:180 WriteImpl).
 *   When multiple threads call db->Put() concurrently, one thread becomes
 *   the "leader" and merges all pending writes into ONE WAL append.
 *
 * CODE PATH:
 *   WriteImpl() → WriteThread::JoinBatchGroup()  db/write_thread.cc
 *               → leader merges WriteBatches
 *               → ONE call to WriteToWAL()        db_impl_write.cc:507
 *               → followers apply to MemTable in parallel
 *
 * WHAT WE TEST:
 *   Explicit batch size: how does grouping N writes into one WriteBatch
 *   change throughput? This isolates the WAL amortization effect.
 *
 * TRADEOFF:
 *   Larger batches = fewer WAL appends = higher throughput
 *   But: larger batches = higher per-operation latency (must wait to fill batch)
 *   This is the durability vs throughput tradeoff at the core of WAL design.
 */

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <filesystem>

namespace fs = std::filesystem;
using Clock = std::chrono::high_resolution_clock;

rocksdb::DB* fresh_db(const std::string& path) {
    fs::remove_all(path);
    rocksdb::Options opts;
    opts.create_if_missing        = true;
    opts.disable_auto_compactions = true;
    opts.write_buffer_size        = 512 << 20;
    rocksdb::DB* db;
    rocksdb::DB::Open(opts, path, &db);
    return db;
}

uintmax_t wal_size(const std::string& path) {
    uintmax_t t = 0;
    for (auto& e : fs::directory_iterator(path))
        if (e.path().extension() == ".log") t += fs::file_size(e.path());
    return t;
}

struct Result {
    int batch_size;
    int num_wal_appends;   // total WAL writes = total_ops / batch_size
    double ops_per_sec;
    double p99_us;         // tail latency per batch
    uintmax_t wal_bytes;
};

Result bench(int batch_size, int total_ops, int vsize) {
    std::string path = "/tmp/exp4_b" + std::to_string(batch_size);
    auto* db = fresh_db(path);
    rocksdb::WriteOptions wo;
    std::string val(vsize, 'v');

    std::vector<double> latencies;
    latencies.reserve(total_ops / batch_size + 1);

    auto t0 = Clock::now();
    int ops_done = 0;

    for (int i = 0; i < total_ops; i += batch_size) {
        rocksdb::WriteBatch wb;
        int end = std::min(i + batch_size, total_ops);
        for (int j = i; j < end; j++)
            wb.Put("key" + std::to_string(j), val);

        auto ts = Clock::now();
        db->Write(wo, &wb);
        auto te = Clock::now();

        double lat = std::chrono::duration<double, std::micro>(te - ts).count();
        latencies.push_back(lat);
        ops_done += (end - i);
    }

    double elapsed = std::chrono::duration<double>(Clock::now() - t0).count();

    // p99 latency
    std::sort(latencies.begin(), latencies.end());
    double p99 = latencies[(size_t)(latencies.size() * 0.99)];

    uintmax_t wb = wal_size(path);
    delete db;
    fs::remove_all(path);

    return {
        batch_size,
        (int)latencies.size(),
        ops_done / elapsed,
        p99,
        wb
    };
}

int main() {
    const int TOTAL = 100000;
    const int VSIZE = 256;

    std::cout << "\n=== EXPERIMENT 4: Write Batch Size vs WAL Appends ===\n";
    std::cout << "Total ops: " << TOTAL << "  Value size: " << VSIZE << "B\n\n";

    std::vector<int> batch_sizes = {1, 10, 50, 100, 500, 1000, 5000, 10000};
    std::vector<Result> results;
    for (int b : batch_sizes)
        results.push_back(bench(b, TOTAL, VSIZE));

    std::cout << std::left
              << std::setw(12) << "Batch Size"
              << std::setw(16) << "WAL Appends"
              << std::setw(18) << "Throughput"
              << std::setw(16) << "p99 Latency"
              << "WAL Size\n"
              << std::string(78, '-') << "\n";

    for (auto& r : results) {
        double wal_mb = (double)r.wal_bytes / (1024.0 * 1024.0);
        std::cout << std::setw(12) << r.batch_size
                  << std::setw(16) << r.num_wal_appends
                  << std::setw(18) << (std::to_string((int)r.ops_per_sec) + " ops/s")
                  << std::setw(16) << (std::to_string((int)r.p99_us) + " µs")
                  << std::to_string((int)wal_mb) + " MB\n";
    }

    std::cout << "\nTRADEOFF ANALYSIS:\n";
    double base = results[0].ops_per_sec;
    for (auto& r : results) {
        std::cout << "  batch=" << std::setw(6) << r.batch_size
                  << " -> " << std::fixed << std::setprecision(1)
                  << (r.ops_per_sec / base) << "x throughput"
                  << "  p99=" << (int)r.p99_us << "µs\n";
    }

    std::cout << "\n--- Graphical Comparison (Throughput ops/s) ---\n";
    double max_ops = 0;
    for (const auto& r : results) max_ops = std::max(max_ops, r.ops_per_sec);
    
    for (const auto& r : results) {
        int bar_len = max_ops > 0 ? (int)(50.0 * r.ops_per_sec / max_ops) : 0;
        std::cout << "batch=" << std::left << std::setw(8) << r.batch_size << " |"
                  << std::string(bar_len, '#') << std::string(50 - bar_len, ' ')
                  << "| " << (int)r.ops_per_sec << " ops/s\n";
    }

    std::cout << "\nDESIGN INSIGHT:\n";
    std::cout << "  Each WriteBatch = ONE WAL append (one call to Writer::AddRecord)\n";
    std::cout << "  Reducing WAL appends from " << results[0].num_wal_appends
              << " to " << results.back().num_wal_appends
              << " gives ~" << (int)(results.back().ops_per_sec / base) << "x throughput.\n";
    std::cout << "  But p99 latency grows as batch must wait to be filled.\n";
    std::cout << "  RocksDB's write group (WriteThread, db/write_thread.cc) does this\n";
    std::cout << "  automatically for concurrent callers — no explicit batching needed.\n\n";

    return 0;
}
