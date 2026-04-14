/*
 * EXPERIMENT 5: Failure Analysis — What Happens When Data Size Increases?
 *
 * FAILURE ANALYSIS QUESTION: "What happens when data size increases significantly?"
 *
 * WAL-SPECIFIC RISKS AT SCALE:
 *   1. WAL accumulates until MemTable is flushed to SST
 *      → If flush is slow, WAL grows unboundedly
 *      Code: db/db_impl/db_impl.cc DBImpl::FindObsoleteFiles() controls WAL deletion
 *
 *   2. WAL file rotation: max_total_wal_size option
 *      Code: db/db_impl/db_impl_write.cc SwitchWAL() called when WAL exceeds limit
 *
 *   3. Recovery time grows linearly with WAL size
 *      Code: db/db_impl/db_impl_open.cc:1073 RecoverLogFiles() reads entire WAL
 *
 * THIS EXPERIMENT:
 *   Measures WAL growth rate and recovery time as dataset grows.
 *   Shows the linear relationship between data volume and recovery cost.
 */

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <filesystem>

namespace fs = std::filesystem;
using Clock = std::chrono::high_resolution_clock;

uintmax_t dir_size(const std::string& path, const std::string& ext) {
    uintmax_t t = 0;
    for (auto& e : fs::directory_iterator(path))
        if (e.path().extension() == ext) t += fs::file_size(e.path());
    return t;
}

struct ScaleResult {
    int num_ops;
    int vsize;
    double data_mb;
    double wal_mb;
    double write_sec;
    double recovery_ms;
    double wal_overhead_pct;
};

ScaleResult measure(int num_ops, int vsize) {
    const std::string path = "/tmp/exp5_scale";
    fs::remove_all(path);

    rocksdb::Options opts;
    opts.create_if_missing        = true;
    opts.disable_auto_compactions = true;
    opts.write_buffer_size        = 1024 << 20; // 1GB memtable: never flush
    rocksdb::DB* db;
    rocksdb::DB::Open(opts, path, &db);

    rocksdb::WriteOptions wo;
    std::string val(vsize, 'x');

    auto t0 = Clock::now();
    for (int i = 0; i < num_ops; i++)
        db->Put(wo, "k" + std::to_string(i), val);
    double write_sec = std::chrono::duration<double>(Clock::now() - t0).count();

    uintmax_t wal_bytes = dir_size(path, ".log");
    double data_mb = (double)num_ops * vsize / (1024.0 * 1024.0);
    double wal_mb  = (double)wal_bytes / (1024.0 * 1024.0);

    // Close WITHOUT flushing (WAL must be replayed on next open)
    delete db;

    // Measure recovery time = time to replay WAL
    auto tr0 = Clock::now();
    rocksdb::DB* db2;
    opts.create_if_missing = false;
    rocksdb::DB::Open(opts, path, &db2);
    double recovery_ms = std::chrono::duration<double, std::milli>(
        Clock::now() - tr0).count();
    delete db2;
    fs::remove_all(path);

    return {
        num_ops, vsize, data_mb, wal_mb, write_sec, recovery_ms,
        (wal_mb / data_mb) * 100.0
    };
}

int main() {
    std::cout << "\n=== EXPERIMENT 5: WAL Growth and Recovery Time at Scale ===\n\n";

    std::cout << "--- Part A: Fixed value size (256B), increasing number of ops ---\n";
    std::vector<int> op_counts = {1000, 5000, 10000, 50000, 100000};

    std::cout << std::left
              << std::setw(12) << "Ops"
              << std::setw(14) << "Data (MB)"
              << std::setw(14) << "WAL (MB)"
              << std::setw(16) << "Write Time"
              << std::setw(16) << "Recovery Time"
              << "WAL Overhead\n"
              << std::string(82, '-') << "\n";

    std::vector<ScaleResult> results_a;
    for (int ops : op_counts) {
        auto r = measure(ops, 256);
        results_a.push_back(r);
        std::cout << std::setw(12) << r.num_ops
                  << std::setw(14) << std::fixed << std::setprecision(1) << r.data_mb
                  << std::setw(14) << r.wal_mb
                  << std::setw(16) << (std::to_string((int)(r.write_sec*1000)) + " ms")
                  << std::setw(16) << (std::to_string((int)r.recovery_ms) + " ms")
                  << std::to_string((int)r.wal_overhead_pct) + "%\n";
    }

    std::cout << "\n--- Graphical Comparison (Recovery Time vs Ops at 256B) ---\n";
    double max_rec_a = 0;
    for (const auto& r : results_a) max_rec_a = std::max(max_rec_a, r.recovery_ms);
    for (const auto& r : results_a) {
        int bar_len = max_rec_a > 0 ? (int)(50.0 * r.recovery_ms / max_rec_a) : 0;
        std::cout << "ops=" << std::left << std::setw(8) << r.num_ops << " |"
                  << std::string(bar_len, '#') << std::string(50 - bar_len, ' ')
                  << "| " << (int)r.recovery_ms << " ms\n";
    }

    std::cout << "\n--- Part B: Fixed ops (10000), increasing value size ---\n";
    std::vector<int> vsizes = {64, 512, 4096, 16384};

    std::cout << std::left
              << std::setw(12) << "Value Size"
              << std::setw(14) << "Data (MB)"
              << std::setw(14) << "WAL (MB)"
              << std::setw(16) << "Write Time"
              << "Recovery Time\n"
              << std::string(60, '-') << "\n";

    std::vector<ScaleResult> results_b;
    for (int vs : vsizes) {
        auto r = measure(10000, vs);
        results_b.push_back(r);
        std::cout << std::setw(12) << (std::to_string(vs) + "B")
                  << std::setw(14) << std::fixed << std::setprecision(1) << r.data_mb
                  << std::setw(14) << r.wal_mb
                  << std::setw(16) << (std::to_string((int)(r.write_sec*1000)) + " ms")
                  << std::to_string((int)r.recovery_ms) + " ms\n";
    }

    std::cout << "\n--- Graphical Comparison (Recovery Time vs Value Size at 10k ops) ---\n";
    double max_rec_b = 0;
    for (const auto& r : results_b) max_rec_b = std::max(max_rec_b, r.recovery_ms);
    for (const auto& r : results_b) {
        int bar_len = max_rec_b > 0 ? (int)(50.0 * r.recovery_ms / max_rec_b) : 0;
        std::cout << "val=" << std::left << std::setw(8) << (std::to_string(r.vsize) + "B") << " |"
                  << std::string(bar_len, '#') << std::string(50 - bar_len, ' ')
                  << "| " << (int)r.recovery_ms << " ms\n";
    }

    std::cout << "\nFAILURE ANALYSIS:\n";
    std::cout << "  Recovery time grows linearly with WAL size.\n";
    std::cout << "  Code: RecoverLogFiles() at db_impl_open.cc:1073 reads every record.\n";
    std::cout << "  A system that crashes with 10GB WAL may take minutes to recover.\n";
    std::cout << "  RocksDB mitigates this with:\n";
    std::cout << "    1. max_total_wal_size: forces MemTable flush → WAL rotation\n";
    std::cout << "       Code: db_impl_write.cc SwitchWAL()\n";
    std::cout << "    2. WAL TTL: recycle old WAL files\n";
    std::cout << "       Code: log_writer.cc recycle_log_files_ flag\n";
    std::cout << "    3. Checkpoint: snapshot SST state → WAL can start fresh\n\n";

    return 0;
}
