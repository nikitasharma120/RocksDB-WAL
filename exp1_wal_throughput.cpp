/*
 * EXPERIMENT 1: WAL Enabled vs Disabled — Write Throughput
 *
 * CODE PATH (WAL ON):
 *   DBImpl::Write()            db/db_impl/db_impl_write.cc:150
 *   → DBImpl::WriteImpl()      db/db_impl/db_impl_write.cc:180
 *   → DBImpl::WriteToWAL()     db/db_impl/db_impl_write.cc:1320
 *   → log::Writer::AddRecord() db/log_writer.cc:65
 *   → MemTable::Add()
 *
 * CODE PATH (WAL OFF):
 *   Same, but at db_impl_write.cc:494 disableWAL=true sets
 *   has_unpersisted_data_=true and the WriteToWAL() call at line 501 is skipped.
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
    auto s = rocksdb::DB::Open(opts, path, &db);
    if (!s.ok()) { std::cerr << s.ToString() << "\n"; exit(1); }
    return db;
}

struct Result {
    std::string label;
    double ops_per_sec;
    double us_per_op;
};

Result bench(const std::string& label, bool wal_disabled, bool sync, int batch) {
    const int TOTAL = 100000;
    const int VSIZE = 256;
    std::string path = "/tmp/exp1_" + std::to_string((int)wal_disabled) +
                       std::to_string((int)sync) + std::to_string(batch);
    auto* db = fresh_db(path);

    rocksdb::WriteOptions wo;
    wo.disableWAL = wal_disabled;
    wo.sync       = sync;
    std::string val(VSIZE, 'v');
    int ops = 0;
    auto t0 = Clock::now();

    if (batch == 1) {
        for (int i = 0; i < TOTAL; i++) {
            db->Put(wo, "k" + std::to_string(i), val);
            ops++;
        }
    } else {
        for (int i = 0; i < TOTAL; i += batch) {
            rocksdb::WriteBatch wb;
            int end = std::min(i + batch, TOTAL);
            for (int j = i; j < end; j++)
                wb.Put("k" + std::to_string(j), val);
            db->Write(wo, &wb);
            ops += (end - i);
        }
    }

    double elapsed = std::chrono::duration<double>(Clock::now() - t0).count();
    delete db;
    fs::remove_all(path);
    return { label, ops / elapsed, elapsed * 1e6 / ops };
}

int main() {
    std::cout << "\n=== EXPERIMENT 1: WAL Cost — Throughput Benchmark ===\n";
    std::cout << "100,000 writes, 256-byte values\n\n";

    std::vector<Result> results = {
        bench("WAL=ON  sync=OFF batch=1",    false, false, 1),
        bench("WAL=OFF sync=OFF batch=1",    true,  false, 1),
        bench("WAL=ON  sync=ON  batch=1",    false, true,  1),
        bench("WAL=ON  sync=OFF batch=100",  false, false, 100),
        bench("WAL=ON  sync=OFF batch=1000", false, false, 1000),
    };

    std::cout << std::left
              << std::setw(32) << "Configuration"
              << std::setw(18) << "Throughput"
              << "Avg Latency\n"
              << std::string(64, '-') << "\n";

    for (auto& r : results) {
        std::cout << std::setw(32) << r.label
                  << std::setw(18) << (std::to_string((int)r.ops_per_sec) + " ops/s")
                  << std::to_string((int)r.us_per_op) + " us/op\n";
    }

    double base = results[0].ops_per_sec;
    std::cout << "\n--- Speedup vs WAL=ON sync=OFF baseline ---\n";
    for (auto& r : results)
        std::cout << "  " << r.label << " -> "
                  << std::fixed << std::setprecision(2)
                  << (r.ops_per_sec / base) << "x\n";

    double max_ops = 0;
    for (const auto& r : results) max_ops = std::max(max_ops, r.ops_per_sec);

    std::cout << "\n--- Graphical Comparison (Throughput ops/s) ---\n";
    for (const auto& r : results) {
        int bar_len = max_ops > 0 ? (int)(50.0 * r.ops_per_sec / max_ops) : 0;
        std::cout << std::left << std::setw(32) << r.label << " |"
                  << std::string(bar_len, '#') << std::string(50 - bar_len, ' ')
                  << "| " << (int)r.ops_per_sec << "\n";
    }

    std::cout << "\nCODE REFS:\n"
              << "  WAL skip branch: db/db_impl/db_impl_write.cc:494\n"
              << "  WAL write call:  db/db_impl/db_impl_write.cc:507\n"
              << "  Disk append:     db/log_writer.cc:65 Writer::AddRecord()\n\n";
    return 0;
}
