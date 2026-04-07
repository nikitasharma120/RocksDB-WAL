/*
 * EXPERIMENT 3: Behavior Under Skew — Large Values + Key Skew
 *
 * FAILURE ANALYSIS QUESTION: "What happens under skew?"
 *
 * TWO SUB-TESTS:
 *   A) Value size skew: small vs large values — how does WAL record size affect throughput?
 *      WAL record = header(7 bytes) + CRC(4 bytes) + payload
 *      Code: db/log_writer.cc:65 Writer::AddRecord() — fragments records across 32KB blocks
 *
 *   B) Key skew: repeated writes to same key (hot key) vs uniform distribution
 *      Each Put() still writes a full WAL record even for overwrites.
 *      The WAL is append-only — it does NOT deduplicate. This is by design.
 *      Code: db/db_impl/db_impl_write.cc:507 — WriteToWAL() called for every Put()
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
#include <random>

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

uintmax_t wal_size_bytes(const std::string& path) {
    uintmax_t total = 0;
    for (auto& e : fs::directory_iterator(path))
        if (e.path().extension() == ".log")
            total += fs::file_size(e.path());
    return total;
}

struct Result {
    std::string label;
    double ops_per_sec;
    uintmax_t wal_bytes;
    int ops;
};

Result run(const std::string& label, const std::string& path,
           int num_ops, int vsize, bool hot_key) {
    auto* db = fresh_db(path);
    rocksdb::WriteOptions wo;
    wo.disableWAL = false;

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(0, num_ops - 1);
    std::string val(vsize, 'v');

    auto t0 = Clock::now();
    for (int i = 0; i < num_ops; i++) {
        std::string key = hot_key
            ? "hotkey_0000"                          // same key always
            : "key_" + std::to_string(dist(rng));    // random key
        db->Put(wo, key, val);
    }
    double elapsed = std::chrono::duration<double>(Clock::now() - t0).count();
    uintmax_t wal = wal_size_bytes(path);
    delete db;
    fs::remove_all(path);
    return { label, num_ops / elapsed, wal, num_ops };
}

int main() {
    const int OPS = 50000;

    std::cout << "\n=== EXPERIMENT 3: WAL Behavior Under Skew ===\n\n";

    // --- PART A: Value size skew ---
    std::cout << "--- Part A: Value Size Impact on WAL ---\n";
    std::cout << "Each WAL record = 7-byte header + CRC + payload\n";
    std::cout << "Code: db/log_writer.cc:65 Writer::AddRecord()\n\n";

    std::vector<std::pair<int,std::string>> sizes = {
        {64,    "64B   (tiny)  "},
        {512,   "512B  (small) "},
        {4096,  "4KB   (medium)"},
        {65536, "64KB  (large) "},
    };

    std::cout << std::left
              << std::setw(20) << "Value Size"
              << std::setw(18) << "Throughput"
              << std::setw(18) << "WAL Size"
              << "MB written\n"
              << std::string(70, '-') << "\n";

    std::vector<Result> results_a;
    for (auto& [vsize, label] : sizes) {
        auto r = run(label, "/tmp/exp3_vs_" + std::to_string(vsize),
                     OPS, vsize, false);
        results_a.push_back(r);
        double mb = (double)r.wal_bytes / (1024.0 * 1024.0);
        double data_mb = (double)OPS * vsize / (1024.0 * 1024.0);
        std::cout << std::setw(20) << label
                  << std::setw(18) << (std::to_string((int)r.ops_per_sec) + " ops/s")
                  << std::setw(18) << (std::to_string((int)mb) + " MB")
                  << std::to_string((int)data_mb) + " MB data\n";
    }

    std::cout << "\n--- Graphical Comparison (WAL Size MB) ---\n";
    double max_wal_mb = 0;
    for (auto& r : results_a) {
        double mb = (double)r.wal_bytes / (1024.0 * 1024.0);
        max_wal_mb = std::max(max_wal_mb, mb);
    }
    for (auto& r : results_a) {
        double mb = (double)r.wal_bytes / (1024.0 * 1024.0);
        int bar_len = max_wal_mb > 0 ? (int)(50.0 * mb / max_wal_mb) : 0;
        std::cout << std::left << std::setw(20) << r.label << " |"
                  << std::string(bar_len, '#') << std::string(50 - bar_len, ' ')
                  << "| " << (int)mb << " MB\n";
    }

    // --- PART B: Key skew (hot key) ---
    std::cout << "\n--- Part B: Hot Key Skew ---\n";
    std::cout << "WAL is append-only. Overwrites to same key STILL write full WAL records.\n";
    std::cout << "Code: db/db_impl/db_impl_write.cc:507 WriteToWAL() called per Put()\n\n";

    auto uniform = run("Uniform keys", "/tmp/exp3_uniform", OPS, 256, false);
    auto hotkey  = run("Hot key (1 key)", "/tmp/exp3_hot",  OPS, 256, true);

    std::cout << std::left
              << std::setw(20) << "Pattern"
              << std::setw(18) << "Throughput"
              << "WAL Size\n"
              << std::string(50, '-') << "\n";

    auto print = [](const Result& r) {
        double mb = (double)r.wal_bytes / (1024.0 * 1024.0);
        std::cout << std::setw(20) << r.label
                  << std::setw(18) << (std::to_string((int)r.ops_per_sec) + " ops/s")
                  << std::to_string((int)mb) + " MB WAL\n";
    };
    print(uniform);
    print(hotkey);

    std::cout << "\n--- Graphical Comparison (Throughput ops/s) ---\n";
    double max_ops_b = std::max(uniform.ops_per_sec, hotkey.ops_per_sec);
    auto print_bar_ops = [max_ops_b](const Result& r) {
        int bar_len = max_ops_b > 0 ? (int)(50.0 * r.ops_per_sec / max_ops_b) : 0;
        std::cout << std::left << std::setw(20) << r.label << " |"
                  << std::string(bar_len, '#') << std::string(50 - bar_len, ' ')
                  << "| " << (int)r.ops_per_sec << " ops/s\n";
    };
    print_bar_ops(uniform);
    print_bar_ops(hotkey);

    std::cout << "\nKEY INSIGHT:\n";
    std::cout << "  Hot key writes produce IDENTICAL WAL sizes to uniform writes.\n";
    std::cout << "  The WAL does not know or care about key uniqueness — it is a\n";
    std::cout << "  pure operation log (like a redo log in MySQL/PostgreSQL).\n";
    std::cout << "  Deduplication only happens LATER during MemTable compaction.\n";
    std::cout << "  Under hot-key skew: WAL grows at the same rate, but the MemTable\n";
    std::cout << "  stays small (one key). This means WAL can outgrow MemTable significantly.\n\n";

    return 0;
}
