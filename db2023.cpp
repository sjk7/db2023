#pragma once

#include <string>
#include <string_view>
#include <cstdint>
#include <fstream>
#include <cmath>
#include <vector>
#include <cassert>
#include <iostream>

#ifdef _WIN32
#include <io.h>
#define F_OK 0
#define access _access
#endif

namespace db2023 {
using countType = std::uint32_t;
static inline uint32_t constexpr MAGIC = 558819;

struct RecordBase {
    countType uid;
    uint32_t flags;
    uint32_t reserved;
};
struct header {
    countType magic;
    countType version;
    countType rowCount;
    countType reserved;
    countType recordSize;
};

bool fileExists(const std::string& fp) {
    if (access(fp.c_str(), F_OK) == 0) {
        return true;
    } else {
        return false;
    }
}

auto fileSize(const std::string& fp) {
#ifdef _WIN32
    struct _stat64 st;
    int result = _stat64(fp.c_str(), &st);
    if (result != 0) {
        throw std::runtime_error("Cannot get status for file: " + fp);
    }
    return st.st_size;

#else
    struct stat64 st;
    int result = stat64(fp.c_str(), &st);
    if (result != 0) {
        throw std::runtime_error("Cannot get status for file: " + fp);
    }
    return st.st_size;
#endif
}

template <typename R> class DB {
    DB() = delete;
    DB(const std::string& fp) : m_filePath(fp) {}
    DB(const DB&) = delete;
    DB(DB&&) = delete;
    template <typename D> friend class DBWriter;

    void open(const std::string& fp, bool recurse = false) {
        std::ios::openmode om = std::ios::binary | std::ios::out;
        const auto existed = fileExists(fp);
        if (existed) om |= std::ios::in;
        if (m_f.is_open()) {
            m_f.close();
            m_f.clear();
        }
        m_f.open(fp, om);
        if (!m_f) throw std::runtime_error("Cannot open db at location: " + fp);

        if (existed) {
            read_header(m_f, fp);
        } else {
            if (!recurse) {
                writeHeader(makeHeader(), m_f, m_filePath);
                open(fp, true); // recurse
            } else {
                throw std::runtime_error(
                    "Unable to open the requested file: " + fp);
            }
        }
    }
    inline header makeHeader() {
        header ret = {MAGIC, 1, rowCount(), 0, sizeof(R)};
        return ret;
    }
    void writeHeader(
        const header& h, std::fstream& f, const std::string& filePath) {
        m_hdr = h;
        m_f.seekp(0);
        m_f.write((char*)&h, sizeof(h));
        if (!m_f) {
            throw std::runtime_error(
                "Cannot write header to file: " + filePath);
        }
    }

    void writeHeader(countType newRowCount) {
        m_rowCount = newRowCount;
        m_hdr = makeHeader();
        writeHeader(m_hdr, m_f, m_filePath);
    }
    header m_hdr{};
    header& read_header(std::fstream& f, const std::string& fp) {
        f.seekg(0);
        f.read((char*)&m_hdr, sizeof(m_hdr));
        if (!f) {
            throw std::runtime_error("Cannot read header in file: " + fp);
        }
        if (m_hdr.magic != MAGIC) {
            throw std::runtime_error("Header: bad magic");
        }
        if (m_hdr.reserved != 0) {
            throw std::runtime_error("Header: bad reserved");
        }
        if (m_hdr.version != 1) {
            throw std::runtime_error("Header: bad version");
        }
        const auto calced = calcRowCount();
        if (m_hdr.rowCount != calced) {
            throw std::runtime_error("Header: bad row count");
        }
        m_rowCount = calced;
        const auto szR = sizeof(R);
        if (m_hdr.recordSize != szR) {
            throw std::runtime_error("Header: bad record size.");
        }
        return m_hdr;
    }

    countType calcRowCount() {
        const auto sz = fileSize(m_filePath);
        if (sz <= sizeof(m_hdr)) return 0;
        const double adjustedSize = (double)(sz - sizeof(m_hdr));
        double dec = 0;
        const auto fractPart = std::modf(adjustedSize, &dec);
        if (fractPart != 0) {
            throw std::runtime_error("DB, with filepath: " + m_filePath
                + " is corrupt. The size is wrong");
        }
        const auto szR = sizeof(R);
        countType adj = static_cast<countType>(adjustedSize);
        const auto ret = adj / (countType)szR;
        return ret;
    }
    void seekToRecord(const countType which) {
        const auto start = sizeof(m_hdr);
        const auto avail = fileSize(m_filePath);
        const auto pos = start + (which * sizeof(R));
        m_f.seekg(pos);
        if (!m_f) {
            throw std::runtime_error("Unable to seek to record "
                + std::to_string(which) + " " + m_filePath);
        }
    }

    template <typename CB>
    void readAll(CB&& cb, bool avoidCallbackAbort = false) {
        seekToRecord(0);
        const auto count = rowCount();
        R r = {};
        m_uidIndex.resize(count);
        countType ctr = 0;

        while (ctr < count) {
            m_f.read((char*)&r, sizeof(R));
            if (!m_f) {
                throw std::runtime_error(
                    "Bad readAll as position: " + ctr + m_filePath);
            }
            assert(r.uid > 0); // 0 is INVALID_UID
            m_uidIndex[r.uid - 1] = ctr;
            if (avoidCallbackAbort) {
                cb(r);
            } else {
                if (cb(r) < 0) break;
            }
            ++ctr;
        }
    }
    std::vector<countType> m_uidIndex;
    std::string m_filePath;
    countType m_rowCount{0};
    countType m_uidNext{0};
    std::fstream m_f;
    countType nextUID() { return ++m_uidNext; }

    R createNewRecord() {
        R ret{};
        ret.uid = nextUID();
        return ret;
    }

    public:
    template <typename CB>
    DB(const std::string& filePath, CB&& cb) : DB(filePath) {
        open(filePath);
        readAll(cb, true);
        static_assert(std::is_base_of_v<RecordBase, R>);
        static_assert(std::is_trivial_v<R>);
    }
    countType rowIndexFromUID(countType uid) {
        if (uid == 0) {
            throw std::runtime_error("uid0 is not a valid uid");
        }
        if (uid >= m_uidIndex.size()) {
            throw std::runtime_error("rowIndexFromUID: out of range uid");
        }
        return m_uidIndex[uid];
    }

    using RecordType = R;
    // read records from rowStart until your callback returns < 0, or the end of
    // the records.
    template <typename CB> void readUntil(countType rowStart, CB&& cb) {
        seekToRecord(rowStart);
        const auto count = rowCount();
        R r = {};
        countType ctr = 0;
        while (ctr < count) {
            m_f.read((char*)&r, sizeof(R));
            if (cb(r) < 0) break;
        }
    }

    countType rowCount() const noexcept { return m_rowCount; }
};

template <typename DB> class DBWriter {

    DB& m_db;
    using RecordType = typename DB::RecordType;

    public:
    template <typename CB> DBWriter(DB& db, CB&& cb) : m_db(db) {

        auto& f = db.m_f;
        bool ok = true;
        f.seekp(0, std::ios::end);
        oldRowCount = m_db.rowCount();
        newRowCount = oldRowCount;

        RecordType r = {};
        while (ok) {
            r.uid = m_db.nextUID();
            if (cb(r)) {
                f.write((char*)&r, sizeof(RecordType));
                if (!f) {
                    if (newRowCount != oldRowCount) {
                        m_db.writeHeader(newRowCount);
                    }
                    throw std::runtime_error("DBWriter: file is bad");
                    ok = false;
                }
                ++newRowCount;

            } else {
                ok = false;
            }
        };
        finish();
    }

    countType newRowCount{0};
    countType oldRowCount{0};

    void finish() {
        if (newRowCount != oldRowCount && newRowCount) {
            m_db.writeHeader(newRowCount);
            const auto c = m_db.calcRowCount();
            const auto r = m_db.rowCount();
            assert(c == r);
            newRowCount = oldRowCount;
            // re-index
            m_db.readAll([](auto&) { return 0; }, true);
        }
    }

    ~DBWriter() { finish(); }
};

} // namespace db2023

struct mystruct : db2023::RecordBase {
    char artist[32];
    char title[32];
    char categories[64];
    uint32_t intro[4];
    char filepath[512];
    uint8_t opener;
};

struct mystructBigger : mystruct {
    char artist[32];
    char title[32];
    char categories[64];
    uint32_t intro[4];
    char filepath[512];
    uint8_t opener;
    uint8_t reserved;
};

int main() {
    using std::cout;
    using std::endl;

    bool threw = false;

    std::string filePath("test.db");
    // This one should not throw (though it will be empty)
    db2023::DB<mystruct> DB(filePath, [](const mystruct&) { return 0; });
    try {

        // This next one should throw
        db2023::DB<mystructBigger> BadDB(
            filePath, [](const mystructBigger&) { return 0; });
    } catch (const std::exception& e) {
        cout << "Correctly threw: " << e.what() << endl;
        threw = true;
    }
    assert(threw);

    size_t ctr = 0;
    const auto countNow = DB.rowCount();
    if (countNow < 100) {
        db2023::DBWriter myWriter(DB, [&](mystruct& r) {
            const auto s = std::to_string(ctr);
            memcpy(r.artist, s.data(), s.size());
            if (ctr++ >= 100) return 0;
            return 1;
        });
    }

    const auto newCount = DB.rowCount() + 10;
    ctr = DB.rowCount();
    db2023::DBWriter myWriter(DB, [&](mystruct& r) {
        const auto s = std::to_string(ctr);
        memcpy(r.artist, s.data(), s.size());
        if (ctr++ >= newCount) return 0;
        return 1;
    });

    const auto myCount = DB.rowCount();
    assert(myCount == newCount);
    const auto rw = DB.rowIndexFromUID(myCount);
    return 0;
}
