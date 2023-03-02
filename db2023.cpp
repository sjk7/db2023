// This is an independent project of an individual developer. Dear PVS-Studio,
// please check it.

// PVS-Studio Static Code Analyzer for C, C++, C#, and Java:
// http://www.viva64.com

#include <string>
#include <string_view>
#include <cstdint>
#include <fstream>
#include <cmath>
#include <vector>
#include <cassert>
#include <iostream>
#include <set> // uidchecker

#ifdef _WIN32
#include <io.h>
#define F_OK 0
#define access _access
#else
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#endif

namespace db2023 {
using countType = std::uint32_t;
static inline uint32_t constexpr MAGIC = 558819;

struct RecordBase {
    countType uid;
    uint32_t flags;
    uint32_t reserved;
};

static inline constexpr countType INVALID_UID = 0;
static inline constexpr countType INVALID_ROW = -1;
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
#ifdef __APPLE__
    struct stat st; // stat() is always 64-bit on mac, it seems.
    int result = stat(fp.c_str(), &st);
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
#endif
}

struct ReadFlags {
    static constexpr unsigned int avoidCallbackAbort = 2;
    static constexpr unsigned int DEFAULT = avoidCallbackAbort;
    static constexpr unsigned int recursing = 4;
    static constexpr unsigned int repairFlag = 8;
};
struct SeekWhat {
    static constexpr unsigned int read = 2;
    static constexpr unsigned int write = 4;
};

struct DBState {
    static inline constexpr unsigned int allOK = 0;
    static inline constexpr unsigned int uidsInconsistent = 2;
};

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
        if (fabs(fractPart) > std::numeric_limits<double>::epsilon()) {
            throw std::runtime_error("DB, with filepath: " + m_filePath
                + " is corrupt. The size is wrong");
        }
        const auto szR = sizeof(R);
        countType adj = static_cast<countType>(adjustedSize);
        const auto ret = adj / (countType)szR;
        return ret;
    }

    unsigned int state() const noexcept { return m_state; }

    auto seekToRecord(
        const countType where, unsigned int which = SeekWhat::read) {
        const auto start = sizeof(m_hdr);
        const auto avail = fileSize(m_filePath);
        const auto pos = start + (where * sizeof(R));
        if (which & SeekWhat::read) {
            m_f.seekg(pos);
            if (!m_f) {
                throw std::runtime_error("Unable to seek to record (for read) "
                    + std::to_string(which) + " " + m_filePath);
            }
        }
        if (which & SeekWhat::write) {
            m_f.seekp(pos);
            if (!m_f) {
                throw std::runtime_error("Unable to seek to record (for write) "
                    + std::to_string(which) + " " + m_filePath);
            }
        }
        return pos;
    }

    template <typename CB>
    void readAll(CB&& cb, uint32_t flags = ReadFlags::DEFAULT) {
        seekToRecord(0, SeekWhat::read);
        const auto count = rowCount();
        R r = {};
        m_uidIndex.resize(count);
        // we take a guess here, since if we failed with bad uids,
        // we need somewhere to create UIDs from!
        this->m_uidNext = count;
        std::fill(m_uidIndex.begin(), m_uidIndex.end(), INVALID_ROW);
        countType ctr = 0;
        auto highestUID = INVALID_UID;
        std::set<countType> uidCheck;

        while (ctr < count) {
            m_f.read((char*)&r, sizeof(R));
            if (!m_f) {
                throw std::runtime_error("Bad readAll as position: "
                    + std::to_string(ctr) + m_filePath);
            }
            if (r.uid > highestUID) highestUID = r.uid;
            assert(r.uid > INVALID_UID);
            if (r.uid - 1 >= m_uidIndex.size()) {
                const auto old_size = m_uidIndex.size();
                m_uidIndex.resize(r.uid);
                const auto new_size = m_uidIndex.size();
                std::fill(m_uidIndex.begin() + old_size, m_uidIndex.end(),
                    INVALID_ROW);
                m_state |= DBState::uidsInconsistent;
            }
            assert(r.uid - 1 < m_uidIndex.size());
            m_uidIndex[r.uid - 1] = ctr;
            //// checking for duplicate UID .. /////
            const auto found = uidCheck.find(r.uid);
            if (found != uidCheck.cend()) {
                if (flags & ReadFlags::recursing) {
                    throw std::runtime_error("Bad DB, uids are not unique. DB "
                                             "Repair failed for "
                        + m_filePath);
                } else {
                    if (flags & ReadFlags::repairFlag) {
                        this->UIDRepair();
                        return;
                    } else {
                        throw std::runtime_error(
                            "Bad DB, uids are not unique. Try "
                            "again with the repair flag set, for file: "
                            + m_filePath);
                    }
                }
            }
            uidCheck.insert(r.uid);
            //// ///////////////////////////////////
            ///
            if (flags & ReadFlags::avoidCallbackAbort) {
                cb(r);
            } else {
                if (cb(r) < 0) break;
            }
            ++ctr;
        }

#ifndef NDEBUG
        checkUIDSanity(highestUID);
#endif
    }

    void checkUIDSanity(const countType highestUID) {
        this->m_uidNext = highestUID;
        auto test = this->nextUID(true);
        assert(test == highestUID + 1);
        test = this->nextUID(true);
        assert(test == highestUID + 1);
        test = this->nextUID();
        assert(test == highestUID + 1);
        test = this->nextUID();
        assert(test == highestUID + 2);
        this->m_uidNext = highestUID; // put it back!
    }

    void reIndex() {
        seekToRecord(0, SeekWhat::read | SeekWhat::write);
        m_rowCount = calcRowCount();
        countType highestUID = 0;

        R r = {};
        // this pass to get highest UID
        countType c = 0;
        while (c < m_rowCount) {
            m_f.read((char*)&r, sizeof(R));
            if (!m_f) {
                throw std::runtime_error(
                    "Bad file in reIndex(). Likely file is corrupt: "
                    + m_filePath);
            }
            if (r.uid >= highestUID) highestUID = r.uid;
            ++c;
        }
        m_uidIndex.resize(highestUID + 1); // uids are 1-based.
        std::fill(m_uidIndex.begin(), m_uidIndex.end(), INVALID_ROW);

        c = 0;
        countType expectedUID = 0;
        seekToRecord(0, SeekWhat::read | SeekWhat::write);
        while (c < m_rowCount) {
            ++expectedUID;
            m_f.read((char*)&r, sizeof(R));
            if (!m_f) {
                throw std::runtime_error(
                    "Bad file in reIndex(). Likely file is corrupt: "
                    + m_filePath);
            }

            if (r.uid != expectedUID) {
                r.uid = expectedUID;
                seekToRecord(c, SeekWhat::write);
                m_f.write((char*)&r, sizeof(R));
                if (!m_f) {
                    throw std::runtime_error("Bad file in reIndex(), after "
                                             "write. Likely file is corrupt: "
                        + m_filePath);
                }
            }
            ++c;
        }
    }

    void UIDRepair() {

        const auto sz = sizeof(R);
        assert(sz == 672); // on Windows, at least
        reIndex();
        // careful here: likely uidIndex is not fully formed,
        // so don't use it.
        /*/
        auto recordPos = seekToRecord(0, SeekWhat::read | SeekWhat::write);

        std::set<countType> checker;
        R r = {};
        const auto rwCount = rowCount() ? rowCount() : calcRowCount();
        this->m_uidNext = rwCount; // guess as best we can
        countType curRow = 0;

        auto wtf = sizeof(m_hdr) + sizeof(R);
        std::cout << wtf << std::endl;

        while (curRow < rwCount) {
            const auto readPos = m_f.tellg();
            m_f.read((char*)&r, sizeof(r));
            if (!m_f) {
                perror("file is bad\n");
                throw std::runtime_error(
                    "UID repair: file is bad whilst reading record.");
            }
            ++curRow;
            recordPos += sizeof(R);
            const auto found = checker.find(r.uid);
            if (found != checker.cend()) {
                r.uid = nextUID();
                const auto myrecordpos = m_f.tellg()
                    - static_cast<decltype(m_f.tellg())>(sizeof(R));
                const auto compare = seekToRecord(curRow - 1, SeekWhat::write);
                const std::ptrdiff_t diff = compare - myrecordpos;
                assert(compare == myrecordpos);
                assert(myrecordpos == readPos);
                if (!m_f) {
                    perror("file is bad\n");
                    throw std::runtime_error(
                        "UID repair: file is bad after seekp.");
                }
                m_f.write((char*)&r, sizeof(r));

                if (!m_f) {
                    throw std::runtime_error(
                        "UID repair: file is bad after seekp.");
                }
            }
            checker.insert(r.uid);
        };
        m_f.flush();
        readAll([](const R&) { return 0; },
            ReadFlags::recursing | ReadFlags::avoidCallbackAbort);
/*/
        return;
    }
    std::vector<countType> m_uidIndex;
    std::string m_filePath;
    countType m_rowCount{0};
    countType m_uidNext{0};
    std::fstream m_f;
    unsigned int m_state = DBState::allOK;

    countType nextUID(bool peek = false) {
        if (!peek) {
            return ++m_uidNext;
        } else {
            const auto ret = m_uidNext + 1;
            return ret;
        }
    }

    R createNewRecord() {
        R ret{};
        ret.uid = nextUID();
        return ret;
    }

    public:
    template <typename CB>
    DB(const std::string& filePath, CB&& cb,
        unsigned int flags = ReadFlags::DEFAULT)
        : DB(filePath) {
        open(filePath);
        readAll(cb, flags);
        static_assert(std::is_base_of_v<RecordBase, R>);
        static_assert(std::is_trivial_v<R>);
    }

    void close() {
        if (m_f.is_open()) m_f.close();
        this->m_filePath.clear();
        this->m_rowCount = 0;
        this->m_uidIndex.clear();
        this->m_uidNext = 0;
    }

    const std::string& filePath() const noexcept { return this->m_filePath; }

    countType rowIndexFromUID(countType uid) {
        if (uid == 0) {
            throw std::runtime_error("uid0 is not a valid uid");
        }
        const auto idxsz = m_uidIndex.size();
        const auto nRows = this->rowCount();
        (void)nRows;
        const auto key = uid - 1;
        if (key >= idxsz) {
            throw std::runtime_error("rowIndexFromUID: out of range uid");
        }
        return m_uidIndex[key];
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
    // This will add new records all the while your callback returns true
    template <typename CB> DBWriter(DB& db, CB&& cb) : m_db(db) {

        auto& f = db.m_f;
        bool ok = true;
        f.seekp(0, std::ios::end);
        oldRowCount = m_db.rowCount();
        newRowCount = oldRowCount;

        RecordType r = {};
        while (ok) {
            r.uid = m_db.nextUID(true);

            if (cb(r)) {
                f.write((char*)&r, sizeof(RecordType));
                if (!f) {
                    if (newRowCount != oldRowCount) {
                        m_db.writeHeader(newRowCount);
                    }
                    ok = false;
                    throw std::runtime_error("DBWriter: file is bad");
                }
                ++newRowCount;
                r.uid = m_db.nextUID(); // I know it doesn't do anything useful
                                        // to the record here, but we should
                                        // only increment on succerss

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
            m_db.readAll([](auto&) { return 0; }, ReadFlags::DEFAULT);
        }
    }

    ~DBWriter() { finish(); }
};

namespace tests {
    // break a db by buggering up uids when writing
    std::string serr;
    template <typename DB> static inline void breakKnownGoodDB(DB& db) {
        countType broken = 0;
        bool thrown = false;
        try {
            // this will throw as it encounters fucked-up uids when
            // ir re-indexes after saving.
            db2023::DBWriter myWriter(db, [&](typename DB::RecordType& r) {
                if (r.uid % 10 == 0) {
                    r.uid = 10;
                    ++broken;
                }

                if (broken > 2) return false;
                return true;
            });
        } catch (const std::exception& e) {
            std::cerr << "GOOD! Threw " << e.what() << ", as expected";
            thrown = true;
        }

        assert(thrown);
    }

    template <typename DB> static inline void testRepair(DB& db) {
        using R = typename DB::RecordType;
        db2023::tests::breakKnownGoodDB(db);
        const auto filePath = db.filePath();
        db.close();

        bool threw = false;
        try {
            db2023::DB<R> badDB(filePath, [](const R&) { return 0; });

        } catch (const std::exception& e) {
            std::cerr << "Good. expected this exception: " << e.what()
                      << std::endl;
            serr = e.what();
            threw = true;
        }
        assert(threw);
        threw = false;

        try {
            db2023::DB<R> repairedDB(
                filePath, [](const R&) { return 0; },
                db2023::ReadFlags::repairFlag);

        } catch (const std::exception& e) {
            std::cerr << "BAD! did not expect this exception: " << e.what()
                      << std::endl;
            serr = e.what();
            threw = true;
        }
        assert(!threw);
        if (!threw) {
            std::cout << "OK, repaired!" << std::endl;
        }
    }

} // namespace tests

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

    uint8_t reserved;
};

int main() {
    using std::cerr;
    using std::cout;
    using std::endl;

    bool threw = false;

    std::string filePath("test.db");
    // This one should not throw (though it may be empty)
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
    const auto rwIndex = DB.rowIndexFromUID(myCount);
    assert(rwIndex == myCount - 1);
    cout << "There are now " << myCount << " rows in the db." << endl;

    db2023::tests::testRepair(DB);

    return 0;
}
