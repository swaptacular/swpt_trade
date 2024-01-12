# distutils: language = c++

cdef extern from *:
    """
    #ifndef AGGREGATION_CLASSES_H
    #define AGGREGATION_CLASSES_H

    #include <unordered_map>
    #include <stdexcept>

    typedef long long i64;

    class Account {
    public:
      i64 creditor_id;
      i64 debtor_id;

      Account()
        : creditor_id(0), debtor_id(0) {
      }
      Account(i64 creditor_id, i64 debtor_id)
        : creditor_id(creditor_id), debtor_id(creditor_id) {
      }
      Account(const Account& other) {
        creditor_id = other.creditor_id;
        debtor_id = other.debtor_id;
      }
      bool operator== (const Account& other) const {
        return (
          creditor_id == other.creditor_id
          && debtor_id == other.debtor_id
        );
      }
      size_t calc_hash();
    };

    namespace std {
      template <> struct hash<Account>
      {
        inline size_t operator()(const Account& account) const
        {
          // Combine creditor_id and debtor_id to calculate a hash value.
          size_t h = hash<i64>()(account.creditor_id);
          size_t k = hash<i64>()(account.debtor_id);
          h ^= k + 0x9e3779b9 + (h << 6) + (h >> 2);
          return h;
        }
      };
    }

    inline size_t Account::calc_hash() {
      return std::hash<Account>()(*this);
    }

    class AccountData {
    public:
      i64 amount_change;
      i64 collector_id;

      AccountData()
        : amount_change(0), collector_id(0) {
      }
      AccountData(i64 amount_change, i64 collector_id)
        : amount_change(amount_change), collector_id(collector_id) {
      }
    };

    #endif
    """
    ctypedef long long i64

    cdef cppclass Account:
        """Uniquely indetifies an account.
        """
        const i64 creditor_id
        const i64 debtor_id
        Account() noexcept
        Account(i64, i64) noexcept
        size_t calc_hash() noexcept

    cdef cppclass AccountData:
        """Uniquely indetifies an account.
        """
        i64 amount_change
        i64 collector_id
        AccountData() noexcept
        AccountData(i64, i64) noexcept
