# distutils: language = c++
from libcpp cimport bool
from libcpp.vector cimport vector

cdef extern from *:
    """
    #ifndef PRICING_CLASSES_H
    #define PRICING_CLASSES_H

    #include <unordered_map>
    #include <stdexcept>

    typedef long long i64;
    typedef short int bidflags;
    const bidflags CALCULATED_FLAG = 1;
    const bidflags PRICEABLE_FLAG = 2;
    const bidflags TRADABLE_FLAG = 4;


    class Account {
    public:
      i64 creditor_id;
      i64 debtor_id;

      Account(i64 creditor_id, i64 debtor_id) {
        this->creditor_id = creditor_id;
        this->debtor_id = debtor_id;
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
    };

    namespace std {
      template <> struct hash<Account>
      {
        inline size_t operator()(const Account& account) const
        {
          size_t h = hash<i64>()(account.creditor_id);
          size_t k = hash<i64>()(account.debtor_id);
          h ^= k + 0x9e3779b9 + (h << 6) + (h >> 2);  // "add" k to h
          return h;
        }
      };
    }


    class Bid {
    private:
      bidflags flags = 0;
      i64 peg_id;

      bool calc_priceable_flag() {
        if (flags & CALCULATED_FLAG) {
          return (flags & PRICEABLE_FLAG) != 0;
        }
        flags = CALCULATED_FLAG;
        if (peg_ptr == NULL || !peg_ptr->calc_priceable_flag()) {
          return false;
        }
        flags = CALCULATED_FLAG | PRICEABLE_FLAG;
        return true;
      }

    public:
      i64 creditor_id;
      i64 debtor_id;
      i64 amount;
      Bid* peg_ptr = NULL;
      float peg_exchange_rate;

      Bid(
        i64 creditor_id,
        i64 debtor_id,
        i64 amount,
        i64 peg_id,
        float peg_exchange_rate
      ) {
        this->peg_id = peg_id;
        this->creditor_id = creditor_id;
        this->debtor_id = debtor_id;
        this->amount = amount;
        this->peg_exchange_rate = peg_exchange_rate;
      }
      bool priceable() {
        return (flags & PRICEABLE_FLAG) != 0;
      }
      bool tradable() {
        return (flags & TRADABLE_FLAG) != 0;
      }
      void set_tradable() {
        flags |= TRADABLE_FLAG;
      }

      friend class BidRegistry;
    };


    class BidRegistry {
    private:
      std::unordered_map<Account, Bid*> map;
      std::unordered_map<Account, Bid*>::const_iterator start, stop;
      bool iteration_started = false;

      void start_iteration() {
        for (auto pair = map.begin(); pair != map.end(); ++pair) {
          Bid* bid_ptr = pair->second;
          if (bid_ptr->debtor_id == base_debtor_id) {
            bid_ptr->flags = CALCULATED_FLAG | PRICEABLE_FLAG;
          }
          Account peg_account(bid_ptr->creditor_id, bid_ptr->peg_id);
          try {
            bid_ptr->peg_ptr = map.at(peg_account);
          } catch (const std::out_of_range& oor) {
            bid_ptr->peg_ptr = NULL;
          }
        }
        for (auto pair = map.begin(); pair != map.end(); ++pair) {
          pair->second->calc_priceable_flag();
        }
        start = map.cbegin();
        stop = map.cend();
        iteration_started = true;
      }

    public:
      const i64 base_debtor_id;

      BidRegistry(i64 base_debtor_id): base_debtor_id(base_debtor_id) {
      }
      ~BidRegistry() {
        for (auto pair = map.begin(); pair != map.end(); ++pair) {
          delete pair->second;
        }
      }
      Bid* add_bid(
        i64 creditor_id,
        i64 debtor_id,
        i64 amount,
        i64 peg_id,
        float peg_exchange_rate
      ) {
        if (iteration_started) {
          throw std::runtime_error("add_bid called after iteration stared");
        }
        if (debtor_id == 0) {
          // We use debtor ID `0` to indicate a missing peg.
          return NULL;
        }
        Account account(creditor_id, debtor_id);
        return map[account] = new Bid(
          creditor_id, debtor_id, amount, peg_id, peg_exchange_rate
        );
      }
      Bid* get_priceable_bid() {
        if (!iteration_started) {
          start_iteration();
        }
        while (start != stop) {
          Bid* bid = start->second;
          ++start;
          if (bid->priceable()) {
            return bid;
          }
        }
        return NULL;
      }
    };

    #endif
    """
    ctypedef long long i64

    cdef cppclass Bid:
        i64 creditor_id
        i64 debtor_id
        i64 amount
        Bid* peg_ptr
        float peg_exchange_rate
        Bid(i64, i64, i64, i64, float) except +
        bool priceable() noexcept
        bool tradable() noexcept
        void set_tradable() noexcept

    cdef cppclass BidRegistry:
        const i64 base_debtor_id
        BidRegistry(i64) except +
        Bid* add_bid(i64, i64, i64, i64, float) except +
        Bid* get_priceable_bid() noexcept
