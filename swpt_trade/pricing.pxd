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
    typedef unsigned char bidflags;
    const bidflags DECIDED_FLAG = 1;
    const bidflags PRICEABLE_FLAG = 2;
    const bidflags TRADABLE_FLAG = 4;
    const bidflags VISITED_FLAG = 8;

    class Account {
    public:
      i64 creditor_id;
      i64 debtor_id;

      Account(i64 creditor_id, i64 debtor_id)
        : creditor_id(creditor_id), debtor_id(debtor_id) {
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
          // Combine creditor_id and debtor_id to calculate a hash value.
          size_t h = hash<i64>()(account.creditor_id);
          size_t k = hash<i64>()(account.debtor_id);
          h ^= k + 0x9e3779b9 + (h << 6) + (h >> 2);
          return h;
        }
      };
    }


    class Bid {
    private:
      bidflags flags = 0;

    public:
      const i64 creditor_id;
      const i64 debtor_id;
      const i64 amount;
      i64 anchor_id;
      Bid* peg_ptr = NULL;
      const float peg_exchange_rate;

      Bid(
        i64 creditor_id,
        i64 debtor_id,
        i64 amount,
        i64 peg_debtor_id,
        float peg_exchange_rate
      ) : creditor_id(creditor_id),
          debtor_id(debtor_id),
          amount(amount),
          anchor_id(peg_debtor_id),
          peg_exchange_rate(peg_exchange_rate) {
      }
      bool priceable() {
        return (flags & PRICEABLE_FLAG) != 0;
      }
      bool visited() {
        return (flags & VISITED_FLAG) != 0;
      }
      bool tradable() {
        return (flags & TRADABLE_FLAG) != 0;
      }
      void set_visited() {
        flags |= VISITED_FLAG;
      }
      void set_tradable() {
        flags |= TRADABLE_FLAG;
      }

      friend class BidRegistry;
    };


    class BidRegistry {
    private:
      std::unordered_map<Account, Bid*> map;
      std::unordered_map<Account, Bid*>::const_iterator iter_curr, iter_stop;
      bool iter_started = false;

      static bool decide_priceability(Bid* bid) {
        if (bid != NULL) {
          if (bid->flags & DECIDED_FLAG) {
            return bid->priceable();
          }
          bid->flags |= DECIDED_FLAG;
          if (decide_priceability(bid->peg_ptr)) {
            bid->flags |= PRICEABLE_FLAG;
            return true;
          }
        }
        return false;
      }

      void prepare_for_iteration() {
        for (auto pair = map.begin(); pair != map.end(); ++pair) {
          Bid* bid_ptr = pair->second;  // the current bid
          // If the current bid is for the base currency, it is priceable
          // by definition.
          if (bid_ptr->debtor_id == base_debtor_id) {
            bid_ptr->flags |= DECIDED_FLAG | PRICEABLE_FLAG;
          }
          // Try to find the bid relative to which the current bid must
          // be priced.
          try {
            bid_ptr->peg_ptr = map.at(
              Account(bid_ptr->creditor_id, bid_ptr->anchor_id)
            );
          } catch (const std::out_of_range& oor) {
            bid_ptr->peg_ptr = NULL;
          }
        }
        for (auto pair = map.begin(); pair != map.end(); ++pair) {
          // Starting from the current bid, traverse the peg graph, trying to
          // reach the base currency (which is priceable by definition).
          decide_priceability(pair->second);
        }
      }

    public:
      const i64 base_debtor_id;

      BidRegistry(i64 base_debtor_id)
        : base_debtor_id(base_debtor_id) {
      }
      ~BidRegistry() {
        for (auto pair = map.begin(); pair != map.end(); ++pair) {
          delete pair->second;
        }
      }
      void add_bid(
        i64 creditor_id,
        i64 debtor_id,
        i64 amount,
        i64 peg_debtor_id,
        float peg_exchange_rate
      ) {
        if (iter_started) {
          throw std::runtime_error("add_bid called after iteration stared");
        }
        // Because we use debtor ID `0` to indicate a missing peg, we must
        // ignore bids for it. This should be safe, because debtor ID `0`
        // is declared as reserved in the spec.
        if (debtor_id != 0) {
          map[Account(creditor_id, debtor_id)] = new Bid(
            creditor_id, debtor_id, amount, peg_debtor_id, peg_exchange_rate
          );
        }
      }
      Bid* get_priceable_bid() {
        if (!iter_started) {
          prepare_for_iteration();
          iter_curr = map.cbegin();
          iter_stop = map.cend();
          iter_started = true;
        }
        while (iter_curr != iter_stop) {
          Bid* bid = iter_curr->second;
          ++iter_curr;
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
        const i64 creditor_id
        const i64 debtor_id
        const i64 amount
        i64 anchor_id
        Bid* const peg_ptr
        const float peg_exchange_rate
        Bid(i64, i64, i64, i64, float) except +
        bool priceable() noexcept
        bool visited() noexcept
        void set_visited() noexcept
        bool tradable() noexcept
        void set_tradable() noexcept

    cdef cppclass BidRegistry:
        const i64 base_debtor_id
        BidRegistry(i64) except +
        void add_bid(i64, i64, i64, i64, float) except +
        Bid* get_priceable_bid() noexcept
