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
    const bidflags PRICEABILITY_DECIDED_FLAG = 1 << 0;
    const bidflags PRICEABLE_FLAG = 1 << 1;
    const bidflags PROCESSED_FLAG = 1 << 3;
    const bidflags DEADEND_FLAG = 1 << 4;
    const bidflags ANCHOR_FLAG = 1 << 5;

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
      const i64 peg_debtor_id;

      bool priceable() {
        return (flags & PRICEABLE_FLAG) != 0;
      }

    public:
      const i64 creditor_id;
      const i64 debtor_id;
      const i64 amount;
      Bid* peg_ptr = NULL;
      const float peg_exchange_rate;

      Bid(
        i64 creditor_id,
        i64 debtor_id,
        i64 amount,
        i64 peg_debtor_id,
        float peg_exchange_rate
      ) : peg_debtor_id(peg_debtor_id),
          creditor_id(creditor_id),
          debtor_id(debtor_id),
          amount(amount),
          peg_exchange_rate(peg_exchange_rate) {
      }
      bool processed() {
        return (flags & PROCESSED_FLAG) != 0;
      }
      bool deadend() {
        return (flags & DEADEND_FLAG) != 0;
      }
      bool anchor() {
        return (flags & ANCHOR_FLAG) != 0;
      }
      void set_processed() {
        flags |= PROCESSED_FLAG;
      }
      void set_deadend() {
        flags |= DEADEND_FLAG;
      }
      void set_anchor() {
        flags |= ANCHOR_FLAG;
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
          if (bid->flags & PRICEABILITY_DECIDED_FLAG) {
            return bid->priceable();
          }
          bid->flags |= PRICEABILITY_DECIDED_FLAG;
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
            bid_ptr->flags |= PRICEABILITY_DECIDED_FLAG | PRICEABLE_FLAG;
          }
          // Try to find the bid relative to which the current bid must
          // be priced.
          try {
            bid_ptr->peg_ptr = map.at(
              Account(bid_ptr->creditor_id, bid_ptr->peg_debtor_id)
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
        Bid* const peg_ptr
        const float peg_exchange_rate
        Bid(i64, i64, i64, i64, float) except +
        bool processed() noexcept
        void set_processed() noexcept
        bool deadend() noexcept
        void set_deadend() noexcept
        bool anchor() noexcept
        void set_anchor() noexcept

    cdef cppclass BidRegistry:
        const i64 base_debtor_id
        BidRegistry(i64) except +
        void add_bid(i64, i64, i64, i64, float) except +
        Bid* get_priceable_bid() noexcept


cdef class CandidateOffer:
    cdef readonly i64 amount
    cdef readonly i64 debtor_id
    cdef readonly i64 creditor_id


cdef class BidProcessor:
    cdef i64 base_debtor_id
    cdef i64 min_trade_amount
    cdef BidRegistry* bid_registry_ptr
    cdef object candidate_offers
    cdef bool _check_if_tradable(self, i64 debtor_id) noexcept
    cdef (i64, float) _calc_endorsed_peg(self,i64 debtor_id) noexcept
    cdef void _register_tradable_bid(self,Bid* bid)
    cdef (i64, float) _calc_anchored_peg(self, Bid* bid) noexcept
    cdef bool _validate_peg(self, Bid* bid) noexcept
    cdef void _process_bid(self, Bid* bid) noexcept

