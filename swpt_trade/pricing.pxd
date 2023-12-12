# distutils: language = c++
from libcpp cimport bool
from libcpp.unordered_set cimport unordered_set

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
    const bidflags CONFIRMED_FLAG = 1 << 6;

    const unsigned short INFINITE_DISTANCE = 0xffff;


    class Key128 {
    public:
      i64 first;
      i64 second;

      Key128(i64 first, i64 second)
        : first(first), second(second) {
      }
      Key128(const Key128& other) {
        first = other.first;
        second = other.second;
      }
      bool operator== (const Key128& other) const {
        return first == other.first && second == other.second;
      }
    };

    namespace std {
      template <> struct hash<Key128>
      {
        inline size_t operator()(const Key128& key) const
        {
          // Combine first and second to calculate a hash value.
          size_t h = hash<i64>()(key.first);
          size_t k = hash<i64>()(key.second);
          h ^= k + 0x9e3779b9 + (h << 6) + (h >> 2);
          return h;
        }
      };
    }


    class Peg {
    private:
      bidflags flags = 0;
      unsigned short distance_to_base = INFINITE_DISTANCE;
      float price = NAN;
      const Key128 peg_debtor_key;
      const i64 peg_debtor_id;

      bool priceable() {
        return (flags & PRICEABLE_FLAG) != 0;
      }
      void set_anchor() {
        flags |= ANCHOR_FLAG;
      }
      void set_confirmed() {
        flags |= CONFIRMED_FLAG;
      }

    public:
      const i64 debtor_id;
      const float peg_exchange_rate;
      Peg* peg_ptr = NULL;

      Peg(
        i64 debtor_id,
        Key128 peg_debtor_key,
        i64 peg_debtor_id,
        float peg_exchange_rate
      ) : peg_debtor_key(peg_debtor_key),
          peg_debtor_id(peg_debtor_id),
          debtor_id(debtor_id),
          peg_exchange_rate(peg_exchange_rate) {
      }
      bool anchor() {
        return (flags & ANCHOR_FLAG) != 0;
      }
      bool confirmed() {
        return (flags & CONFIRMED_FLAG) != 0;
      }
      bool tradable() {
        return confirmed() && priceable();
      }

      friend class PegRegistry;
    };


    class PegRegistry {
    private:
      std::unordered_map<Key128, Peg*> pegs;
      std::unordered_map<i64, Peg*> tradables;
      bool prepared_for_queries = false;

      unsigned short calc_distance_to_base(Peg* peg) {
        if (peg != NULL) {
          if (peg->flags & PRICEABILITY_DECIDED_FLAG) {
            return peg->distance_to_base;
          }
          peg->flags |= PRICEABILITY_DECIDED_FLAG;
          Peg* parent = peg->peg_ptr;
          unsigned short dist;
          if ((dist = calc_distance_to_base(parent)) < max_distance_to_base) {
            peg->distance_to_base = dist + 1;
            peg->price = parent->price * peg->peg_exchange_rate;
            peg->flags |= PRICEABLE_FLAG;
            return peg->distance_to_base;
          }
        }
        return INFINITE_DISTANCE;
      }

    public:
      const Key128 base_debtor_key;
      const i64 base_debtor_id;
      const unsigned short max_distance_to_base;

      PegRegistry(
        Key128 base_debtor_key,
        i64 base_debtor_id,
        unsigned short max_distance_to_base
      ) : base_debtor_key(base_debtor_key),
          base_debtor_id(base_debtor_id),
          max_distance_to_base(max_distance_to_base) {
        if (base_debtor_id == 0) {
          throw std::runtime_error("invalid base_debtor_id");
        }
        if (max_distance_to_base == INFINITE_DISTANCE) {
          throw std::runtime_error("invalid max_distance_to_base");
        }
      }
      ~PegRegistry() {
        for (auto pair = pegs.begin(); pair != pegs.end(); ++pair) {
          delete pair->second;
        }
      }
      void add_currency(
        Key128 debtor_key,
        i64 debtor_id,
        Key128 peg_debtor_key,
        i64 peg_debtor_id,
        float peg_exchange_rate,
        bool confirmed
      ) {
        if (prepared_for_queries) {
          throw std::runtime_error(
            "add_currency called after query preparation"
          );
        }
        if (debtor_id == 0) {
          // Currencies claiming debtor ID `0` are excluded from the graph.
          if (confirmed) {
            throw std::runtime_error("invalid confirmed debtor_id");
          }
          return;
        }
        Peg*& peg_ptr_ref = pegs[debtor_key];
        if (peg_ptr_ref != NULL) {
          throw std::runtime_error("duplicated debtor_key");
        }
        peg_ptr_ref = new Peg(
          debtor_id,
          peg_debtor_key,
          peg_debtor_id,
          peg_exchange_rate
        );
        if (debtor_key == base_debtor_key && debtor_id == base_debtor_id) {
          peg_ptr_ref->flags = (
            PRICEABILITY_DECIDED_FLAG
            | PRICEABLE_FLAG
            | ANCHOR_FLAG
          );
          peg_ptr_ref->distance_to_base = 0;
          peg_ptr_ref->price = 1.0;
        }
        if (confirmed) {
          peg_ptr_ref->set_confirmed();
        }
      }
      void prepare_for_queries() {
        if (pegs.count(base_debtor_key) == 0) {
          add_currency(
            base_debtor_key, base_debtor_id,
            Key128(0, 0), 0,
            0.0, false
          );
        }
        for (auto pair = pegs.begin(); pair != pegs.end(); ++pair) {
          Peg* peg_ptr = pair->second;
          try {
            Peg* parent_peg_ptr = pegs.at(peg_ptr->peg_debtor_key);
            peg_ptr->peg_ptr = (
              (parent_peg_ptr->debtor_id == peg_ptr->peg_debtor_id)
              ? parent_peg_ptr : NULL
            );
          } catch (const std::out_of_range& oor) {
            peg_ptr->peg_ptr = NULL;
          }
        }
        tradables.clear();
        for (auto pair = pegs.begin(); pair != pegs.end(); ++pair) {
          Peg* peg_ptr = pair->second;
          calc_distance_to_base(peg_ptr);
          if (peg_ptr->tradable()) {
            Peg*& tradable_ptr_ref = tradables[peg_ptr->debtor_id];
            if (tradable_ptr_ref != NULL) {
              throw std::runtime_error("duplicated anchor debtor_id");
            }
            peg_ptr->set_anchor();
            tradable_ptr_ref = peg_ptr;
          }
        }
        if (
          pegs.at(base_debtor_key)->anchor()
          && !pegs.at(base_debtor_key)->tradable()
          && tradables.count(base_debtor_id) != 0
        ) {
          throw std::runtime_error("duplicated anchor debtor_id");
        }
        prepared_for_queries = true;
      }
    };


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
      std::unordered_map<Key128, Bid*> bids;
      std::unordered_map<Key128, Bid*>::const_iterator iter_curr, iter_stop;
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
        for (auto pair = bids.begin(); pair != bids.end(); ++pair) {
          Bid* bid_ptr = pair->second;
          if (bid_ptr->debtor_id == base_debtor_id) {
            bid_ptr->flags |= PRICEABILITY_DECIDED_FLAG | PRICEABLE_FLAG;
          }
          try {
            bid_ptr->peg_ptr = bids.at(
              Key128(bid_ptr->creditor_id, bid_ptr->peg_debtor_id)
            );
          } catch (const std::out_of_range& oor) {
            bid_ptr->peg_ptr = NULL;
          }
        }
        for (auto pair = bids.begin(); pair != bids.end(); ++pair) {
          decide_priceability(pair->second);
        }
      }

    public:
      const i64 base_debtor_id;

      BidRegistry(i64 base_debtor_id)
        : base_debtor_id(base_debtor_id) {
      }
      ~BidRegistry() {
        for (auto pair = bids.begin(); pair != bids.end(); ++pair) {
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
          Bid*& bid_ptr_ref = bids[Key128(creditor_id, debtor_id)];
          if (bid_ptr_ref != NULL) {
            throw std::runtime_error("duplicated bid");
          }
          bid_ptr_ref = new Bid(
            creditor_id, debtor_id, amount, peg_debtor_id, peg_exchange_rate
          );
        }
      }
      Bid* get_priceable_bid() {
        if (!iter_started) {
          prepare_for_iteration();
          iter_curr = bids.cbegin();
          iter_stop = bids.cend();
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

    cdef cppclass Key128:
        """An 128-bit opaque hashable identifier.
        """
        const i64 first
        const i64 second


    cdef cppclass Peg:
        """Tells to which other currency a given currency is pegged.

        Pegs are organized in tree-like structures, each currency
        pointing to its peg currency (see the `peg_ptr` field). In
        addition to this, every bid maintains several flags bits,
        which are used during the traversal of the peg-tree.
        """
        const i64 debtor_id
        const float peg_exchange_rate
        Peg* const peg_ptr
        Peg(i64, Key128, i64, float) except +
        bool anchor() noexcept
        bool confirmed() noexcept
        bool tradable() noexcept


    cdef cppclass PegRegistry:
        """Given a set of currencies, generates the peg-tree.

        At the root of the peg tree is the "base currency" (determined
        by the `base_debtor_key` and `base_debtor_id` fields).
        Currencies that are directly or indirectly pegged to the base
        currency, are considered "priceable", and will be included in
        the generated tree. Currencies that are not priceable, will be
        excluded from the tree.

        The `prepare_for_queries` method must be called before
        queering the registry, after all currencies have been added to
        the registry. (A currency can be added to the registry by
        calling the `add_currency` method.)
        """
        const i64 base_debtor_key
        const i64 base_debtor_id
        const unsigned short max_distance_to_base
        PegRegistry(Key128, i64, unsigned short) except +
        void add_currency(Key128, i64, Key128, i64, float, bool) except +
        void prepare_for_queries() except +


    cdef cppclass Bid:
        """Tells the disposition of a given trader to a given currency.

        The `amount` field can be negative (the trader wants to sell),
        positive (the trader wants to buy), or zero (the trader do not
        want to trade). Bids with zero amounts must also be processed,
        because they may declare an exchange rate to another currency
        (the bid's "peg currency"), which have been approved by the
        trader.

        Bids are organized in tree-like structures, each bid pointing
        to the bid for its peg currency (see the `peg_ptr` field). In
        addition to this, every bid maintains several flags bits,
        which are used during the traversal of the bid-tree.
        """
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
        """Given a set of `Bid`s, generates a tree of priceable bids.

        At the root of the tree is the "base currency" (determined by
        the `base_debtor_id` field). Bids whose currencies are
        directly or indirectly pegged to the base currency, are
        considered "priceable", and will be included in the generated
        tree. Bids that are not priceable, will be excluded from the
        tree.
        """
        const i64 base_debtor_id
        BidRegistry(i64) except +
        void add_bid(i64, i64, i64, i64, float) except +
        Bid* get_priceable_bid() noexcept


cdef class CandidateOffer:
    """A trader bid, that may eventually become a confirmed offer.

    The `amount` field can be negative (the trader wants to sell), or
    positive (the trader wants to buy). The amount can not be zero.
    """
    cdef readonly i64 amount
    cdef readonly i64 debtor_id
    cdef readonly i64 creditor_id


cdef class BidProcessor:
    cdef i64 base_debtor_id
    cdef i64 min_trade_amount
    cdef BidRegistry* bid_registry_ptr
    cdef object candidate_offers
    cdef unordered_set[i64] buyers
    cdef unordered_set[i64] sellers
    cdef bool _check_if_tradable(self, Bid*) noexcept
    cdef (i64, float) _calc_endorsed_peg(self, i64) noexcept
    cdef void _add_candidate_offer(self, Bid*)
    cdef (i64, float) _calc_anchored_peg(self, Bid*) noexcept
    cdef bool _validate_peg(self, Bid*) noexcept
    cdef void _process_bid(self, Bid*) noexcept
