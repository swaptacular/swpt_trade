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
    typedef unsigned short bitflags;
    typedef unsigned short distance;

    const bitflags PRICEABILITY_DECIDED_FLAG = 1 << 0;
    const bitflags PRICEABLE_FLAG = 1 << 1;
    const bitflags CONFIRMED_FLAG = 1 << 2;

    const distance INFINITE_DISTANCE = 0xffff;
    const float EPSILON = 1e-5;


    inline bool compare_prices(float price1, float price2) {
       return (price2 != 0.0) ? abs(1.0 - price1 / price2) < EPSILON : false;
    }

    class Key128 {
    public:
      i64 first;
      i64 second;

      Key128()
        : first(0), second(0) {
      }
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
      size_t calc_hash();
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

    inline size_t Key128::calc_hash() {
      return std::hash<Key128>()(*this);
    }


    class Currency {
    private:
      const Key128 peg_debtor_key;
      const i64 peg_debtor_id;
      bitflags flags = 0;
      distance distance_to_base = INFINITE_DISTANCE;

      bool priceable() {
        return (flags & PRICEABLE_FLAG) != 0;
      }
      void set_confirmed() {
        flags |= CONFIRMED_FLAG;
      }
      void mark_as_base() {
        flags |= PRICEABILITY_DECIDED_FLAG | PRICEABLE_FLAG;
        distance_to_base = 0;
        price = 1.0;
      }

    public:
      float price = NAN;
      Currency* peg_ptr = NULL;
      const i64 debtor_id;
      const float peg_exchange_rate;

      Currency(
        i64 debtor_id,
        Key128 peg_debtor_key,
        i64 peg_debtor_id,
        float peg_exchange_rate
      ) : peg_debtor_key(peg_debtor_key),
          peg_debtor_id(peg_debtor_id),
          debtor_id(debtor_id),
          peg_exchange_rate(peg_exchange_rate) {
      }
      bool confirmed() {
        return (flags & CONFIRMED_FLAG) != 0;
      }
      bool tradable() {
        return confirmed() && priceable();
      }

      friend class CurrencyRegistry;
    };


    class CurrencyRegistry {
    private:
      std::unordered_map<Key128, Currency*> currencies;
      std::unordered_map<i64, Currency*> tradables;
      bool prepared_for_queries = false;

      distance calc_distance_to_base(Currency* currency) {
        if (currency != NULL) {
          if (currency->flags & PRICEABILITY_DECIDED_FLAG) {
            return currency->distance_to_base;
          }
          currency->flags |= PRICEABILITY_DECIDED_FLAG;
          Currency* peg = currency->peg_ptr;
          distance dist;
          if ((dist = calc_distance_to_base(peg)) < max_distance_to_base) {
            currency->distance_to_base = dist + 1;
            currency->price = peg->price * currency->peg_exchange_rate;
            currency->flags |= PRICEABLE_FLAG;
            return currency->distance_to_base;
          }
        }
        return INFINITE_DISTANCE;
      }
      void set_pointers() {
        for (auto it = currencies.begin(); it != currencies.end(); ++it) {
          Currency* currency = it->second;
          try {
            Currency* peg = currencies.at(currency->peg_debtor_key);
            currency->peg_ptr = (
              peg->debtor_id == currency->peg_debtor_id ? peg : NULL
            );
          } catch (const std::out_of_range& oor) {
            currency->peg_ptr = NULL;
          }
        }
      }
      void find_tradables() {
        for (auto it = currencies.begin(); it != currencies.end(); ++it) {
          Currency* currency = it->second;
          calc_distance_to_base(currency);
          if (currency->tradable()) {
            Currency*& tradable_ptr_ref = tradables[currency->debtor_id];
            if (tradable_ptr_ref != NULL) {
              throw std::runtime_error("duplicated tradable debtor_id");
            }
            tradable_ptr_ref = currency;
          }
        }
      }
      void validate_base_currency() {
        Currency* base_currency;
        try {
          base_currency = currencies.at(base_debtor_key);
        } catch (const std::out_of_range& oor) {
          return;
        }
        if (
          base_currency->debtor_id != base_debtor_id
          || (
            !base_currency->tradable()
            && tradables.count(base_debtor_id) != 0
          )
        ) {
          throw std::runtime_error(
            "inconsistent base_debtor_key and base_debtor_id"
          );
        }
      }

    public:
      const Key128 base_debtor_key;
      const i64 base_debtor_id;
      const distance max_distance_to_base;

      CurrencyRegistry(
        Key128 base_debtor_key,
        i64 base_debtor_id,
        distance max_distance_to_base
      ) : base_debtor_key(base_debtor_key),
          base_debtor_id(base_debtor_id),
          max_distance_to_base(max_distance_to_base) {
        if (base_debtor_id == 0) {
          throw std::runtime_error("invalid base_debtor_id");
        }
      }
      ~CurrencyRegistry() {
        for (auto it = currencies.begin(); it != currencies.end(); ++it) {
          delete it->second;
        }
      }
      void add_currency(
        bool confirmed,
        Key128 debtor_key,
        i64 debtor_id,
        Key128 peg_debtor_key,
        i64 peg_debtor_id,
        float peg_exchange_rate
      ) {
        if (prepared_for_queries) {
          throw std::runtime_error(
            "add_currency called after query preparation"
          );
        }
        // Currencies claiming debtor ID `0` are excluded from the graph.
        if (debtor_id != 0) {
          Currency*& currency_ptr_ref = currencies[debtor_key];
          if (currency_ptr_ref != NULL) {
            throw std::runtime_error("duplicated debtor_key");
          }
          currency_ptr_ref = new Currency(
            debtor_id, peg_debtor_key, peg_debtor_id, peg_exchange_rate
          );
          if (debtor_key == base_debtor_key && debtor_id == base_debtor_id) {
            currency_ptr_ref->mark_as_base();
          }
          if (confirmed) {
            currency_ptr_ref->set_confirmed();
          }
        }
      }
      void prepare_for_queries() {
        if (!prepared_for_queries) {
          set_pointers();
          find_tradables();
          validate_base_currency();
          prepared_for_queries = true;
        }
      }
      Currency* get_tradable_currency(i64 debtor_id) {
        if (!prepared_for_queries) {
          throw std::runtime_error("issued query before query preparation");
        }
        try {
          return tradables.at(debtor_id);
        } catch (const std::out_of_range& oor) {
          return NULL;
        }
      }
      float get_currency_price(i64 debtor_id) {
        Currency* currency = get_tradable_currency(debtor_id);
        return (currency == NULL) ? NAN : currency->price;
      }
    };


    class Bid {
    private:
      // The `data` field holds the debtor ID of the peg currency, but
      // only until the `peg_ptr` field has been initialized. After
      // that, the `data` field holds various bit-flags.
      i64 data;

      bool priceable() {
        return (data & PRICEABLE_FLAG) != 0;
      }

    public:
      Bid* peg_ptr = NULL;
      float currency_price = NAN;
      const float peg_exchange_rate;
      const i64 creditor_id;
      const i64 debtor_id;
      const i64 amount;

      Bid(
        i64 creditor_id,
        i64 debtor_id,
        i64 amount,
        i64 peg_debtor_id,
        float peg_exchange_rate
      ) : data(peg_debtor_id),
          peg_exchange_rate(peg_exchange_rate),
          creditor_id(creditor_id),
          debtor_id(debtor_id),
          amount(amount) {
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
          if (bid->data & PRICEABILITY_DECIDED_FLAG) {
            return bid->priceable();
          }
          bid->data |= PRICEABILITY_DECIDED_FLAG;
          Bid* peg = bid->peg_ptr;
          if (decide_priceability(peg)) {
            bid->data |= PRICEABLE_FLAG;
            bid->currency_price = peg->currency_price * bid->peg_exchange_rate;
            return true;
          }
        }
        return false;
      }
      void set_pointers() {
        for (auto it = bids.begin(); it != bids.end(); ++it) {
          Bid* bid = it->second;
          Key128 peg_key = Key128(bid->creditor_id, bid->data);
          try {
            bid->peg_ptr = bids.at(peg_key);
          } catch (const std::out_of_range& oor) {
            bid->peg_ptr = NULL;
          }
          bid->data = 0;  // `data` will hold the bit-flags from now on.
          if (bid->debtor_id == base_debtor_id) {
            bid->data |= PRICEABILITY_DECIDED_FLAG | PRICEABLE_FLAG;
            bid->currency_price = 1.0;
          }
        }
      }
      void calc_currency_prices() {
        for (auto it = bids.begin(); it != bids.end(); ++it) {
          decide_priceability(it->second);
        }
      }
      void prepare_for_iteration() {
        set_pointers();
        calc_currency_prices();
      }

    public:
      const i64 base_debtor_id;

      BidRegistry(i64 base_debtor_id)
        : base_debtor_id(base_debtor_id) {
      }
      ~BidRegistry() {
        for (auto it = bids.begin(); it != bids.end(); ++it) {
          delete it->second;
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
    ctypedef unsigned short distance

    cdef bool compare_prices(float, float) noexcept

    cdef cppclass Key128:
        """An 128-bit opaque hashable identifier.
        """
        const i64 first
        const i64 second
        Key128() noexcept
        Key128(i64, i64) noexcept
        size_t calc_hash() noexcept

    cdef cppclass Currency:
        """Contains information about a currency.

        Currencies are organized in a tree-like structure, each
        currency pointing to its peg currency (see the `peg_ptr`
        field). At the root of the tree is the "base currency". In
        addition to this, every currency maintains a `price` field
        (expressed in base currency's tokens), and several bit-flags:

        * To be a "confirmed currency" means that a system account has
          been successfully created in this currency, and the
          currency's debtor info document has been confirmed as
          correct.

        * To be a "tradable currency" means that the currency is both
          priceable (has a direct or indirect peg to the base
          currency), and confirmed.
        """
        const i64 debtor_id
        Currency* const peg_ptr
        const float peg_exchange_rate
        const float price
        Currency(i64, Key128, i64, float) except +
        bool confirmed() noexcept
        bool tradable() noexcept


    cdef cppclass CurrencyRegistry:
        """Given a set of currencies, generates the currency-tree.

        The base currency is determined by the `base_debtor_key` and
        `base_debtor_id` fields. Currencies that are separated from
        the base currency by no more than `max_distance_to_base` pegs
        are considered "priceable", and will be included in the
        generated tree.

        The `prepare_for_queries` method must be called before
        querying the registry, but only after all currencies have been
        added to the registry (by calling the `add_currency` method
        for each currency).
        """
        const Key128 base_debtor_key
        const i64 base_debtor_id
        const distance max_distance_to_base
        CurrencyRegistry(Key128, i64, distance) except +
        void add_currency(bool, Key128, i64, Key128, i64, float) except +
        void prepare_for_queries() except +
        Currency* get_tradable_currency(i64) except +
        float get_currency_price(i64) except +

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
        addition to this, for every bid, a `currency_price` expressed
        in base currency's tokens will be calculated.
        """
        const i64 creditor_id
        const i64 debtor_id
        const i64 amount
        const float currency_price
        Bid* const peg_ptr
        const float peg_exchange_rate
        Bid(i64, i64, i64, i64, float) except +

    cdef cppclass BidRegistry:
        """Given a set of `Bid`s, generates a tree of priceable bids.

        At the root of the tree is the "base currency" (determined by
        the `base_debtor_id` field). Bids whose currencies are
        directly or indirectly pegged to the base currency, are
        considered "priceable", and will be included in the generated
        tree. Bids that are not priceable, will be excluded from the
        tree.

        To obtain all the bids in the generated tree, continue calling
        the `get_priceable_bid` method, until it returns NULL.
        """
        const i64 base_debtor_id
        BidRegistry(i64) except +
        void add_bid(i64, i64, i64, i64, float) except +
        Bid* get_priceable_bid() noexcept


cdef class CandidateOffer:
    cdef readonly i64 amount
    cdef readonly i64 debtor_id
    cdef readonly i64 creditor_id


cdef class BidProcessor:
    cdef readonly str base_debtor_info_uri
    cdef readonly i64 base_debtor_id
    cdef readonly distance max_distance_to_base
    cdef readonly i64 min_trade_amount
    cdef BidRegistry* bid_registry_ptr
    cdef CurrencyRegistry* currency_registry_ptr
    cdef list[CandidateOffer] candidate_offers
    cdef unordered_set[i64] buyers
    cdef unordered_set[i64] sellers
    cdef unordered_set[i64] to_be_confirmed
    cdef Currency* _find_tradable_currency(self, Bid*)
    cdef void _add_candidate_offer(self, Bid*)
    cdef Key128 _calc_key128(self, str)
