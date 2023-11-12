# distutils: language = c++

cdef nodeflags FLG_D_DEFAULT = 0
cdef nodeflags FLG_C_DEFAULT = 0
cdef i64 ROOT_CREDITOR_ID = 0


cdef class Digraph:
    def __cinit__(self):
        self.root_creditor = self.creditors.create_node(
            ROOT_CREDITOR_ID, 0.0, FLG_C_DEFAULT
        )

    def add_debtor(self, i64 debtor_id, double min_amount):
        if self.debtors.get_node(debtor_id) != NULL:
            raise RuntimeError("duplicated debtor node")

        debtor = self.debtors.create_node(debtor_id, min_amount, FLG_D_DEFAULT)
        self.root_creditor.add_arc(debtor, infinity)

    def add_supply(self, double amount, i64 currency, i64 seller):
        debtor, creditor = self.ensure_nodes(currency, seller)
        debtor.add_arc(creditor, amount)

    def add_demand(self, i64 buyer, double amount, i64 currency):
        debtor, creditor = self.ensure_nodes(currency, buyer)
        creditor.add_arc(debtor, amount)

    cdef (Node*, Node*) ensure_nodes(self, i64 debtor_id, i64 creditor_id):
        debtor = self.debtors.get_node(debtor_id)
        if debtor == NULL:
            raise RuntimeError("invalid debtor node")

        creditor = self.creditors.get_node(creditor_id)
        if creditor == NULL:
            creditor = self.creditors.create_node(
                creditor_id, 0.0, FLG_C_DEFAULT
            )
        return debtor, creditor
