# distutils: language = c++

cdef class Digraph:
    cdef NodeRegistry debtors
    cdef NodeRegistry creditors

    def __cinit__(self):
        pass
        # self._vmap = {ROOT_VERTEX: []}

    cdef void add_supply(self, i64 debtor_id, double amount, i64 creditor_id):
        cdef Node* debtor_ptr = self.debtors.get_node(debtor_id)
        if debtor_ptr == NULL:
            raise RuntimeError("invalid debtor node")

        cdef Node* creditor_ptr = self.creditors.get_node(creditor_id)
        if creditor_ptr == NULL:
            creditor_ptr = self.creditors.create_node(creditor_id, 0.0, 0)

        creditor_ptr.add_arc(debtor_ptr, amount)

        # assert v is not None
        # assert v != ROOT_VERTEX
        # if u in self._vmap:
        #     self._vmap[u].append(v)
        # else:
        #     self._vmap[u] = [v]
        #     self._vmap[ROOT_VERTEX].append(u)

    cdef void add_demand(self, i64 creditor_id, i64 debtor_id, double amount):
        pass
        # assert v is not None
        # assert v != ROOT_VERTEX
        # if u in self._vmap:
        #     self._vmap[u].append(v)
        # else:
        #     self._vmap[u] = [v]
        #     self._vmap[ROOT_VERTEX].append(u)

    def remove_arc(self, u, v):
        pass
        # assert u != ROOT_VERTEX
        # try:
        #     vlist = self._vmap[u]
        #     vlist[vlist.index(v)] = None
        # except (KeyError, ValueError):
        #     pass

    def _sink_vertex(self, v):
        pass
        # if v != ROOT_VERTEX:
        #     try:
        #         del self._vmap[v]
        #     except KeyError:
        #         pass
