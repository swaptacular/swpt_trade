from . import cytest
from swpt_trade.example cimport mysum

@cytest
def test_mysum():
    assert mysum(2, 3) == 5
