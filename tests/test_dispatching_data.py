from swpt_trade.run_turn_subphases import DispatchingData


def test_dispatching_data():
    dd = DispatchingData(2)
    dd.register_collecting(1, 2, 3, 100)
    dd.register_collecting(1, 2, 3, 150)
    dd.register_collecting(1, 2, 4, 300)
    dd.register_sending(1, 2, 3, 500)
    dd.register_receiving(1, 2, 3, 1000)
    dd.register_dispatching(1, 2, 3, 2000)

    ll = list(dd.dispatching_statuses())
    ll.sort(key=lambda x: (x["collector_id"], x["turn_id"], x["debtor_id"]))
    assert len(ll) == 2
    assert ll[0]["collector_id"] == 1
    assert ll[0]["turn_id"] == 2
    assert ll[0]["debtor_id"] == 3
    assert ll[0]["amount_to_collect"] == 250
    assert ll[0]["amount_to_send"] == 500
    assert ll[0]["number_to_receive"] == 1
    assert ll[0]["amount_to_dispatch"] == 2000
    assert ll[1]["collector_id"] == 1
    assert ll[1]["turn_id"] == 2
    assert ll[1]["debtor_id"] == 4
    assert ll[1]["amount_to_collect"] == 300
    assert ll[1]["amount_to_send"] == 0
    assert ll[1]["number_to_receive"] == 0
    assert ll[1]["amount_to_dispatch"] == 0
