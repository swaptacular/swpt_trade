import pytest
from swpt_trade.models import CollectorAccount


@pytest.fixture(scope="function")
def client(app, db_session):
    return app.test_client()


def test_ensure_collectors(client):
    json_request = {
        "type": "ActivateCollectorsRequest",
        "numberOfAccounts": 5,
    }

    r = client.post(
        "/trade/collectors/666/activate",
        headers={"X-Swpt-User-Id": "creditors:3"},
        json=json_request,
    )
    assert r.status_code == 403
    assert len(CollectorAccount.query.all()) == 0

    r = client.post(
        "/trade/collectors/666/activate",
        headers={"X-Swpt-User-Id": "INVALID"},
        json=json_request,
    )
    assert r.status_code == 403
    assert len(CollectorAccount.query.all()) == 0

    r = client.post(
        "/trade/collectors/666/activate",
        headers={"X-Swpt-User-Id": "creditors-supervisor"},
        json={
            "type": "WrongType",
            "debtorId": 666,
            "numberOfAccounts": 5,
        },
    )
    assert r.status_code == 422
    assert len(CollectorAccount.query.all()) == 0

    r = client.post(
        "/trade/collectors/666/activate",
        headers={"X-Swpt-User-Id": "creditors-supervisor"},
        json={},
    )
    assert r.status_code == 204
    assert len(CollectorAccount.query.all()) == 1

    r = client.post(
        "/trade/collectors/666/activate",
        headers={"X-Swpt-User-Id": "creditors-supervisor"},
        json=json_request,
    )
    assert r.status_code == 204
    cas = CollectorAccount.query.all()
    assert len(cas) == 5
    assert all(x.debtor_id == 666 for x in cas)

    r = client.post(
        "/trade/collectors/666/activate",
        headers={"X-Swpt-User-Id": "creditors-superuser"},
        json=json_request,
    )
    assert r.status_code == 204
    assert len(CollectorAccount.query.all()) == 5

    r = client.post(
        "/trade/collectors/666/activate",
        json=json_request,
    )
    assert r.status_code == 204
    assert len(CollectorAccount.query.all()) == 5

    r = client.post(
        "/trade/collectors/666/activate",
        json={
            "type": "ActivateCollectorsRequest",
            "numberOfAccounts": 5000000000000,
        },
    )
    assert r.status_code == 500
    assert len(CollectorAccount.query.all()) == 5

