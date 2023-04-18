# Tests that work exclusively with the flask endpoints to ensure functionality


def test_view_set_retrieve(flaskClient, resetViewTracker):
    test_view = {"view": ["192.158.42.0:1242"]}
    flaskClient.put("/kvs/admin/view", json=test_view)
    response = flaskClient.get("/kvs/admin/view")
    assert response.json == test_view


def test_uninitialized(flaskClient, resetViewTracker):
    response = flaskClient.delete("/kvs/admin/view")
    expected_error = {"error": "uninitialized"}
    assert response.json == expected_error
    assert response.status_code == 418


def test_put_data_large(flaskClient, initNode, resetLocalDatabase):
    data = {"val": "1" * 8001, "causal-metadata": {}}
    expected_error = "val too large"
    response = flaskClient.put("/kvs/data/testkey", json=data)
    assert response.json["error"] == expected_error
    assert response.status_code == 400


def test_put_data(flaskClient, initNode, resetLocalDatabase):
    data = {"val": "foo", "causal-metadata": {}}
    response = flaskClient.put("/kvs/data/testkey", json=data)
    assert "causal-metadata" in response.json.keys()
    assert response.status_code == 201

    response = flaskClient.put("/kvs/data/testkey", json=data)
    assert "causal-metadata" in response.json.keys()
    assert response.status_code == 200


def test_put_gossip_(flaskClient, initNode, resetLocalDatabase):
    kvs_content = {"x": (1, 10, {})}
    data = {"origin": "10.10.0.5", "kvs": kvs_content.copy()}
    response = flaskClient.put("/gossip", json=data)
    assert response.status_code == 201
    assert response.json == {}

    expected_kvs_response = {"x": [1, 10, {}]}
    data = {"origin": "10.10.0.5", "kvs": {}}
    response = flaskClient.put("/gossip", json=data)
    assert response.status_code == 200
    assert (
        response.json == expected_kvs_response
    )  # tuples are serialized and turned into json arrays
