from orchestrator.dags.mongo_to_postgres_stops import send_stops_to_postgres


def test_send_stops_to_postgres():
    stops_collection = get_mongo_collection('Stops')
    expected = stops_collection.count_documents({})  # number of documents: {} 8288
    rolled, committed = send_stops_to_postgres()
    result = rolled + committed
    assert result == expected


def test_committed_stops():
    stops_collection = get_mongo_collection('Stops')
    committed, _ = send_stops_to_postgres()
    pypeline = [
                {"$group": {
                  "_id": {"unit": "$unit", "post": "$post"}
                        }
                 },
                {"$count": "count"}
                ]
    distinct_unit_post = stops_collection.aggregate(pypeline)
    expected = list(distinct_unit_post)[-1]["count"]

    assert expected == committed
