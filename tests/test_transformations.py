import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_fare_amount_positive():
    data = pd.DataFrame({
        "fare_amount": [10.5, 25.0, 0.0, -5.0, 100.0]
    })
    valid = data[data["fare_amount"] > 0]
    assert len(valid) == 3
    assert all(valid["fare_amount"] > 0)


def test_trip_distance_bounds():
    data = pd.DataFrame({
        "trip_distance": [0.5, 2.0, 600.0, -1.0, 10.0]
    })
    valid = data[
        (data["trip_distance"] > 0) &
        (data["trip_distance"] < 500)
    ]
    assert len(valid) == 3


def test_passenger_count_bounds():
    data = pd.DataFrame({
        "passenger_count": [0, 1, 4, 8, 9, 3]
    })
    valid = data[
        (data["passenger_count"] >= 1) &
        (data["passenger_count"] <= 8)
    ]
    assert len(valid) == 4


def test_time_of_day_classification():
    def classify_time(hour):
        if 6 <= hour < 12:
            return "morning"
        elif 12 <= hour < 17:
            return "afternoon"
        elif 17 <= hour < 21:
            return "evening"
        else:
            return "night"

    assert classify_time(7) == "morning"
    assert classify_time(13) == "afternoon"
    assert classify_time(18) == "evening"
    assert classify_time(23) == "night"
    assert classify_time(0) == "night"
    assert classify_time(6) == "morning"


def test_trip_duration_calculation():
    data = pd.DataFrame({
        "pickup_seconds": [1000, 2000, 3000],
        "dropoff_seconds": [1500, 2800, 2500]
    })
    data["duration_minutes"] = (
        data["dropoff_seconds"] - data["pickup_seconds"]
    ) / 60

    assert round(data["duration_minutes"].iloc[0], 2) == round(500/60, 2)
    valid = data[data["duration_minutes"] > 0]
    assert len(valid) == 2


def test_payment_type_mapping():
    payment_map = {1: "Credit Card", 2: "Cash", 3: "No Charge", 4: "Dispute"}

    data = pd.DataFrame({
        "payment_type": [1, 2, 3, 4, 99]
    })
    data["payment_desc"] = data["payment_type"].map(payment_map).fillna("Unknown")

    assert data[data["payment_type"] == 1]["payment_desc"].values[0] == "Credit Card"
    assert data[data["payment_type"] == 2]["payment_desc"].values[0] == "Cash"
    assert data[data["payment_type"] == 99]["payment_desc"].values[0] == "Unknown"


def test_tip_percentage_calculation():
    data = pd.DataFrame({
        "tip_amount": [2.0, 0.0, 5.0],
        "fare_amount": [10.0, 15.0, 0.0]
    })
    data["tip_pct"] = data.apply(
        lambda row: round((row["tip_amount"] / row["fare_amount"]) * 100, 2)
        if row["fare_amount"] > 0 else 0.0,
        axis=1
    )

    assert data["tip_pct"].iloc[0] == 20.0
    assert data["tip_pct"].iloc[1] == 0.0
    assert data["tip_pct"].iloc[2] == 0.0


def test_is_weekend_classification():
    data = pd.DataFrame({
        "day_of_week": [1, 2, 3, 4, 5, 6, 7]
    })
    data["is_weekend"] = data["day_of_week"].isin([1, 7])

    assert data[data["day_of_week"] == 1]["is_weekend"].values[0] == True
    assert data[data["day_of_week"] == 7]["is_weekend"].values[0] == True
    assert data[data["day_of_week"] == 3]["is_weekend"].values[0] == False


def test_speed_calculation():
    distance_miles = 10.0
    duration_minutes = 30.0
    speed = distance_miles / (duration_minutes / 60)
    assert speed == 20.0

    duration_zero = 0.0
    speed_zero = None if duration_zero == 0 else distance_miles / (duration_zero / 60)
    assert speed_zero is None


def test_data_quality_check_null_detection():
    data = pd.DataFrame({
        "pickup_datetime": [pd.Timestamp("2024-01-01"), None, pd.Timestamp("2024-01-03")],
        "fare_amount": [10.0, 20.0, None]
    })

    null_pickup = data["pickup_datetime"].isna().sum()
    null_fare = data["fare_amount"].isna().sum()

    assert null_pickup == 1
    assert null_fare == 1