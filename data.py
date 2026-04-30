import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()

VEHICLE_TYPE_MAPPING = [
    {
        "vehicle_type_id": 1,
        "vehicle_type": "UberX",
        "description": "Standard",
        "base_rate": 2.50,
        "per_mile": 1.75,
        "per_minute": 0.35,
    },
    {
        "vehicle_type_id": 2,
        "vehicle_type": "UberXL",
        "description": "Extra Large",
        "base_rate": 3.50,
        "per_mile": 2.25,
        "per_minute": 0.45,
    },
    {
        "vehicle_type_id": 3,
        "vehicle_type": "UberPOOL",
        "description": "Shared Ride",
        "base_rate": 2.00,
        "per_mile": 1.50,
        "per_minute": 0.30,
    },
    {
        "vehicle_type_id": 4,
        "vehicle_type": "Uber Comfort",
        "description": "Comfortable",
        "base_rate": 3.00,
        "per_mile": 2.00,
        "per_minute": 0.40,
    },
    {
        "vehicle_type_id": 5,
        "vehicle_type": "Uber Black",
        "description": "Premium",
        "base_rate": 5.00,
        "per_mile": 3.50,
        "per_minute": 0.60,
    },
]

PAYMENT_METHOD_MAPPING = [
    {
        "payment_method_id": 1,
        "payment_method": "Credit Card",
        "is_card": True,
        "requires_auth": True,
    },
    {
        "payment_method_id": 2,
        "payment_method": "Debit Card",
        "is_card": True,
        "requires_auth": True,
    },
    {
        "payment_method_id": 3,
        "payment_method": "Digital Wallet",
        "is_card": False,
        "requires_auth": False,
    },
    {
        "payment_method_id": 4,
        "payment_method": "Cash",
        "is_card": False,
        "requires_auth": False,
    },
]

RIDE_STATUS_MAPPING = [
    {"ride_status_id": 1, "ride_status": "Completed", "is_completed": True},
    {"ride_status_id": 2, "ride_status": "Cancelled", "is_completed": False},
]

VEHICLE_MAKE_MAPPING = [
    {"vehicle_make_id": 1, "vehicle_make": "Toyota"},
    {"vehicle_make_id": 2, "vehicle_make": "Honda"},
    {"vehicle_make_id": 3, "vehicle_make": "Ford"},
    {"vehicle_make_id": 4, "vehicle_make": "Chevrolet"},
    {"vehicle_make_id": 5, "vehicle_make": "Nissan"},
    {"vehicle_make_id": 6, "vehicle_make": "BMW"},
    {"vehicle_make_id": 7, "vehicle_make": "Mercedes"},
]

CITY_MAPPING = [
    {"city_id": 1, "city": "New York", "state": "NY", "region": "Northeast"},
    {"city_id": 2, "city": "Los Angeles", "state": "CA", "region": "West"},
    {"city_id": 3, "city": "Chicago", "state": "IL", "region": "Midwest"},
    {"city_id": 4, "city": "Houston", "state": "TX", "region": "South"},
    {"city_id": 5, "city": "Phoenix", "state": "AZ", "region": "Southwest"},
    {"city_id": 6, "city": "Philadelphia", "state": "PA", "region": "Northeast"},
    {"city_id": 7, "city": "San Antonio", "state": "TX", "region": "South"},
    {"city_id": 8, "city": "San Diego", "state": "CA", "region": "West"},
    {"city_id": 9, "city": "Dallas", "state": "TX", "region": "South"},
    {"city_id": 10, "city": "San Jose", "state": "CA", "region": "West"},
]

CANCELLATION_REASON_MAPPING = [
    {"cancellation_reason_id": 1, "cancellation_reason": "Driver cancelled"},
    {"cancellation_reason_id": 2, "cancellation_reason": "Passenger cancelled"},
    {"cancellation_reason_id": 3, "cancellation_reason": "No show"},
    {"cancellation_reason_id": 4, "cancellation_reason": None},
]

RIDE_STATUS_BY_NAME = {status["ride_status"]: status for status in RIDE_STATUS_MAPPING}
CANCELLATION_REASONS = [
    reason
    for reason in CANCELLATION_REASON_MAPPING
    if reason["cancellation_reason"] is not None
]
NO_CANCELLATION_REASON = next(
    reason
    for reason in CANCELLATION_REASON_MAPPING
    if reason["cancellation_reason"] is None
)
CANCELLED_RIDE_PROBABILITY = 0.10


def _random_trip_times():
    pickup_time = datetime.now() - timedelta(
        days=random.randint(0, 30),
        hours=random.randint(0, 23),
    )
    duration_minutes = random.randint(5, 120)
    booking_time = pickup_time - timedelta(minutes=random.randint(1, 10))
    dropoff_time = pickup_time + timedelta(minutes=duration_minutes)

    return booking_time, pickup_time, dropoff_time, duration_minutes


def _calculate_fares(vehicle_type, distance, duration_minutes, is_cancelled):
    base_fare = vehicle_type["base_rate"]
    distance_fare = round(distance * vehicle_type["per_mile"], 2)
    time_fare = round(duration_minutes * vehicle_type["per_minute"], 2)
    surge_multiplier = round(random.uniform(1.0, 2.5), 2)
    subtotal = round((base_fare + distance_fare + time_fare) * surge_multiplier, 2)

    tip = 0
    if not is_cancelled:
        tip = round(
            random.choice([0, 0, 0, 1, 2, 3, 5, random.uniform(1, 20)]),
            2,
        )

    return {
        "base_fare": base_fare,
        "distance_fare": distance_fare,
        "time_fare": time_fare,
        "surge_multiplier": surge_multiplier,
        "subtotal": subtotal,
        "tip_amount": tip,
        "total_fare": round(subtotal + tip, 2),
    }


def _ride_status(is_cancelled):
    if is_cancelled:
        status = RIDE_STATUS_BY_NAME["Cancelled"]
        reason = random.choice(CANCELLATION_REASONS)
    else:
        status = RIDE_STATUS_BY_NAME["Completed"]
        reason = NO_CANCELLATION_REASON

    return status, reason


def _random_rating(is_cancelled):
    if is_cancelled:
        return None

    return random.choice([None, random.randint(1, 5)])


def generate_uber_ride_confirmation():
    booking_time, pickup_time, dropoff_time, duration_minutes = _random_trip_times()
    distance = round(random.uniform(0.5, 50), 2)
    is_cancelled = random.random() < CANCELLED_RIDE_PROBABILITY

    vehicle_type = random.choice(VEHICLE_TYPE_MAPPING)
    ride_status, cancellation_reason = _ride_status(is_cancelled)
    fares = _calculate_fares(vehicle_type, distance, duration_minutes, is_cancelled)

    pickup_city = random.choice(CITY_MAPPING)
    dropoff_city = random.choice(CITY_MAPPING)
    vehicle_make = random.choice(VEHICLE_MAKE_MAPPING)
    payment_method = random.choice(PAYMENT_METHOD_MAPPING)

    return {
        "ride_id": str(uuid.uuid4()),
        "confirmation_number": fake.bothify("??#-####-??##"),
        "passenger_id": str(uuid.uuid4()),
        "driver_id": str(uuid.uuid4()),
        "vehicle_id": str(uuid.uuid4()),
        "pickup_location_id": str(uuid.uuid4()),
        "dropoff_location_id": str(uuid.uuid4()),
        "vehicle_type_id": vehicle_type["vehicle_type_id"],
        "vehicle_make_id": vehicle_make["vehicle_make_id"],
        "payment_method_id": payment_method["payment_method_id"],
        "ride_status_id": ride_status["ride_status_id"],
        "pickup_city_id": pickup_city["city_id"],
        "dropoff_city_id": dropoff_city["city_id"],
        "cancellation_reason_id": cancellation_reason["cancellation_reason_id"],
        "passenger_name": fake.name(),
        "passenger_email": fake.email(),
        "passenger_phone": fake.phone_number(),
        "driver_name": fake.name(),
        "driver_rating": round(random.uniform(4.0, 5.0), 2),
        "driver_phone": fake.phone_number(),
        "driver_license": fake.bothify("??-???-#######"),
        "vehicle_model": fake.word().capitalize(),
        "vehicle_color": random.choice(
            ["Black", "White", "Gray", "Silver", "Blue", "Red"]
        ),
        "license_plate": fake.bothify("???-####"),
        "pickup_address": fake.address().replace("\n", ", "),
        "pickup_latitude": round(random.uniform(-90, 90), 6),
        "pickup_longitude": round(random.uniform(-180, 180), 6),
        "dropoff_address": fake.address().replace("\n", ", "),
        "dropoff_latitude": round(random.uniform(-90, 90), 6),
        "dropoff_longitude": round(random.uniform(-180, 180), 6),
        "distance_miles": distance,
        "duration_minutes": duration_minutes,
        "booking_timestamp": booking_time.isoformat(),
        "pickup_timestamp": pickup_time.isoformat(),
        "dropoff_timestamp": dropoff_time.isoformat(),
        **fares,
        "rating": _random_rating(is_cancelled),
    }
