from pyspark import pipelines as dp

SILVER_OBT_TABLE = "uber.bronze.silver_obt"


def _silver_obt():
    return spark.readStream.table(SILVER_OBT_TABLE)


def _select_distinct(columns, keys):
    return _silver_obt().select(*columns).dropDuplicates(keys)


def _create_cdc_flow(target, source, keys, sequence_by, scd_type=1):
    dp.create_streaming_table(target)
    dp.create_auto_cdc_flow(
        target=target,
        source=source,
        keys=keys,
        sequence_by=sequence_by,
        stored_as_scd_type=scd_type,
    )


@dp.view
def dim_passenger_view():
    return _select_distinct(
        ["passenger_id", "passenger_name", "passenger_email", "passenger_phone"],
        ["passenger_id"],
    )


_create_cdc_flow("dim_passenger", "dim_passenger_view", ["passenger_id"], "passenger_id")


@dp.view
def dim_driver_view():
    return _select_distinct(
        ["driver_id", "driver_name", "driver_rating", "driver_phone", "driver_license"],
        ["driver_id"],
    )


_create_cdc_flow("dim_driver", "dim_driver_view", ["driver_id"], "driver_id")


@dp.view
def dim_vehicle_view():
    return _select_distinct(
        [
            "vehicle_id",
            "vehicle_make_id",
            "vehicle_type_id",
            "vehicle_model",
            "vehicle_color",
            "license_plate",
            "vehicle_make",
            "vehicle_type",
        ],
        ["vehicle_id"],
    )


_create_cdc_flow("dim_vehicle", "dim_vehicle_view", ["vehicle_id"], "vehicle_id")


@dp.view
def dim_payment_view():
    return _select_distinct(
        ["payment_method_id", "payment_method", "is_card", "requires_auth"],
        ["payment_method_id"],
    )


_create_cdc_flow(
    "dim_payment",
    "dim_payment_view",
    ["payment_method_id"],
    "payment_method_id",
)


@dp.view
def dim_booking_view():
    return _select_distinct(
        [
            "ride_id",
            "confirmation_number",
            "dropoff_location_id",
            "ride_status_id",
            "dropoff_city_id",
            "cancellation_reason_id",
            "dropoff_address",
            "dropoff_latitude",
            "dropoff_longitude",
            "booking_timestamp",
            "dropoff_timestamp",
            "pickup_address",
            "pickup_latitude",
            "pickup_longitude",
            "pickup_location_id",
        ],
        ["ride_id"],
    )


_create_cdc_flow("dim_booking", "dim_booking_view", ["ride_id"], "ride_id")


@dp.view
def dim_location_view():
    return _select_distinct(
        ["pickup_city_id", "pickup_city", "region", "state"],
        ["pickup_city_id"],
    )


_create_cdc_flow(
    "dim_location",
    "dim_location_view",
    ["pickup_city_id"],
    "pickup_city_id",
)


@dp.view
def fact_view():
    return _silver_obt().select(
        "ride_id",
        "pickup_city_id",
        "payment_method_id",
        "driver_id",
        "passenger_id",
        "vehicle_id",
        "distance_miles",
        "duration_minutes",
        "base_fare",
        "distance_fare",
        "time_fare",
        "surge_multiplier",
        "total_fare",
        "tip_amount",
        "rating",
        "base_rate",
        "per_mile",
        "per_minute",
    )


_create_cdc_flow(
    "fact",
    "fact_view",
    [
        "ride_id",
        "pickup_city_id",
        "payment_method_id",
        "driver_id",
        "passenger_id",
        "vehicle_id",
    ],
    "ride_id",
)
