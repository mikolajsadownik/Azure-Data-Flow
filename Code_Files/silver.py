from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


def _string(name):
    return StructField(name, StringType(), True)


def _long(name):
    return StructField(name, LongType(), True)


def _double(name):
    return StructField(name, DoubleType(), True)


RIDES_SCHEMA = StructType(
    [
        _string("ride_id"),
        _string("confirmation_number"),
        _string("passenger_id"),
        _string("driver_id"),
        _string("vehicle_id"),
        _string("pickup_location_id"),
        _string("dropoff_location_id"),
        _long("vehicle_type_id"),
        _long("vehicle_make_id"),
        _long("payment_method_id"),
        _long("ride_status_id"),
        _long("pickup_city_id"),
        _long("dropoff_city_id"),
        _long("cancellation_reason_id"),
        _string("passenger_name"),
        _string("passenger_email"),
        _string("passenger_phone"),
        _string("driver_name"),
        _double("driver_rating"),
        _string("driver_phone"),
        _string("driver_license"),
        _string("vehicle_model"),
        _string("vehicle_color"),
        _string("license_plate"),
        _string("pickup_address"),
        _double("pickup_latitude"),
        _double("pickup_longitude"),
        _string("dropoff_address"),
        _double("dropoff_latitude"),
        _double("dropoff_longitude"),
        _double("distance_miles"),
        _long("duration_minutes"),
        _string("booking_timestamp"),
        _string("pickup_timestamp"),
        _string("dropoff_timestamp"),
        _double("base_fare"),
        _double("distance_fare"),
        _double("time_fare"),
        _double("surge_multiplier"),
        _double("subtotal"),
        _double("tip_amount"),
        _double("total_fare"),
        _double("rating"),
    ]
)

TIMESTAMP_COLUMNS = ("booking_timestamp", "pickup_timestamp", "dropoff_timestamp")


def _cast_timestamps(df):
    for column_name in TIMESTAMP_COLUMNS:
        df = df.withColumn(column_name, col(column_name).cast("timestamp"))
    return df


dp.create_streaming_table("stg_rides")


@dp.append_flow(target="stg_rides")
def rides_bulk():
    return _cast_timestamps(spark.readStream.table("bulk_rides"))


@dp.append_flow(target="stg_rides")
def rides_stream():
    parsed_rides = (
        spark.readStream.table("rides_raw")
        .withColumn("parsed_rides", from_json(col("rides"), RIDES_SCHEMA))
        .select("parsed_rides.*")
    )
    return _cast_timestamps(parsed_rides)
