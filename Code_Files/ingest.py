from pyspark import pipelines as dp
from pyspark.sql.functions import col

EVENT_HUB_NAMESPACE = "uberevents"
EVENT_HUB_NAME = "ubertopic"
EVENT_HUB_CONNECTION_STRING = spark.conf.get("connection_string")

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EVENT_HUB_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EVENT_HUB_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
        f'required username="$ConnectionString" password="{EVENT_HUB_CONNECTION_STRING}";'
    ),
    "kafka.request.timeout.ms": "10000",
    "kafka.session.timeout.ms": "10000",
    "maxOffsetsPerTrigger": "10000",
    "failOnDataLoss": "true",
    "startingOffsets": "earliest",
}


@dp.table
def rides_raw():
    return (
        spark.readStream.format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
        .withColumn("rides", col("value").cast("string"))
    )
