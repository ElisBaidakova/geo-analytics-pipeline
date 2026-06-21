import logging
import argparse
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

from user_mart import UserGeoProcessor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ZoneGeoProcessor:
    def __init__(self, spark: SparkSession, events_path: str, cities_path: str):
        self.spark = spark
        self.events_path = events_path
        self.cities_path = cities_path
        self.user_processor = UserGeoProcessor(spark, events_path, cities_path)

    def _prepare_coordinates_for_enrichment(self, df_events):
        """Подготовка координат для обогащения"""
        logger.info("Подготовка координат: поиск последних сообщений пользователей...")
        
        w_last_msg = Window.partitionBy("user_id").orderBy(F.col("datetime").desc())
        
        df_last_msg = df_events.filter(F.col("event_type") == "message") \
            .withColumn("rn", F.row_number().over(w_last_msg)) \
            .filter(F.col("rn") == 1) \
            .select(
                "user_id", 
                F.col("lat").alias("last_msg_lat"), 
                F.col("lon").alias("last_msg_lon")
            )
        
        df_with_fallback = df_events.join(df_last_msg, "user_id", "left")
        
        df_unified = df_with_fallback \
            .withColumn(
                "lat", 
                F.when(F.col("event_type") == "message", F.col("lat"))
                 .otherwise(F.col("last_msg_lat"))
            ) \
            .withColumn(
                "lon", 
                F.when(F.col("event_type") == "message", F.col("lon"))
                 .otherwise(F.col("last_msg_lon"))
            ) \
            .drop("last_msg_lat", "last_msg_lon", "rn")
            
        logger.info("Подготовка координат завершена.")
        return df_unified

    def build_zone_mart(self, df_events):
        """Построение витрины в разрезе географических зон"""
        logger.info("Начало построения витрины в разрезе зон...")

        df_prepared = self._prepare_coordinates_for_enrichment(df_events)
        df_enriched = self.user_processor.enrich_events_with_city(df_prepared)

        # Определяем "регистрации" — это первое событие каждого пользователя
        w_first = Window.partitionBy("user_id").orderBy(F.col("datetime").asc())

        df_base = df_enriched \
            .withColumn("zone_id", F.col("city_name")) \
            .withColumn("event_date", F.to_date("datetime")) \
            .withColumn("month", F.date_trunc("month", "event_date")) \
            .withColumn("week", F.date_trunc("week", "event_date")) \
            .withColumn("rn_first", F.row_number().over(w_first)) \
            .withColumn("is_registration", F.col("rn_first") == 1)

        logger.info("Вычисление недельных агрегатов...")
        week_agg = df_base.groupBy("month", "week", "zone_id").agg(
            F.count(F.when(F.col("event_type") == "message", 1)).alias("week_message"),
            F.count(F.when(F.col("event_type") == "reaction", 1)).alias("week_reaction"),
            F.count(F.when(F.col("event_type") == "subscription", 1)).alias("week_subscription"),
            F.count(F.when(F.col("is_registration"), 1)).alias("week_user")
        )

        logger.info("Вычисление месячных агрегатов...")
        month_agg = df_base.groupBy("month", "zone_id").agg(
            F.count(F.when(F.col("event_type") == "message", 1)).alias("month_message"),
            F.count(F.when(F.col("event_type") == "reaction", 1)).alias("month_reaction"),
            F.count(F.when(F.col("event_type") == "subscription", 1)).alias("month_subscription"),
            F.count(F.when(F.col("is_registration"), 1)).alias("month_user")
        )

        result = week_agg.join(month_agg, ["month", "zone_id"], "left")

        numeric_cols = [
            "week_message", "week_reaction", "week_subscription", "week_user",
            "month_message", "month_reaction", "month_subscription", "month_user"
        ]

        for col_name in numeric_cols:
            result = result.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))

        logger.info("Построение витрины в разрезе зон завершено.")
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Zone Geo Mart")
    parser.add_argument("--sample", type=float, default=1.0, help="Sample fraction for testing (e.g., 0.1)")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("ZoneGeoMartBuilder") \
        .getOrCreate()

    EVENTS_PATH = "/user/master/data/geo/events"
    CITIES_PATH = "/user/s26546941/data/geo/geo.csv"
    OUTPUT_PATH = "/user/s26546941/data/marts/zone_geo_mart"

    try:
        logger.info(f"Чтение данных из {EVENTS_PATH} (sample={args.sample})...")
        df_raw = spark.read.parquet(EVENTS_PATH)
        
        # Распаковка struct и унификация user_id
        df_events = df_raw.select(
            "event_type", "lat", "lon", "date",
            F.monotonically_increasing_id().alias("event_id"),
            F.coalesce(
                F.col("event.user"),
                F.col("event.reaction_from"),
                F.col("event.subscription_user"),
                F.col("event.message_from").cast("string")
            ).alias("user_id"),
            F.to_timestamp("event.datetime").alias("datetime"),
            F.col("event.channel_id").alias("channel_id")
        )
        
        if args.sample < 1.0:
            df_events = df_events.sample(False, args.sample)
            logger.info(f"Взята выборка {args.sample * 100}% данных")

        processor = ZoneGeoProcessor(spark, EVENTS_PATH, CITIES_PATH)
        
        df_zone_mart = processor.build_zone_mart(df_events)
        
        logger.info(f"Запись витрины в {OUTPUT_PATH}...")
        df_zone_mart.write.mode("overwrite").parquet(OUTPUT_PATH)
        logger.info("Процесс успешно завершен.")

    except Exception as e:
        logger.error(f"Ошибка при выполнении: {e}")
        raise
    finally:
        spark.stop()
