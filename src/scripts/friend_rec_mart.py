import logging
import argparse
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

from user_mart import UserGeoProcessor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FriendRecProcessor:
    def __init__(self, spark: SparkSession, events_path: str, cities_path: str):
        self.spark = spark
        self.events_path = events_path
        self.cities_path = cities_path
        self.geo_processor = UserGeoProcessor(spark, events_path, cities_path)

    def build_friend_rec_mart(self, df_events):
        """Построение витрины для рекомендации друзей"""
        logger.info("Начало построения витрины рекомендаций друзей...")

        # Шаг 1: Находим пары пользователей, подписанных на один и тот же канал
        logger.info("Поиск пар пользователей в общих каналах...")
        
        # Для подписок используем subscription_user и subscription_channel
        df_subs = df_events.filter(F.col("event_type") == "subscription") \
            .select(
                F.col("user_id"),
                F.col("subscription_channel").alias("channel_id")
            )
        
        df_pairs = df_subs.alias("s1").join(
            df_subs.alias("s2"),
            (F.col("s1.channel_id") == F.col("s2.channel_id")) & 
            (F.col("s1.user_id") != F.col("s2.user_id"))
        ).select(
            F.col("s1.user_id").alias("user_left"),
            F.col("s2.user_id").alias("user_right")
        ).distinct()

        # Шаг 2: Обеспечиваем уникальность пар
        df_unique_pairs = df_pairs.withColumn(
            "pair_id",
            F.when(F.col("user_left") < F.col("user_right"),
                   F.concat_ws("-", "user_left", "user_right"))
             .otherwise(F.concat_ws("-", "user_right", "user_left"))
        ).dropDuplicates(["pair_id"]).drop("pair_id")

        # Шаг 3: Исключаем пары, которые уже переписывались
        logger.info("Фильтрация пар, которые уже переписывались...")
        
        # Для сообщений используем message_from и message_to (приводим к string)
        df_chats = df_events.filter(F.col("event_type") == "message") \
            .select(
                F.when(F.col("message_from").cast("string") < F.col("message_to").cast("string"),
                       F.col("message_from").cast("string"))
                .otherwise(F.col("message_to").cast("string")).alias("u1"),
                F.when(F.col("message_from").cast("string") < F.col("message_to").cast("string"),
                       F.col("message_to").cast("string"))
                .otherwise(F.col("message_from").cast("string")).alias("u2")
            ).distinct()
        
        df_no_chat = df_unique_pairs.join(
            df_chats,
            (F.col("user_left") == F.col("u1")) & (F.col("user_right") == F.col("u2")),
            "left_anti"
        ).drop("u1", "u2")
        
        # Шаг 4: Фильтр по расстоянию <= 1 км
        logger.info("Вычисление расстояния между пользователями...")
        w_last = Window.partitionBy("user_id").orderBy(F.col("datetime").desc())

        # Сначала находим последние сообщения пользователей
        df_last_messages = df_events.filter(F.col("event_type") == "message") \
            .withColumn("rn", F.row_number().over(w_last)) \
            .filter(F.col("rn") == 1) \
            .drop("rn")

        # Обогащаем последние сообщения данными о городе
        df_last_coords = self.geo_processor.enrich_events_with_city(df_last_messages) \
            .select(
                "user_id",
                F.col("lat").alias("lat_u"),
                F.col("lon").alias("lon_u"),
                F.col("city_name").alias("city_name"),
                F.col("city_timezone").alias("city_timezone")
            )
        
        # Создаём копию с переименованными колонками для правого пользователя
        # чтобы избежать конфликта имён при join (lat_u, lon_u есть и у l, и у r)
        df_last_coords_r = df_last_coords.select(
            F.col("user_id").alias("user_id_r"),
            F.col("lat_u").alias("lat_u_r"),
            F.col("lon_u").alias("lon_u_r"),
            F.col("city_name").alias("city_name_r"),
            F.col("city_timezone").alias("city_timezone_r")
        )
        
        df_with_dist = df_no_chat \
            .join(df_last_coords.alias("l"), F.col("user_left") == F.col("l.user_id"), "left") \
            .join(df_last_coords_r, F.col("user_right") == F.col("user_id_r"), "left") \
            .withColumn(
                "dist",
                self.geo_processor._calculate_haversine("lat_u", "lon_u", "lat_u_r", "lon_u_r")
            ) \
            .filter(F.col("dist") <= 1.0)

        # Шаг 5: Добавление метаданных витрины
        logger.info("Формирование финальных атрибутов витрины...")
        result = df_with_dist \
            .withColumn("processed_dttm", F.current_timestamp()) \
            .withColumn("zone_id", F.col("l.city_name")) \
            .withColumn("local_time", F.from_utc_timestamp(F.current_timestamp(), F.col("l.city_timezone"))) \
            .select(
                "user_left", 
                "user_right", 
                "processed_dttm", 
                "zone_id", 
                "local_time"
            )

        logger.info("Построение витрины рекомендаций друзей завершено.")
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Friend Recommendation Mart")
    parser.add_argument("--sample", type=float, default=1.0, help="Sample fraction for testing (e.g., 0.1)")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("FriendRecMartBuilder") \
        .getOrCreate()

    EVENTS_PATH = "/user/master/data/geo/events"
    CITIES_PATH = "/user/s26546941/data/geo/geo.csv"
    OUTPUT_PATH = "/user/s26546941/data/marts/friend_rec_mart"

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
            F.col("event.channel_id").alias("channel_id"),
            # Сохраняем оригинальные поля для friend_rec
            F.col("event.message_from").alias("message_from"),
            F.col("event.message_to").alias("message_to"),
            F.col("event.subscription_channel").alias("subscription_channel")
        )
        
        if args.sample < 1.0:
            df_events = df_events.sample(False, args.sample)
            logger.info(f"Взята выборка {args.sample * 100}% данных")

        processor = FriendRecProcessor(spark, EVENTS_PATH, CITIES_PATH)
        
        df_friend_rec_mart = processor.build_friend_rec_mart(df_events)
        
        logger.info(f"Запись витрины в {OUTPUT_PATH}...")
        df_friend_rec_mart.write.mode("overwrite").parquet(OUTPUT_PATH)
        logger.info("Процесс успешно завершен.")

    except Exception as e:
        logger.error(f"Ошибка при выполнении: {e}")
        raise
    finally:
        spark.stop()
