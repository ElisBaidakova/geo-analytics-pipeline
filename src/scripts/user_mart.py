import logging
import argparse
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from config import EVENTS_PATH, CITIES_PATH, USER_MART_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class UserGeoProcessor:
    def __init__(self, spark: SparkSession, events_path: str, cities_path: str):
        self.spark = spark
        self.events_path = events_path
        self.cities_path = cities_path
        self.earth_radius = 6371

    def _calculate_haversine(self, lat1: str, lon1: str, lat2: str, lon2: str) -> F.Column:
        """Расчёт расстояния по формуле Хаверсина"""
        lat1_rad = F.radians(F.col(lat1))
        lon1_rad = F.radians(F.col(lon1))
        lat2_rad = F.radians(F.col(lat2))
        lon2_rad = F.radians(F.col(lon2))
        
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = F.pow(F.sin(dlat / 2), 2) + \
            F.cos(lat1_rad) * F.cos(lat2_rad) * F.pow(F.sin(dlon / 2), 2)
        
        return F.lit(2) * F.lit(self.earth_radius) * F.asin(F.sqrt(a))

    def enrich_events_with_city(self, df_events):
        """Обогащение событий данными о ближайшем городе"""
        logger.info("Начало обогащения событий координатами городов...")
    
        df_cities = self.spark.read.csv(
            self.cities_path, 
            header=True, 
            sep=";",
            inferSchema=True
        ) \
            .withColumnRenamed("lat", "city_lat") \
            .withColumnRenamed("lng", "city_lon") \
            .withColumnRenamed("city", "city_name") \
            .withColumnRenamed("timezone", "city_timezone")
    
        # Удаляем колонку id, она не нужна
        if "id" in df_cities.columns:
            df_cities = df_cities.drop("id")
    
        df_cross = df_events.crossJoin(F.broadcast(df_cities))
    
        df_with_dist = df_cross.withColumn(
            "distance",
            self._calculate_haversine("lat", "lon", "city_lat", "city_lon")
        )
    
        window_spec = Window.partitionBy("event_id").orderBy(F.col("distance").asc())
    
        df_nearest = df_with_dist.withColumn("rn", F.row_number().over(window_spec)) \
            .filter(F.col("rn") == 1) \
            .drop("rn", "distance", "city_lat", "city_lon")
        
        logger.info("Обогащение событий завершено.")
        return df_nearest
    
    def build_user_mart(self, df_enriched):
        """Формирование витрины в разрезе пользователей"""
        logger.info("Начало построения пользовательской витрины...")

        # 1. Актуальный город (последнее сообщение)
        w_last = Window.partitionBy("user_id").orderBy(F.col("datetime").desc())
        df_act = df_enriched.filter(F.col("event_type") == "message") \
            .withColumn("rn", F.row_number().over(w_last)) \
            .filter(F.col("rn") == 1) \
            .select("user_id", F.col("city_name").alias("act_city"), F.col("city_timezone").alias("timezone"))

        # 2. Домашний город — последнее непрерывное посещение длительностью >= 27 дней
        # Уникальные дни активности в городе
        df_daily = df_enriched \
            .withColumn("event_date", F.to_date("datetime")) \
            .select("user_id", "city_name", "event_date") \
            .distinct()

        # Определяем моменты смены города
        w_user_date = Window.partitionBy("user_id").orderBy("event_date")
        df_with_prev = df_daily \
            .withColumn("prev_city", F.lag("city_name").over(w_user_date)) \
            .withColumn(
                "city_changed",
                (F.col("prev_city").isNull()) | (F.col("city_name") != F.col("prev_city"))
            )

        # Нумеруем группы непрерывного присутствия
        df_with_group = df_with_prev \
            .withColumn(
                "group_id",
                F.sum(F.when(F.col("city_changed"), 1).otherwise(0)).over(w_user_date)
            )

        # Для каждой группы считаем длительность посещения
        df_groups = df_with_group \
            .groupBy("user_id", "city_name", "group_id") \
            .agg(
                F.min("event_date").alias("start_date"),
                F.max("event_date").alias("end_date"),
                (F.datediff(F.max("event_date"), F.min("event_date")) + 1).alias("duration_days")
            )

        # Последнее посещение длительностью >= 27 дней = домашний город
        w_last_visit = Window.partitionBy("user_id").orderBy(F.col("end_date").desc())
        df_home = df_groups \
            .filter(F.col("duration_days") >= 27) \
            .withColumn("rn", F.row_number().over(w_last_visit)) \
            .filter(F.col("rn") == 1) \
            .select("user_id", F.col("city_name").alias("home_city"))

        # 3. Статистика путешествий
        w_travel = Window.partitionBy("user_id").orderBy("datetime")
        df_travel = df_enriched.select("user_id", "city_name", "datetime") \
            .withColumn("prev_city", F.lag("city_name").over(w_travel)) \
            .filter((F.col("city_name") != F.col("prev_city")) | F.col("prev_city").isNull()) \
            .groupBy("user_id") \
            .agg(
                F.count("city_name").alias("travel_count"),
                F.collect_list("city_name").alias("travel_array")
            )

        # 4. Местное время последнего события
        df_last_event = df_enriched.withColumn("rn", F.row_number().over(w_last)) \
            .filter(F.col("rn") == 1) \
            .withColumn("local_time", F.from_utc_timestamp(F.col("datetime"), F.col("city_timezone"))) \
            .select("user_id", "local_time")

        # 5. Сборка итоговой витрины
        result = df_act \
            .join(df_home, "user_id", "left") \
            .join(df_travel, "user_id", "left") \
            .join(df_last_event, "user_id", "left") \
            .select(
                "user_id",
                "act_city",
                "home_city",
                "travel_count",
                "travel_array",
                "local_time"
            )

        # Если домашний город не определён, используем актуальный
        result = result.withColumn("home_city", F.coalesce(F.col("home_city"), F.col("act_city")))

        logger.info("Построение пользовательской витрины завершено.")
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build User Geo Mart")
    parser.add_argument("--sample", type=float, default=1.0, help="Sample fraction for testing (e.g., 0.1)")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("UserGeoMartBuilder") \
        .getOrCreate()

    try:
        logger.info(f"Чтение данных из {EVENTS_PATH} (sample={args.sample})...")
        df_raw = spark.read.parquet(EVENTS_PATH)    
        # Распаковка struct и унификация user_id
        df_events = df_raw.select(
            "event_type", "lat", "lon", "date",
            F.monotonically_increasing_id().alias("event_id"),
            # Унифицируем user_id: для разных типов событий берём разные поля
            F.coalesce(
                F.col("event.user"),
                F.col("event.reaction_from"),
                F.col("event.subscription_user"),
                F.col("event.message_from").cast("string")
            ).alias("user_id"),
            # Преобразуем datetime из string в timestamp
            F.to_timestamp("event.datetime").alias("datetime"),
            F.col("event.channel_id").alias("channel_id")
        )
        
        if args.sample < 1.0:
            df_events = df_events.sample(False, args.sample)
            logger.info(f"Взята выборка {args.sample * 100}% данных")

        processor = UserGeoProcessor(spark, EVENTS_PATH, CITIES_PATH)
        
        df_enriched = processor.enrich_events_with_city(df_events)
        df_user_mart = processor.build_user_mart(df_enriched)
        
        logger.info(f"Запись витрины в {OUTPUT_PATH}...")
        df_user_mart.write.mode("overwrite").parquet(OUTPUT_PATH)
        logger.info("Процесс успешно завершен.")

    except Exception as e:
        logger.error(f"Ошибка при выполнении: {e}")
        raise
    finally:
        spark.stop()
