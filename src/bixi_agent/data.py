class BixiDataProcessor:
    """Process BIXI data for machine learning."""

    def __init__(self):
        """Initialize the data processor."""
        pass

    def merge_trips_with_stations(
        self, trips_df, stations_df
    ):
        """Merge trip data with station data to include zone information.

        Args:
            trips_df: Spark DataFrame containing trip data
            stations_df: Spark DataFrame containing station data

        Returns:
            Merged Spark DataFrame with zone information
        """
        # Merge start station information
        trips_with_start_zones = trips_df.join(
            stations_df.selectExpr("code as start_code", "name as start_zone"),
            trips_df.start_station_code == stations_df.code,
            "left"
        ).drop(stations_df.code)

        # Merge end station information
        trips_with_zones = trips_with_start_zones.join(
            stations_df.selectExpr("code as end_code", "name as end_zone"),
            trips_with_start_zones.end_station_code == stations_df.code,
            "left"
        ).drop(stations_df.code)

        # Remove missing zones
        trips_with_zones = trips_with_zones.dropna(subset=["start_zone"])

        return trips_with_zones

    def aggregate_trips_by_zone_and_date(self, trips_df):
        """Aggregate trips by zone and date.

        Args:
            trips_df: Spark DataFrame containing trip data with zone information

        Returns:
            Aggregated Spark DataFrame with trip counts by zone and date
        """
        from pyspark.sql.functions import to_date, col, dayofweek, month, when

        trips_df = trips_df.withColumn("start_date", to_date("start_date"))

        trips_by_zone_date = (
            trips_df.groupBy("start_zone", "start_date")
            .count()
            .withColumnRenamed("count", "trip_count")
        )

        trips_by_zone_date = trips_by_zone_date.withColumn(
            "day_of_week", dayofweek("start_date") - 2
        ).withColumn(
            "month", month("start_date")
        ).withColumn(
            "is_weekend", col("day_of_week").isin([5, 6])
        )

        return trips_by_zone_date
