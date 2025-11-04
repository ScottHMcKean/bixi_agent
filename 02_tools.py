# Databricks notebook source
# MAGIC %md
# MAGIC # BIXI GBFS API Calls
# MAGIC This notebook demonstrates how to register and use BIXI GBFS API functions
# MAGIC as Unity Catalog functions, making them available to your agent. Tested on Serverless V3

# COMMAND ----------

import mlflow
config = mlflow.models.ModelConfig(development_config='config.yaml')
dbutils.widgets.text('catalog', config.get('catalog'))
dbutils.widgets.text('schema', config.get('schema'))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION `${catalog}`.`${schema}`.recent_trips(
# MAGIC   station_code STRING
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   start_date TIMESTAMP,
# MAGIC   start_station_code STRING,
# MAGIC   end_date TIMESTAMP,
# MAGIC   end_station_code STRING,
# MAGIC   duration_sec INT,
# MAGIC   is_member BOOLEAN
# MAGIC )
# MAGIC RETURN
# MAGIC SELECT
# MAGIC   start_date,
# MAGIC   CAST(start_station_code AS STRING) AS start_station_code,
# MAGIC   end_date,
# MAGIC   CAST(end_station_code AS STRING) AS end_station_code,
# MAGIC   duration_sec,
# MAGIC   CAST(is_member AS BOOLEAN) AS is_member
# MAGIC FROM `${catalog}`.`${schema}`.od_trips
# MAGIC WHERE start_station_code = CAST(station_code AS INT)
# MAGIC ORDER BY start_date DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `${catalog}`.`${schema}`.recent_trips('6073')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION `${catalog}`.`${schema}`.get_total_stations()
# MAGIC RETURNS INTEGER
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT "Gives the total number of stations currently on the bixi network"
# MAGIC AS $$
# MAGIC import requests
# MAGIC
# MAGIC def get_total_stations() -> int:
# MAGIC     import requests
# MAGIC     url = f"https://gbfs.velobixi.com/gbfs/2-2/en/station_information.json"
# MAGIC     response = requests.get(url, timeout=10)
# MAGIC     info = response.json()
# MAGIC     total = len(info.get('data', {}).get('stations', []))
# MAGIC     return total
# MAGIC
# MAGIC result = get_total_stations()
# MAGIC return result
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `${catalog}`.`${schema}`.get_total_stations()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION `${catalog}`.`${schema}`.get_station_info(station_code STRING)
# MAGIC RETURNS TABLE (
# MAGIC   capacity BIGINT,
# MAGIC   eightd_has_key_dispenser BOOLEAN,
# MAGIC   electric_bike_surcharge_waiver BOOLEAN,
# MAGIC   external_id STRING,
# MAGIC   has_kiosk BOOLEAN,
# MAGIC   is_charging BOOLEAN,
# MAGIC   lat DOUBLE,
# MAGIC   lon DOUBLE,
# MAGIC   name STRING,
# MAGIC   rental_methods ARRAY<STRING>,
# MAGIC   short_name STRING,
# MAGIC   station_id STRING,
# MAGIC   eightd_station_services ARRAY<MAP<STRING, STRING>>
# MAGIC )
# MAGIC LANGUAGE PYTHON
# MAGIC HANDLER 'SampleTableHandler'
# MAGIC AS $$
# MAGIC class SampleTableHandler:
# MAGIC     def ensure_station_fields(self, station):
# MAGIC         fields = [
# MAGIC             'capacity',
# MAGIC             'eightd_has_key_dispenser',
# MAGIC             'electric_bike_surcharge_waiver',
# MAGIC             'external_id',
# MAGIC             'has_kiosk',
# MAGIC             'is_charging',
# MAGIC             'lat',
# MAGIC             'lon',
# MAGIC             'name',
# MAGIC             'rental_methods',
# MAGIC             'short_name',
# MAGIC             'station_id',
# MAGIC             'eightd_station_services'
# MAGIC         ]
# MAGIC         defaults = {
# MAGIC             'capacity': None,
# MAGIC             'eightd_has_key_dispenser': None,
# MAGIC             'electric_bike_surcharge_waiver': None,
# MAGIC             'external_id': None,
# MAGIC             'has_kiosk': None,
# MAGIC             'is_charging': None,
# MAGIC             'lat': None,
# MAGIC             'lon': None,
# MAGIC             'name': None,
# MAGIC             'rental_methods': [],
# MAGIC             'short_name': None,
# MAGIC             'station_id': None,
# MAGIC             'eightd_station_services': []
# MAGIC         }
# MAGIC         return {k: station.get(k, defaults[k]) for k in fields}
# MAGIC       
# MAGIC     def eval(self, station_code: str):
# MAGIC         import requests
# MAGIC         url = "https://gbfs.velobixi.com/gbfs/2-2/en/station_information.json"
# MAGIC         response = requests.get(url, timeout=10)
# MAGIC         info = response.json()
# MAGIC         stations = info.get("data", {}).get("stations", [])
# MAGIC         filtered = [s for s in stations if s.get("short_name") == station_code]
# MAGIC         return [self.ensure_station_fields(s) for s in filtered]
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `${catalog}`.`${schema}`.get_station_info('6073')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION `${catalog}`.`${schema}`.stations_within_1km(target_lat DOUBLE, target_lon DOUBLE)
# MAGIC RETURNS TABLE (
# MAGIC   short_name STRING
# MAGIC )
# MAGIC LANGUAGE PYTHON
# MAGIC HANDLER 'NearbyStationCodes'
# MAGIC AS $$
# MAGIC class NearbyStationCodes:
# MAGIC     def haversine(self, lat1, lon1, lat2, lon2):
# MAGIC         import math
# MAGIC         # Ensure all args are float
# MAGIC         lat1 = float(lat1)
# MAGIC         lon1 = float(lon1)
# MAGIC         lat2 = float(lat2)
# MAGIC         lon2 = float(lon2)
# MAGIC         R = 6371
# MAGIC         dLat = math.radians(lat2 - lat1)
# MAGIC         dLon = math.radians(lon2 - lon1)
# MAGIC         lat1 = math.radians(lat1)
# MAGIC         lat2 = math.radians(lat2)
# MAGIC         a = math.sin(dLat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dLon / 2)**2
# MAGIC         c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
# MAGIC         return R * c
# MAGIC
# MAGIC     def eval(self, target_lat, target_lon):
# MAGIC         import requests
# MAGIC         # Also ensure the SQL-provided args are float
# MAGIC         target_lat = float(target_lat)
# MAGIC         target_lon = float(target_lon)
# MAGIC         url = "https://gbfs.velobixi.com/gbfs/2-2/en/station_information.json"
# MAGIC         response = requests.get(url, timeout=10)
# MAGIC         data = response.json()
# MAGIC         stations = data.get('data', {}).get('stations', [])
# MAGIC         result = []
# MAGIC         for station in stations:
# MAGIC             if 'lat' in station and 'lon' in station and 'short_name' in station:
# MAGIC                 lat = float(station['lat'])
# MAGIC                 lon = float(station['lon'])
# MAGIC                 distance = self.haversine(target_lat, target_lon, lat, lon)
# MAGIC                 if distance <= 1:
# MAGIC                     result.append({'short_name': station['short_name']})
# MAGIC         return result
# MAGIC $$
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `${catalog}`.`${schema}`.stations_within_1km(45.5, -73.5)
