# Databricks notebook source
# MAGIC %md
# MAGIC # **Notebook - Transformation vers la couche Silver**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## **Objectif :**
# MAGIC Transformer les données brutes (couche Bronze) en données enrichies et nettoyées pour la couche Silver. Ces transformations visent à standardiser, enrichir et valider les données afin de les préparer pour les analyses avancées dans la couche Gold.
# MAGIC

# COMMAND ----------

storage_name="bigdatadlgen2"
container_name="silver"
access_key=""
mount_point_name="/mnt/silver"

# COMMAND ----------

dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_name}.blob.core.windows.net/",
    mount_point = mount_point_name,
    extra_configs = {
        f"fs.azure.account.key.{storage_name}.blob.core.windows.net": access_key
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Lecture des données depuis Iceberg (Bronze Layer)**
# MAGIC
# MAGIC ### **Objectif :**
# MAGIC Charger les données brutes stockées dans la couche Bronze (format Iceberg) pour effectuer des transformations et des analyses dans la couche Silver.
# MAGIC

# COMMAND ----------

df_iceberg_ips_ecole = spark.read.table("iceberg.bronze_ips_ecole")
df_iceberg_effectifs_ecole = spark.read.table("iceberg.bronze_effectifs_ecole")
df_iceberg_ips_college = spark.read.table("iceberg.bronze_ips_college")
df_iceberg_effectifs_college = spark.read.table("iceberg.bronze_effectifs_college")

# COMMAND ----------

# Afficher les données de manière interactive dans Databricks
display(df_iceberg_ips_ecole)
display(df_iceberg_effectifs_ecole)
display(df_iceberg_ips_college)
display(df_iceberg_effectifs_college)


# COMMAND ----------

# MAGIC %md
# MAGIC ## **Transformation et Normalisation des Données (Silver Layer)**
# MAGIC
# MAGIC ### **Objectif :**
# MAGIC Effectuer des transformations sur les données brutes pour les normaliser et les préparer pour l'analyse. Cette étape inclut la jointure entre les indices IPS et les effectifs des établissements scolaires (écoles et collèges), ainsi que le nettoyage des données.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Étapes :**
# MAGIC
# MAGIC 1. **Renommage des colonnes pour éviter les conflits :**
# MAGIC    - Les colonnes de chaque DataFrame ont été renommées pour inclure un préfixe spécifique (`ips_ecole_`, `effectifs_ecole_`, etc.).
# MAGIC    - Cela permet de différencier les colonnes portant des noms similaires lors des jointures.
# MAGIC
# MAGIC 2. **Jointures extérieures entre les DataFrames :**
# MAGIC    - Les données des indices IPS et des effectifs sont combinées pour les écoles et les collèges.
# MAGIC    - Les conditions de jointure incluent :
# MAGIC      - La correspondance entre le nom de l'établissement et le département.
# MAGIC      - Une gestion des valeurs nulles avec `coalesce`.
# MAGIC
# MAGIC 3. **Sélection des colonnes pertinentes :**
# MAGIC    - Après les jointures, seules les colonnes nécessaires pour l'analyse ont été sélectionnées.
# MAGIC    - Les colonnes sélectionnées incluent des informations sur l'établissement, les effectifs et l'indice IPS.
# MAGIC
# MAGIC 4. **Nettoyage des données :**
# MAGIC    - Suppression des lignes contenant des valeurs nulles dans les colonnes critiques.
# MAGIC
# MAGIC 5. **Union des données :**
# MAGIC    - Les DataFrames nettoyés pour les écoles et les collèges ont été combinés avec `unionByName` pour créer un DataFrame consolidé.
# MAGIC
# MAGIC 6. **DataFrame final :**
# MAGIC    - Une version finale du DataFrame a été créée, contenant les colonnes standardisées nécessaires pour l'analyse.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Visualisation des données :**
# MAGIC
# MAGIC - Les DataFrames nettoyés pour les écoles et les collèges sont affichés avec `display` pour vérifier leur structure et leur qualité.
# MAGIC - Le DataFrame final consolidé est également affiché pour validation.

# COMMAND ----------

from pyspark.sql import functions as F

ips_ecole_df = df_iceberg_ips_ecole.alias("ips_ecole")
effectifs_ecole_df = df_iceberg_effectifs_ecole.alias("effectifs_ecole")
ips_college_df = df_iceberg_ips_college.alias("ips_college")
effectifs_college_df = df_iceberg_effectifs_college.alias("effectifs_college")

ips_ecole_df_renamed = ips_ecole_df.select(
    [F.col(c).alias(f"ips_ecole_{c}") for c in ips_ecole_df.columns]
)

effectifs_ecole_df_renamed = effectifs_ecole_df.select(
    [F.col(c).alias(f"effectifs_ecole_{c}") for c in effectifs_ecole_df.columns]
)

ips_college_df_renamed = ips_college_df.select(
    [F.col(c).alias(f"ips_college_{c}") for c in ips_college_df.columns]
)

effectifs_college_df_renamed = effectifs_college_df.select(
    [F.col(c).alias(f"effectifs_college_{c}") for c in effectifs_college_df.columns]
)

ecole_joined_df_renamed = ips_ecole_df_renamed.join(
    effectifs_ecole_df_renamed,
    (
        (ips_ecole_df_renamed["ips_ecole_Nom de l'établissement"] == effectifs_ecole_df_renamed["effectifs_ecole_Dénomination principale"]) &
        (ips_ecole_df_renamed["ips_ecole_Département"] == effectifs_ecole_df_renamed["effectifs_ecole_Département"]) &
        (F.coalesce(effectifs_ecole_df_renamed["effectifs_ecole_Patronyme"], F.lit("")) == F.coalesce(ips_ecole_df_renamed["ips_ecole_Nom de la commune"], F.lit("")))
    ),
    "outer"
)


college_joined_df_renamed = ips_college_df_renamed.join(
    effectifs_college_df_renamed,
    (
        
        (ips_college_df_renamed["ips_college_Département"] == effectifs_college_df_renamed["effectifs_college_Département"]) &
        (F.coalesce(effectifs_college_df_renamed["effectifs_college_Patronyme"], F.lit("")) == F.coalesce(ips_college_df_renamed["ips_college_Nom de la commune"], F.lit("")))
    ),
    "outer"
)

ecole_renamed_df = ecole_joined_df_renamed.select(
    F.col("ips_ecole_Rentrée scolaire").alias("Rentrée scolaire"),
    F.col("ips_ecole_Académie").alias("Académie"),
    F.col("ips_ecole_Code du département").alias("Code département"),
    F.col("ips_ecole_Département").alias("Département"),
    F.col("ips_ecole_UAI").alias("UAI"),
    F.col("ips_ecole_Nom de l'établissement").alias("Nom de l'établissement"),
    F.col("ips_ecole_Code INSEE de la commune").alias("Code INSEE de la commune"),
    F.col("ips_ecole_Nom de la commune").alias("Nom de la commune"),
    F.col("ips_ecole_Secteur").alias("Secteur"),
    F.col("ips_ecole_Effectifs").alias("Effectifs"),
    F.col("ips_ecole_IPS").alias("IPS_Indice"),
)
ecole_cleaned_df = ecole_renamed_df.dropna()

college_renamed_df = college_joined_df_renamed.select(
    F.col("ips_college_Rentrée scolaire").alias("Rentrée scolaire"),
    F.col("ips_college_Académie").alias("Académie"),
    F.col("ips_college_Code du département").alias("Code département"),
    F.col("ips_college_Département").alias("Département"),
    F.col("ips_college_UAI").alias("UAI"),
    F.col("ips_college_Nom de l'établissment").alias("Nom de l'établissement"),
    F.col("ips_college_Code INSEE de la commune").alias("Code INSEE de la commune"),
    F.col("ips_college_Nom de la commune").alias("Nom de la commune"),
    F.col("ips_college_Secteur").alias("Secteur"),
    F.col("ips_college_Effectifs").alias("Effectifs"),
    F.col("ips_college_IPS").alias("IPS_Indice"),
)
college_cleaned_df = college_renamed_df.dropna()

display(ecole_cleaned_df)
display(college_cleaned_df)

combined_df = ecole_cleaned_df.unionByName(college_cleaned_df)


final_df = combined_df.select(
    F.col("Rentrée scolaire"),
    F.col("Académie"),
    F.col("Code département"),
    F.col("Département"),
    F.col("UAI"),
    F.col("Nom de l'établissement"),
    F.col("Code INSEE de la commune"),
    F.col("Nom de la commune"),
    F.col("Secteur"),
    F.col("Effectifs"),
    F.col("IPS_Indice")
)


display(final_df)


# COMMAND ----------

# sauvegarde au format iceberg
final_df.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.final_silver")

