# Databricks notebook source
# MAGIC %md
# MAGIC ## **Transformation et Optimisation des Données (Gold Layer)**
# MAGIC
# MAGIC ### **Objectif :**
# MAGIC Créer un DataFrame consolidé final en transformant les données normalisées pour l'analyse avancée. Cette étape consiste à appliquer des agrégations et des filtres sur les données pour en tirer des informations pertinentes pour la visualisation et la prise de décision.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Étapes :**
# MAGIC
# MAGIC 1. **Agrégation des données :**
# MAGIC    - Pour mieux comprendre l'impact de l'indice de position sociale (IPS) sur la répartition des effectifs par niveau scolaire, des agrégations ont été effectuées sur les colonnes pertinentes.
# MAGIC    - Par exemple, le calcul de la somme des effectifs par académies et départements, en tenant compte des différents niveaux scolaires (école, collège).
# MAGIC
# MAGIC 2. **Création de colonnes calculées :**
# MAGIC    - Des colonnes supplémentaires peuvent être créées pour effectuer des analyses comme l'indice de pauvreté ou la catégorisation de l'IPS.
# MAGIC    - On peut également ajouter des calculs pour estimer les tendances des effectifs en fonction de l'IPS ou d'autres critères.
# MAGIC
# MAGIC 3. **Filtrage des données :**
# MAGIC    - Les données peuvent être filtrées pour exclure certaines académies ou départements selon les critères d'analyse.
# MAGIC    - Cela permet de se concentrer sur les zones géographiques ou les écoles spécifiques d'intérêt.
# MAGIC
# MAGIC 4. **Optimisation des données :**
# MAGIC    - Les données sont optimisées pour une lecture plus rapide et une meilleure gestion dans le cadre de l'analyse.
# MAGIC    - Cela inclut la suppression des doublons et l'indexation sur des colonnes clés pour accélérer les jointures et les calculs.
# MAGIC
# MAGIC 5. **Stockage dans la table Gold :**
# MAGIC    - Une fois les données optimisées et agrégées, elles sont stockées dans une table Iceberg de la couche Gold pour les analyses finales.
# MAGIC    - Cette table est prête à être utilisée pour des visualisations dans des outils comme Power BI, ou pour des analyses statistiques avancées.
# MAGIC

# COMMAND ----------

storage_name="bigdatadlgen2"
container_name="gold"
access_key=""
mount_point_name="/mnt/gold"

# COMMAND ----------

dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_name}.blob.core.windows.net/",
    mount_point = mount_point_name,
    extra_configs = {
        f"fs.azure.account.key.{storage_name}.blob.core.windows.net": access_key
    }
)

# COMMAND ----------

# Charger la table Iceberg dans le notebook Gold
df_silver = spark.read.table("iceberg.final_silver")


# COMMAND ----------

display(df_silver)

# COMMAND ----------

from pyspark.sql import functions as F

# Calculer la moyenne et la distribution de l'IPS par académie
ips_by_academie = df_silver.groupBy("Académie").agg(
    F.avg("IPS_Indice").alias("IPS_Moyenne"),
    F.min("IPS_Indice").alias("IPS_Min"),
    F.max("IPS_Indice").alias("IPS_Max"),
    F.count("Effectifs").alias("Nombre d'écoles")
)

# Afficher les résultats
display(ips_by_academie)


# COMMAND ----------

from pyspark.sql import functions as F

# Calculer le total des élèves et le nombre d'écoles par académie
effectifs_by_region = df_silver.groupBy("Académie").agg(
    F.sum("Effectifs").alias("Total élèves"),
    F.countDistinct("Nom de l'établissement").alias("Nombre d'écoles")
)

# Afficher les résultats
display(effectifs_by_region)


# COMMAND ----------

# Ajouter une colonne pour catégoriser les écoles par niveau d'IPS
silver_df = df_silver.withColumn(
    "Categorie_IPS",
    F.when(F.col("IPS_Indice") < 80, "Faible")
     .when((F.col("IPS_Indice") >= 80) & (F.col("IPS_Indice") < 120), "Moyen")
     .otherwise("Élevé")
)

# Afficher les écoles catégorisées
display(silver_df)


# COMMAND ----------

# Sauvegarder les données dans la table Gold Layer
ips_by_academie.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.gold_ips_by_academie")

effectifs_by_region.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.gold_effectifs_by_region")


# COMMAND ----------

silver_df.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.gold_enriched_schools")


# COMMAND ----------

# Afficher les colonnes disponibles dans le DataFrame
print(silver_df.columns)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC markdown
# MAGIC Copier le code
# MAGIC ## **Création et Sauvegarde des Tables de Dimension et de Faits (Gold Layer)**
# MAGIC
# MAGIC ### **Objectif :**
# MAGIC Créer les tables de dimensions et de faits dans la couche Gold pour permettre une analyse efficace et précise. Ces tables sont utilisées pour fournir des informations détaillées sur les dimensions d'analyse et pour enregistrer les faits agrégés, prêts pour l'analyse et la visualisation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Étapes :**
# MAGIC
# MAGIC 1. **Création des Tables de Dimensions :**
# MAGIC    - **Dimension École :** Contient les informations sur les établissements scolaires, les académies, départements et secteurs.
# MAGIC    - **Dimension IPS :** Contient les indices de position sociale (IPS) et les catégories associées.
# MAGIC    - **Dimension Géographique :** Contient les informations géographiques sur les communes et départements.
# MAGIC    - **Dimension Date :** Contient l'information sur l'année de rentrée scolaire pour une analyse temporelle.
# MAGIC    
# MAGIC    Ces tables sont créées à partir des données disponibles dans la couche Silver, avec l'utilisation de la méthode `distinct()` pour garantir l'unicité des valeurs.
# MAGIC
# MAGIC 2. **Création de la Table de Faits :**
# MAGIC    - **Faits Effectifs :** Contient les informations agrégées sur les effectifs scolaires, avec des liens vers les dimensions École, IPS et géographiques.
# MAGIC
# MAGIC 3. **Sauvegarde dans la Gold Layer :**
# MAGIC    - Chaque table de dimension et la table de faits sont ensuite sauvegardées dans la Gold Layer en utilisant le format **Iceberg**, ce qui permet de garantir la performance des requêtes et de faciliter l'intégration dans des outils d'analyse comme Power BI.
# MAGIC
# MAGIC ---

# COMMAND ----------

# Créer la table de dimension École
dim_ecole = silver_df.select(
    "Nom de l'établissement",
    "Académie",
    "Département",
    "Secteur"
).distinct()

# Sauvegarder dans la Gold Layer
dim_ecole.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.gold_dim_ecole")


# COMMAND ----------

# Créer la table de dimension IPS
dim_ips = silver_df.select(
    "IPS_Indice",
    "Categorie_IPS"
).distinct()

# Sauvegarder dans la Gold Layer
dim_ips.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.gold_dim_ips")


# COMMAND ----------

# Créer la table de dimension Géographique
dim_geo = silver_df.select(
    "Code INSEE de la commune",
    "Nom de la commune",
    "Code département"
).distinct()

# Sauvegarder dans la Gold Layer
dim_geo.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.gold_dim_geo")


# COMMAND ----------

# Créer la table de dimension Date
dim_date = silver_df.select(
    "Rentrée scolaire"
).distinct()

# Sauvegarder dans la Gold Layer
dim_date.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.gold_dim_date")


# COMMAND ----------

# Créer la table de faits Effectifs
fait_effectifs = silver_df.select(
    "Effectifs",
    "Académie",
    "Département",
    "Nom de l'établissement",
    "IPS_Indice",
    "Categorie_IPS"
)

# Sauvegarder dans la Gold Layer
fait_effectifs.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.gold_fait_effectifs")


# COMMAND ----------

dim_dates = silver_df.select(
    "Rentrée scolaire", 
).distinct()


jointure_faits_dim = silver_df.join(
    dim_ecole, on=["Nom de l'établissement", "Académie", "Département", "Secteur"], how="inner"
).join(
    dim_ips, on=["IPS_Indice", "Categorie_IPS"], how="inner"
).join(
    dim_geo, on=["Code INSEE de la commune", "Code département","Nom de la commune"], how="inner"
).join(
    dim_dates, on=["Rentrée scolaire"], how="inner"  
)

display(jointure_faits_dim)

