# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Bronze : Ingestion des Données
# MAGIC ## Objectif :
# MAGIC Collecter les données brutes depuis les sources externes (fichiers, API, bases de données, etc.).
# MAGIC Stocker les données dans un format brut sans transformation.

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration et Montage d'un Point d'Accès Azure Data Lake
# MAGIC Cette section de code définit les paramètres nécessaires pour accéder et monter un conteneur Azure Data Lake Storage Gen2 à partir d'un notebook Databricks. Le montage facilite la lecture et l'écriture des données stockées dans le conteneur à l'aide d'un chemin local virtuel.
# MAGIC ## Variables :
# MAGIC ### storage_name :
# MAGIC
# MAGIC Nom du compte de stockage Azure.
# MAGIC
# MAGIC Exemple : bigdatadlgen2.
# MAGIC
# MAGIC Utilisé pour identifier le compte dans la connexion.
# MAGIC ### container_name :
# MAGIC
# MAGIC Nom du conteneur dans le compte de stockage où les données sont stockées.
# MAGIC
# MAGIC Exemple : bronze.
# MAGIC ### access_key :
# MAGIC
# MAGIC Clé d'accès au compte de stockage Azure.
# MAGIC
# MAGIC Permet d'authentifier et d'autoriser l'accès aux données dans le conteneur.
# MAGIC ### mount_point_name :
# MAGIC
# MAGIC Nom du point de montage local dans le cluster Databricks.
# MAGIC
# MAGIC Exemple : /mnt/bronze.
# MAGIC
# MAGIC Une fois monté, ce chemin est utilisé pour accéder aux données du conteneur.

# COMMAND ----------

storage_name="bigdatadlgen2"
container_name="bronze"
access_key=""
mount_point_name="/mnt/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Montage du conteneur Azure dans Databricks : 
# MAGIC
# MAGIC Le conteneur Azure peut être monté avec la commande dbutils.fs.mount, en utilisant les variables définies ci-dessus.

# COMMAND ----------

dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_name}.blob.core.windows.net/",
    mount_point = mount_point_name,
    extra_configs = {
        f"fs.azure.account.key.{storage_name}.blob.core.windows.net": access_key
    }
)

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")

# COMMAND ----------

bronze_ips_ecole = "/mnt/bronze/ips/ipsecole.csv"
bronze_effectifs_ecole = "/mnt/bronze/effectifs_eleves/Effectif_ecole.csv"


# COMMAND ----------

bronze_ips_college = "/mnt/bronze/ips/ipscollege.csv"
bronze_effectifs_college = "/mnt/bronze/effectifs_eleves/effectifcollege.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Documentation - Lecture des données depuis la couche Bronze**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Description :**
# MAGIC Cette section de code charge des fichiers CSV depuis la couche Bronze dans des DataFrames PySpark. Chaque fichier contient des données brutes correspondant à des écoles ou collèges. Ces DataFrames serviront de base pour le nettoyage et la transformation des données dans les couches Silver et Gold.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Détails de la configuration :**
# MAGIC
# MAGIC 1. **Options communes :**
# MAGIC    - **`format("csv")`** : Spécifie que le fichier source est au format CSV.
# MAGIC    - **`option("header", "true")`** : Indique que la première ligne du fichier contient les noms des colonnes.
# MAGIC    - **`option("inferSchema", "true")`** : Active la détection automatique des types de données des colonnes.
# MAGIC    - **`option("delimiter", ";")`** : Définit le séparateur de colonnes comme étant un point-virgule (`;`), ce qui est courant dans les fichiers CSV.
# MAGIC
# MAGIC 2. **Variables de source :**
# MAGIC    - **`bronze_ips_ecole`** : Chemin vers le fichier CSV contenant les données des indices IPS des écoles.
# MAGIC    - **`bronze_effectifs_ecole`** : Chemin vers le fichier CSV contenant les effectifs des écoles.
# MAGIC    - **`bronze_ips_college`** : Chemin vers le fichier CSV contenant les indices IPS des collèges.
# MAGIC    - **`bronze_effectifs_college`** : Chemin vers le fichier CSV contenant les effectifs des collèges.
# MAGIC
# MAGIC 3. **Création des DataFrames :**
# MAGIC    - **`df_bronze_ips_ecole`** : Contient les données brutes relatives aux indices IPS des écoles.
# MAGIC    - **`df_bronze_effectifs_ecole`** : Contient les données brutes relatives aux effectifs des écoles.
# MAGIC    - **`df_bronze_ips_college`** : Contient les données brutes relatives aux indices IPS des collèges.
# MAGIC    - **`df_bronze_effectifs_college`** : Contient les données brutes relatives aux effectifs des collèges.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Étapes associées :**
# MAGIC
# MAGIC 1. **Chargement des fichiers :**
# MAGIC    Chaque fichier CSV est chargé dans un DataFrame à l'aide de la méthode `spark.read.format("csv").load()`.
# MAGIC
# MAGIC 2. **Vérification du contenu des DataFrames :**
# MAGIC    Une fois les données chargées, vous pouvez afficher les premières lignes pour vérifier la structure :
# MAGIC    ```python
# MAGIC    df_bronze_ips_ecole.show(5)
# MAGIC    df_bronze_effectifs_ecole.show(5)
# MAGIC    df_bronze_ips_college.show(5)
# MAGIC    df_bronze_effectifs_college.show(5)
# MAGIC e
# MAGIC
# MAGIC

# COMMAND ----------

df_bronze_ips_ecole = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load(bronze_ips_ecole)

df_bronze_effectifs_ecole = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load(bronze_effectifs_ecole)

df_bronze_ips_college = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load(bronze_ips_college)

df_bronze_effectifs_college = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load(bronze_effectifs_college)


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Documentation - Sauvegarde des données dans la couche Bronze**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Description :**
# MAGIC Cette section de code sauvegarde les données brutes chargées depuis des fichiers CSV dans des tables Iceberg au sein de la couche Bronze. Ces tables serviront de base pour les transformations ultérieures vers les couches Silver et Gold.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Détails de la configuration :**
# MAGIC
# MAGIC 1. **Méthode de sauvegarde :**
# MAGIC    - **`write.format("iceberg")`** : Spécifie que les données doivent être sauvegardées dans le format Iceberg, un format de table optimisé pour les workflows analytiques.
# MAGIC    - **`mode("overwrite")`** : Permet d'écraser les données existantes dans la table si elle existe déjà.
# MAGIC
# MAGIC 2. **Noms des tables Iceberg :**
# MAGIC    - **`iceberg.bronze_ips_ecole`** : Table contenant les données brutes des indices IPS des écoles.
# MAGIC    - **`iceberg.bronze_effectifs_ecole`** : Table contenant les données brutes des effectifs des écoles.
# MAGIC    - **`iceberg.bronze_ips_college`** : Table contenant les données brutes des indices IPS des collèges.
# MAGIC    - **`iceberg.bronze_effectifs_college`** : Table contenant les données brutes des effectifs des collèges.
# MAGIC
# MAGIC 3. **DataFrames utilisés :**
# MAGIC    - **`df_bronze_ips_ecole`** : Correspond aux données brutes des indices IPS des écoles.
# MAGIC    - **`df_bronze_effectifs_ecole`** : Correspond aux données brutes des effectifs des écoles.
# MAGIC    - **`df_bronze_ips_college`** : Correspond aux données brutes des indices IPS des collèges.
# MAGIC    - **`df_bronze_effectifs_college`** : Correspond aux données brutes des effectifs des collèges.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Étapes associées :**
# MAGIC
# MAGIC 1. **Sauvegarde des tables :**
# MAGIC    Chaque DataFrame est sauvegardé dans une table Iceberg à l'aide de la méthode `write.format("iceberg").saveAsTable()`.
# MAGIC
# MAGIC 2. **Validation des tables sauvegardées :**
# MAGIC    Vérifiez si les tables ont été correctement créées et contiennent les données attendues :
# MAGIC    ```sql
# MAGIC    SELECT * FROM iceberg.bronze_ips_ecole LIMIT 5;
# MAGIC

# COMMAND ----------

df_bronze_ips_ecole.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.bronze_ips_ecole")
df_bronze_effectifs_ecole.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.bronze_effectifs_ecole")
df_bronze_ips_college.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.bronze_ips_college")
df_bronze_effectifs_college.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.bronze_effectifs_college")

