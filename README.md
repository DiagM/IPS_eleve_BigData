# 📊 Analyse de l'Impact de l'Indice de Position Sociale sur la Répartition des Effectifs

## Description du Projet
Ce projet vise à analyser comment l'indice de position sociale (IPS) des élèves influe sur la répartition des effectifs par niveau et la gestion des classes dans les écoles. Il utilise une architecture de type **Lakehouse** intégrant **Databricks Community** et **Microsoft Fabric** (Synapse Data Engineering pour l'ETL et Power BI pour la visualisation).

---

## 🚀 Objectifs
1. **Ingestion et préparation des données** : Importer les fichiers CSV et JSON depuis [data.gouv.fr](https://www.data.gouv.fr).
2. **Transformation des données** : Nettoyage et structuration des données dans un modèle en étoile.
3. **Architecture Lakehouse** : Mise en œuvre avec Apache Iceberg pour le stockage.
4. **Visualisation des données** : Utilisation de Power BI pour fournir des insights clairs et exploitables.

---

## 🛠️ Technologies Utilisées
### Outils et plateformes :
- **Airbyte** : Ingestion des données.
- **Databricks Community** : Préparation et transformation des données.
- **Microsoft Fabric Synapse Data Engineering** : ETL et gestion des données.
- **Databricks** : Visualisation.

### Formats et frameworks :
- **Apache Iceberg** : Format open table pour le stockage.
- **Redpanda** : Gestion du streaming des données en temps réel.
- **PySpark** : Manipulation et traitement des données.
