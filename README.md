# Projet Big Data Cytech 25
Pour l'instant voici les exercices finis :
- ex01_data_retrieval
- ex02_data_ingestion
- ex03_sql_table_creation
- ex04_dashboard

## Prérequis

- Docker et Docker Compose installés
- SBT installé
- Python 3.8+
- `uv` installé ([Installation](https://github.com/astral-sh/uv#installation))

  Si `uv` n'est pas installé, installe-le avec :
  ```sh
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

---

## 0. Initialiser l'environnement Python

À la racine du projet, initialise `uv` :

```sh
uv init
uv sync
```

Cela crée `.venv` et installe les dépendances définies dans `pyproject.toml`.

### Ajouter des dépendances

Ajouter de nouvelle dépendance Python dans `pyproject.toml` :

```toml
[project]
dependencies = [
    "streamlit",
    "pandas",
    "psycopg2-binary",
    "matplotlib",
    "nouvelle-dependance>=1.0.0",
]
```

Puis synchronise :

```sh
uv sync
```

---

## 1. Lancer l'infrastructure

À la racine du projet, lance :

```sh
sudo docker-compose up -d
```

Cela démarre :
- MinIO (stockage)
- PostgreSQL (base de données)
- Spark

---

## 2. Configurer MinIO

1. Ouvre [http://localhost:9000](http://localhost:9000) dans ton navigateur.
2. Connecte-toi avec :
   - **Identifiant** : `minio`
   - **Mot de passe** : `minio123`
3. Crée un bucket nommé :  
   ```
   nyc-yellow-tripdata
   ```

---

## 3. Télécharger et envoyer les données sur MinIO

Dans le dossier `ex01_data_retrieval` :

```sh
cd ex01_data_retrieval
sbt run
```

Cela télécharge le fichier Parquet et l'upload automatiquement dans le bucket MinIO.

---

## 4. Vérifier la base de données PostgreSQL

Reviens à la racine du projet, puis connecte-toi à la base :

```sh
sudo docker exec -it postgres psql -U postgres -d bigdata_db
```

### Tester les tables de dimension

Exécute les requêtes suivantes dans le client `psql` :

```sql
-- Nombre d'emplacements importés
SELECT count(*) FROM dim_location;

-- Afficher quelques emplacements
SELECT * FROM dim_location LIMIT 5;

-- Afficher quelques vendeurs
SELECT * FROM dim_vendor LIMIT 5;

-- Afficher quelques types de paiement
SELECT * FROM dim_payment_type LIMIT 5;

-- Afficher quelques codes tarifaires
SELECT * FROM dim_rate_code LIMIT 5;
```

Pour quitter `psql` :

```
\q
```

---

## 5. Ingérer les données nettoyées dans PostgreSQL

Dans le dossier `ex02_data_ingestion` :

```sh
cd ex02_data_ingestion
sbt run
```

Cela :
- Lit les données brutes depuis MinIO
- Nettoie les données (Branche 1)
- Les sauvegarde en Parquet nettoyé dans MinIO (Branche 1)
- Les insère dans la table `fact_trips` de PostgreSQL (Branche 2)

### Vérifier l'insertion des données

Reviens à la racine et connecte-toi à la base :

```sh
cd ..
sudo docker exec -it postgres psql -U postgres -d bigdata_db
```

Puis vérifie que les données ont bien été insérées :

```sql
-- Nombre de trajets insérés
SELECT COUNT(*) FROM fact_trips;

-- Afficher quelques trajets
SELECT * FROM fact_trips LIMIT 5;

-- Statistiques par vendor
SELECT v.vendor_name, COUNT(*) as nb_trajets, SUM(f.total_amount) as revenue
FROM fact_trips f
JOIN dim_vendor v ON f.vendor_id = v.vendor_id
GROUP BY v.vendor_name;
```

Pour quitter `psql` :

```
\q
```

---

## 6. Lancer le Dashboard Streamlit

Lance le dashboard depuis la racine du projet :

```sh
cd ex04_dashboard
uv run streamlit run app.py
```

Le dashboard s'ouvre automatiquement sur [http://localhost:8501](http://localhost:8501) 🎉

---

**Résumé des commandes principales** :

```sh
# 0. Initialiser l'environnement Python (une seule fois)
uv init
uv sync

# 1. Lancer l'infrastructure
sudo docker-compose up -d

# 2. Configurer MinIO (via interface web http://localhost:9000)

# 3. Télécharger les données
cd ex01_data_retrieval
sbt run
cd ..

# 4. Vérifier PostgreSQL
sudo docker exec -it postgres psql -U postgres -d bigdata_db
# (puis requêtes SQL ci-dessus)

# 5. Insérer les données nettoyées
cd ex02_data_ingestion
sbt run
cd ..

# 6. Vérifier l'insertion
sudo docker exec -it postgres psql -U postgres -d bigdata_db
# (puis requêtes SQL ci-dessus)

# 7. Lancer le dashboard
cd ex04_dashboard
uv run streamlit run app.py
```

---

## Code minimal pour Spark + MinIO

```scala
import org.apache.spark.sql.{SparkSession, DataFrame}

object SparkApp extends App {
  val spark = SparkSession.builder()
    .appName("SparkApp")
    .master("local")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000/")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "6000")
    .config("spark.hadoop.fs.s3a.connection.timeout", "5000")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
}
```

---

## Nettoyage et Dépannage

### Vider MinIO (si stockage plein)

```sh
sudo docker exec -it minio mc rb --force minio/nyc-yellow-tripdata
```

### Redémarrer la BDD PostgreSQL

```sh
sudo docker-compose down postgres
sudo docker-compose up -d postgres
sleep 10
```

### Tout recommencer

```sh
sudo docker-compose down
sudo docker volume prune
sudo docker-compose up -d
```

---

## Modalités de rendu

1. Pull Request vers la branch `master`
2. Dépot du rapport et du code source zippé dans cours.cyu.fr (Les accès seront bientôt ouverts)

Date limite de rendu : 7 février 2026