# Sujet

## Objectif

Aider à identifier les news légitimes/vérifiées pour aider à anticiper l'évolution de certains marchés financiers.

## Sources de données

* [Fact Check AFP](https://factcheck.afp.com/)
* [module local de détection](https://github.com/josumsc/fake-news-detector) de fake news basé sur un modèle de machine learning
* [Gorafi](https://www.legorafi.fr/)
* 2 au choix : Nouvelles sur page d'accueil de journaux nationaux/internationaux (Le Monde, AFP)

## Contraintes

Traiter les informations en temps réel : extraire les données utiles pour la création et la vérification dans la base de l'entreprise 

* title
* résumé de quelques mots
* date d'occurence de l'évènement
* date de publication de la news

Permettre de retraiter très rapidement toutes les 6 heures toutes les informations collectées depuis le début du projet.

La vérification de l'information doit être au maximum automatisée.

## Architecture du Pipeline (Documentation Utilisateurs)

Ce projet implémente une architecture Big Data répondant aux besoins des équipes Data Science et Business Intelligence (B.I.) :

1. **Ingestion (Temps réel)** : Des scrapers Python récupèrent les articles et les publient dans **Apache Kafka**.
2. **Traitement Découplé (Spark Connect)** : Un serveur **Apache Spark** s'exécute dans un conteneur Docker. Le script client PySpark s'y connecte via gRPC (port 15002) pour consommer Kafka et orchestrer le pipeline.
3. **Machine Learning** : À la volée, les articles en français sont traduits en anglais via `deep-translator` puis envoyés à une API de classification ML conteneurisée pour détecter les Fake News.
4. **Stockage B.I. (PostgreSQL)** : Les données structurées sont insérées dans une base de données relationnelle pour l'équipe B.I. (accès `jdbc:postgresql://localhost:5433/finance_news`).
5. **Data Lake (MinIO / S3)** : L'historique brut est stocké sous forme de fichiers JSON dans MinIO, idéal pour l'exploration et l'entraînement de modèles par les Data Scientists (bucket `s3a://data-lake/raw-news/`).

## Déploiement et Exécution

Pour déployer l'architecture et lancer le pipeline, suivez ces étapes :

1. Démarrez l'infrastructure (Postgres, Kafka, MinIO, Airflow, Spark Connect, et le Modèle ML) :
   ```bash
   docker compose up -d --build
   ```
2. Installez les dépendances PySpark sur la machine cliente locale :
   ```bash
   pip install pyspark==3.5.3
   ```
3. Exécutez le script client Spark (qui se connectera au cluster Spark Connect) :
   ```bash
   python jobs/spark_process_news.py
   ```

## Tests et Surveillance

* **Tests** : Des tests unitaires sont disponibles dans le dossier `tests/` pour valider l'intégrité de l'extraction des données. Il est possible de lancer la commande `python -m unittest discover tests/`.
* **Surveillance** : Le job Spark implémente une gestion des erreurs par micro-batch, affichée dans les logs de la console. Le pipeline est conçu pour être orchestré ultérieurement via Apache Airflow.