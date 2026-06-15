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
2. **Traitement (Apache Spark)** : Un job Spark Streaming consomme les messages Kafka, structure les données et les distribue.
3. **Stockage B.I. (PostgreSQL)** : Les données structurées sont insérées dans une base de données relationnelle pour l'équipe B.I. (accès `jdbc:postgresql://localhost:5432/finance_news`).
4. **Data Lake (MinIO / S3)** : L'historique brut est stocké sous forme de fichiers JSON dans MinIO, idéal pour l'exploration et l'entraînement de modèles par les Data Scientists (bucket `s3a://data-lake/raw-news/`).

## Tests et Surveillance

* **Tests** : Des tests unitaires sont disponibles dans le dossier `tests/` pour valider l'intégrité de l'extraction des données depuis les sources. Exécutez `python -m unittest discover tests/`.
* **Surveillance** : Le job Spark implémente une gestion des erreurs par micro-batch, affichée dans les logs de la console. Le pipeline est conçu pour être orchestré ultérieurement via Apache Airflow.