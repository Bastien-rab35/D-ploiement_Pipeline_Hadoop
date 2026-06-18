#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de Surveillance du Pipeline (EID4.4)
Vérifie la santé des services et effectue des contrôles de qualité des données (Data Quality) sur PostgreSQL.
"""

import json
import socket
import sys
from datetime import datetime, timedelta

# Configuration des hôtes et des ports à surveiller
# Remarque : si exécuté depuis l'hôte, on utilise localhost.
# Si exécuté depuis le conteneur Airflow (dans le réseau Docker), on utilise les noms des services compose.
import os
RUNNING_IN_DOCKER = os.path.exists('/.dockerenv')

SERVICES = {
    "Zookeeper": {"host": "zookeeper" if RUNNING_IN_DOCKER else "localhost", "port": 2181},
    "Kafka Broker": {"host": "kafka" if RUNNING_IN_DOCKER else "localhost", "port": 9092 if RUNNING_IN_DOCKER else 9093},
    "PostgreSQL": {"host": "postgres" if RUNNING_IN_DOCKER else "localhost", "port": 5432 if RUNNING_IN_DOCKER else 5433},
    "MinIO Data Lake": {"host": "minio" if RUNNING_IN_DOCKER else "localhost", "port": 9000},
    "Fake News ML API": {"host": "fake-news-detector" if RUNNING_IN_DOCKER else "localhost", "port": 5000 if RUNNING_IN_DOCKER else 5001},
    "Spark Connect": {"host": "spark-connect" if RUNNING_IN_DOCKER else "localhost", "port": 15002},
}

DB_CONFIG = {
    "dbname": "finance_news",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres" if RUNNING_IN_DOCKER else "localhost",
    "port": 5432 if RUNNING_IN_DOCKER else 5433
}

def is_valid_iso_date(date_str):
    """Vérifie si une chaîne de date est au format ISO 8601 valide (EID4.4)."""
    if not date_str:
        return False
    try:
        datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False

def check_tcp_port(host, port):
    """Vérifie si un port TCP est ouvert et accessible."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(3.0)
            s.connect((host, port))
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

def check_services_health():
    """Vérifie la disponibilité de tous les services de la plateforme."""
    print("=== [1/2] Vérification de la disponibilité des services ===")
    status = {}
    all_healthy = True
    
    for name, config in SERVICES.items():
        is_up = check_tcp_port(config["host"], config["port"])
        status[name] = "UP" if is_up else "DOWN"
        if not is_up:
            all_healthy = False
        print(f"  - {name} ({config['host']}:{config['port']}) : {status[name]}")
        
    return all_healthy, status

def perform_data_quality_checks():
    """
    Exécute des requêtes de validation de la qualité des données (EID4.4)
    et détecte des anomalies de représentation de données.
    """
    print("\n=== [2/2] Analyse de la Qualité des Données (Data Quality) ===")
    
    try:
        import psycopg2
    except ImportError:
        print("Avertissement : 'psycopg2' non installé. Impossible d'interroger la base de données PostgreSQL.")
        print("Installez-le avec : pip install psycopg2-binary")
        return {
            "status": "SKIPPED",
            "reason": "psycopg2 library is missing",
            "anomalies_detected": []
        }

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        anomalies = []
        metrics = {}
        
        # 1. Nombre total d'articles stockés
        cur.execute("SELECT COUNT(*) FROM news_articles;")
        total_rows = cur.fetchone()[0]
        metrics["total_records"] = total_rows
        
        # 2. Articles insérés sur les dernières 24h
        limit_date = (datetime.now() - timedelta(hours=24)).isoformat()
        cur.execute("SELECT COUNT(*) FROM news_articles WHERE event_date >= %s;", (limit_date,))
        rows_24h = cur.fetchone()[0]
        metrics["records_last_24h"] = rows_24h
        
        # 3. Détection de valeurs nulles ou vides dans les champs critiques
        cur.execute("""
            SELECT COUNT(*) FROM news_articles 
            WHERE title IS NULL OR title = '' 
               OR summary IS NULL OR summary = '' 
               OR source IS NULL OR source = '';
        """)
        nulls_count = cur.fetchone()[0]
        metrics["missing_fields_records"] = nulls_count
        if nulls_count > 0:
            anomalies.append(f"Qualité faible : {nulls_count} article(s) présente(nt) des champs vides (titre/résumé/source).")
            
        # 4. Détection de représentations incohérentes (ex: longueur du titre anormalement courte)
        cur.execute("SELECT COUNT(*) FROM news_articles WHERE LENGTH(title) < 5;")
        short_titles = cur.fetchone()[0]
        metrics["short_titles_records"] = short_titles
        if short_titles > 0:
            anomalies.append(f"Anomalie : {short_titles} article(s) avec un titre anormalement court (< 5 caractères).")
            
        # 5. Détection de dates mal formatées (non-ISO ou invalides)
        cur.execute("SELECT title, event_date FROM news_articles;")
        invalid_dates_count = 0
        for title, evt_date in cur.fetchall():
            if not is_valid_iso_date(evt_date):
                invalid_dates_count += 1
                
        metrics["invalid_dates_records"] = invalid_dates_count
        if invalid_dates_count > 0:
            anomalies.append(f"Représentation incorrecte : {invalid_dates_count} article(s) avec un format de date invalide.")

        # 6. Répartition Fake News / Vraies News
        cur.execute("SELECT is_fake, COUNT(*) FROM news_articles GROUP BY is_fake;")
        distribution = {str(k): v for k, v in cur.fetchall()}
        metrics["is_fake_distribution"] = distribution
        
        # S'il y a 0 fausses informations sur un gros volume, le classifieur ML a peut-être un bug (alerte)
        if total_rows > 10 and distribution.get("True", 0) == 0:
            anomalies.append("Alerte ML : 0 Fake News détectée sur l'ensemble de la base. Risque de faux négatifs du modèle.")
            
        cur.close()
        conn.close()
        
        # Affichage des métriques
        print(f"  - Nombre total d'articles : {total_rows}")
        print(f"  - Articles sur les dernières 24h : {rows_24h}")
        print(f"  - Enregistrements avec champs manquants : {nulls_count}")
        print(f"  - Distribution Fake News : {distribution}")
        
        if anomalies:
            print("\n  ⚠️ ANOMALIES DÉTECTÉES :")
            for anomaly in anomalies:
                print(f"    - {anomaly}")
        else:
            print("\n  ✅ Aucune anomalie détectée sur les représentations de données.")
            
        return {
            "status": "WARNING" if anomalies else "SUCCESS",
            "metrics": metrics,
            "anomalies_detected": anomalies
        }
        
    except Exception as e:
        print(f"Erreur de connexion PostgreSQL ou d'exécution de requête : {str(e)}")
        if conn:
            conn.close()
        return {
            "status": "FAILED",
            "reason": str(e),
            "anomalies_detected": ["Impossible de se connecter à la base de données PostgreSQL"]
        }

def main():
    services_healthy, services_report = check_services_health()
    data_quality_report = perform_data_quality_checks()
    
    # Compilation du rapport final
    report = {
        "timestamp": datetime.now().isoformat(),
        "services": services_report,
        "data_quality": data_quality_report
    }
    
    # Sauvegarde locale du rapport de surveillance
    with open("pipeline_status.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4, ensure_ascii=False)
    
    print("\n=== Bilan de Surveillance ===")
    if not services_healthy:
        print("❌ CRITIQUE : Un ou plusieurs services indispensables sont DOWN.")
        sys.exit(1)
    elif data_quality_report.get("status") == "FAILED":
        print("❌ ERREUR : Échec de la vérification de la qualité des données.")
        sys.exit(1)
    elif data_quality_report.get("status") == "WARNING":
        print("⚠️ ATTENTION : Des anomalies mineures de qualité ont été relevées.")
        sys.exit(0)  # On ne bloque pas forcément le pipeline pour des warnings
    else:
        print("✅ SUCCESS : Le pipeline est sain et fonctionnel.")
        sys.exit(0)

if __name__ == "__main__":
    main()
