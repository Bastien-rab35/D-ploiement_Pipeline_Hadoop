import json
import time
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from datetime import datetime
import os
from email.utils import parsedate_to_datetime

# Configuration de Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9093')
TOPIC_NAME = 'raw_news'

def create_producer():
    """Initialise le producteur Kafka pour envoyer du JSON."""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def scrape_rss_feed(url, source_name, limit=30):
    """
    Fonction générique pour scraper les flux RSS et respecter le principe DRY.
    """
    print(f"Scraping {source_name}...")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, features="xml")
    
    articles = []
    items = soup.findAll('item')
    
    for item in items[:limit]:
        title = item.find('title').text if item.find('title') else ""
        summary = item.find('description').text[:200] + "..." if item.find('description') else ""
        pub_date_str = item.find('pubDate').text if item.find('pubDate') else ""
        
        # Tente de convertir la date RFC du flux RSS en format ISO (EID4.4)
        try:
            parsed_date = parsedate_to_datetime(pub_date_str).isoformat()
        except Exception:
            parsed_date = datetime.now().isoformat()
        
        article_data = {
            "source": source_name,
            "title": title,
            "summary": summary,
            "event_date": parsed_date, 
            "publish_date": parsed_date
        }
        articles.append(article_data)
        
    return articles

def scrape_afp_factcheck():
    """Scrape le HTML de l'AFP car ils n'ont pas de flux RSS standard."""
    print("Scraping Fact Check AFP...")
    url = "https://factcheck.afp.com/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    
    articles = []
    for item in soup.find_all('article')[:20]:
        title_tag = item.find(['h3', 'h4'])
        title = title_tag.text.strip() if title_tag else "Titre indisponible"
        
        article_data = {
            "source": "Fact Check AFP",
            "title": title,
            "summary": "Vérification de faits par l'AFP...",
            "event_date": datetime.now().isoformat(),
            "publish_date": datetime.now().isoformat()
        }
        articles.append(article_data)
    return articles

def scrape_wp_json_api(url, source_name, limit=10):
    """Scrape une API REST native WordPress (JSON) EID4.4"""
    print(f"Scraping API JSON {source_name}...")
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        
        articles = []
        for item in data[:limit]:
            # Nettoyage des balises HTML du titre et résumé renvoyés par WordPress
            title = BeautifulSoup(item.get('title', {}).get('rendered', ''), "html.parser").text
            summary = BeautifulSoup(item.get('excerpt', {}).get('rendered', ''), "html.parser").text[:200] + "..."
            
            article_data = {
                "source": source_name,
                "title": title,
                "summary": summary,
                "event_date": item.get('date', datetime.now().isoformat()),
                "publish_date": item.get('date', datetime.now().isoformat())
            }
            articles.append(article_data)
        return articles
    except Exception as e:
        print(f"Erreur API JSON {source_name}: {e}")
        return []

def run_all_scrapers():
    """Exécute tous les scrapers et envoie les données à Kafka."""
    producer = create_producer()
    
    news_data = []
    
    news_data.extend(scrape_rss_feed("https://www.legorafi.fr/feed/", "Gorafi"))
    news_data.extend(scrape_rss_feed("https://www.lemonde.fr/rss/une.xml", "Le Monde"))
    news_data.extend(scrape_rss_feed("https://www.francetvinfo.fr/titres.rss", "France Info"))
    
    # Nouvelles sources via API (JSON) plutôt que RSS
    news_data.extend(scrape_wp_json_api("https://www.factcheck.org/wp-json/wp/v2/posts", "FactCheck.org (API)"))
    
    news_data.extend(scrape_afp_factcheck())
    
    for article in news_data:
        producer.send(TOPIC_NAME, value=article)
        print(f"Envoyé dans Kafka : {article['title']}")
        time.sleep(1) 
        
    producer.flush()
    print("Terminé !")

if __name__ == "__main__":
    run_all_scrapers()