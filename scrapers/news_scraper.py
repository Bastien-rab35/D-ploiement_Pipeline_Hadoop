import json
import time
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from datetime import datetime
import os

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
        pub_date = item.find('pubDate').text if item.find('pubDate') else ""
        
        article_data = {
            "source": source_name,
            "title": title,
            "summary": summary,
            "event_date": datetime.now().isoformat(), 
            "publish_date": pub_date
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

def run_all_scrapers():
    """Exécute tous les scrapers et envoie les données à Kafka."""
    producer = create_producer()
    
    news_data = []
    
    news_data.extend(scrape_rss_feed("https://www.legorafi.fr/feed/", "Gorafi"))
    news_data.extend(scrape_rss_feed("https://www.lemonde.fr/rss/une.xml", "Le Monde"))
    news_data.extend(scrape_rss_feed("https://www.francetvinfo.fr/titres.rss", "France Info"))
    news_data.extend(scrape_afp_factcheck())
    
    for article in news_data:
        producer.send(TOPIC_NAME, value=article)
        print(f"Envoyé dans Kafka : {article['title']}")
        time.sleep(1) 
        
    producer.flush()
    print("Terminé !")

if __name__ == "__main__":
    run_all_scrapers()