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
    """Initialise le producteur Kafka qui enverra les messages en JSON."""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def scrape_gorafi():
    """Scrape le flux RSS du Gorafi pour extraire les informations requises."""
    print("Scraping Le Gorafi...")
    url = "https://www.legorafi.fr/feed/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, features="xml")
    
    articles = []
    items = soup.findAll('item')
    
    for item in items[:5]: # On limite aux 5 premiers pour tester
        # Extraction des champs requis
        title = item.find('title').text if item.find('title') else ""
        summary = item.find('description').text[:200] + "..." if item.find('description') else ""
        pub_date = item.find('pubDate').text if item.find('pubDate') else ""
        
        # Assemblage de l'objet de données
        article_data = {
            "source": "Gorafi",
            "title": title,
            "summary": summary,
            # La date d'occurrence exacte est souvent absente des flux, on utilise la date courante pour l'exemple
            "event_date": datetime.now().isoformat(), 
            "publish_date": pub_date
        }
        articles.append(article_data)
        
    return articles

def scrape_lemonde():
    """Scrape le flux RSS du Monde pour extraire les informations requises."""
    print("Scraping Le Monde...")
    url = "https://www.lemonde.fr/rss/une.xml"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, features="xml")
    
    articles = []
    items = soup.findAll('item')
    
    for item in items[:5]: # On limite aux 5 premiers pour tester
        title = item.find('title').text if item.find('title') else ""
        summary = item.find('description').text[:200] + "..." if item.find('description') else ""
        pub_date = item.find('pubDate').text if item.find('pubDate') else ""
        
        article_data = {
            "source": "Le Monde",
            "title": title,
            "summary": summary,
            "event_date": datetime.now().isoformat(), 
            "publish_date": pub_date
        }
        articles.append(article_data)
        
    return articles

def scrape_afp_factcheck():
    """Scrape la page d'accueil de Fact Check AFP (HTML) pour extraire les articles."""
    print("Scraping Fact Check AFP...")
    url = "https://factcheck.afp.com/"
    response = requests.get(url)
    # On utilise html.parser car ce n'est pas un flux RSS (XML) mais une page web standard
    soup = BeautifulSoup(response.content, "html.parser")
    
    articles = []
    # On cherche les cartes d'articles sur la page d'accueil
    for item in soup.find_all('article')[:5]:
        title_tag = item.find(['h3', 'h4'])
        title = title_tag.text.strip() if title_tag else "Titre indisponible"
        
        article_data = {
            "source": "Fact Check AFP",
            "title": title,
            "summary": "Vérification de faits par l'AFP...", # Simplification, le résumé nécessite souvent d'ouvrir l'article
            "event_date": datetime.now().isoformat(),
            "publish_date": datetime.now().isoformat()
        }
        articles.append(article_data)
    return articles

if __name__ == "__main__":
    producer = create_producer()
    
    # 1. On récupère les données
    news_data = scrape_gorafi()
    news_data.extend(scrape_lemonde())
    news_data.extend(scrape_afp_factcheck())
    
    # 2. On envoie chaque article dans Kafka
    for article in news_data:
        producer.send(TOPIC_NAME, value=article)
        print(f"Envoyé dans Kafka : {article['title']}")
        time.sleep(1) # Pause visuelle pour voir le flux
        
    producer.flush() # S'assure que tout est bien transmis avant de fermer
    print("Terminé !")