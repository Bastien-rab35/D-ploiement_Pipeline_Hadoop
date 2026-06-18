import unittest
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scrapers.news_scraper import scrape_rss_feed

class TestNewsScraper(unittest.TestCase):

    # On "mock" (intercepte) la fonction requests.get utilisée dans news_scraper
    @patch('scrapers.news_scraper.requests.get')
    def test_scrape_rss_feed_structure(self, mock_get):
        """Vérifie que la fonction RSS renvoie bien la bonne structure de données à l'aide d'un Mock (sans requête internet)."""
        
        # Création d'une fausse réponse XML imitant un flux RSS
        fausse_reponse_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
            <channel>
                <item>
                    <title>Titre de test unitaire</title>
                    <description>Ceci est un résumé de test unitaire</description>
                    <pubDate>Mon, 01 Jan 2024 12:00:00 +0000</pubDate>
                </item>
            </channel>
        </rss>
        """
        
        # Configuration du mock pour qu'il renvoie notre faux XML
        mock_response = MagicMock()
        mock_response.content = fausse_reponse_xml.encode('utf-8')
        mock_get.return_value = mock_response

        # Exécution de la fonction avec une fausse URL (qui ne sera jamais appelée)
        articles = scrape_rss_feed("http://url-fictive.com", "MockSource", limit=5)
        
        self.assertIsInstance(articles, list)
        self.assertEqual(len(articles), 1)
        
        article = articles[0]
        self.assertEqual(article.get("title"), "Titre de test unitaire")
        # Le scraper ajoute "..." à la fin du résumé
        self.assertEqual(article.get("summary"), "Ceci est un résumé de test unitaire...")
        self.assertEqual(article.get("source"), "MockSource")

    def test_is_valid_iso_date(self):
        """Vérifie que la fonction de validation de date distingue correctement les formats ISO valides et invalides (EID4.4)."""
        from jobs.pipeline_monitor import is_valid_iso_date
        self.assertTrue(is_valid_iso_date("2026-06-18T08:54:53"))
        self.assertTrue(is_valid_iso_date("2026-06-18T08:54:53Z"))
        self.assertFalse(is_valid_iso_date("2026-18-06")) # Inversion mois/jour
        self.assertFalse(is_valid_iso_date("18 juin 2026")) # Format textuel
        self.assertFalse(is_valid_iso_date(None))
        self.assertFalse(is_valid_iso_date(""))

if __name__ == '__main__':
    unittest.main()