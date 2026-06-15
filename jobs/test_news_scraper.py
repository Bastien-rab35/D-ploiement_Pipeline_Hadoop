import unittest
import sys
import os

# Ajout du répertoire parent au path pour pouvoir importer le scraper
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scrapers.news_scraper import scrape_rss_feed

class TestNewsScraper(unittest.TestCase):

    def test_scrape_gorafi_structure(self):
        """Test que la fonction générique RSS fonctionne et ramène les bons champs."""
        articles = scrape_rss_feed("https://www.legorafi.fr/feed/", "Gorafi", limit=5)
        self.assertIsInstance(articles, list)
        if len(articles) > 0:
            article = articles[0]
            self.assertIn("title", article)
            self.assertIn("summary", article)
            self.assertEqual(article.get("source"), "Gorafi")

if __name__ == '__main__':
    unittest.main()