import unittest
import sys
import os

# Ajout du répertoire parent au path pour pouvoir importer le scraper
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scrapers.news_scraper import scrape_gorafi, scrape_lemonde

class TestNewsScraper(unittest.TestCase):

    def test_scrape_gorafi_structure(self):
        """Test que le scraping du Gorafi ramène bien une liste et contient les champs attendus."""
        articles = scrape_gorafi()
        self.assertIsInstance(articles, list)
        if len(articles) > 0:
            article = articles[0]
            self.assertIn("title", article)
            self.assertIn("summary", article)
            self.assertEqual(article.get("source"), "Gorafi")

if __name__ == '__main__':
    unittest.main()