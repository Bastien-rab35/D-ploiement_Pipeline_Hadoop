-- Script d'initialisation de la base de données
-- Permet de pré-créer la table et les index pour optimiser les performances (EID4.5)

CREATE TABLE IF NOT EXISTS news_articles (
    source TEXT,
    title TEXT,
    summary TEXT,
    event_date TEXT,
    publish_date TEXT,
    is_fake BOOLEAN
);

-- Index pour accélérer les requêtes analytiques de la B.I. (Metabase)
CREATE INDEX IF NOT EXISTS idx_news_publish_date ON news_articles (publish_date);
CREATE INDEX IF NOT EXISTS idx_news_source ON news_articles (source);
CREATE INDEX IF NOT EXISTS idx_news_is_fake ON news_articles (is_fake);
