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