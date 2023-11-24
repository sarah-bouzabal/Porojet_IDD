# Rapport de Projet - Entrepôt de Données

## Introduction

Le projet "Entrepôt de Données" vise à mettre en place un entrepôt de données pour analyser les passages aux urgences liés à la COVID-19. Ce rapport détaille les étapes du projet, les outils utilisés, et explique l'utilité de chaque outil dans le contexte du projet.

## Objectif du Projet

L'objectif principal est de répondre à des questions spécifiques concernant les passages aux urgences pour suspicion de COVID-19 en analysant les données disponibles. Cela implique la modélisation d'un entrepôt de données, l'extraction des données sources, les transformations nécessaires, la création de tables de l'entrepôt, et l'alimentation de celui-ci.

## Étapes du Projet

### 1. Analyse des Sources de Données

Avant de commencer, une analyse des sources de données a été réalisée pour comprendre la nature des informations disponibles. Cela inclut l'examen des fichiers CSV, du fichier JSON, et des métadonnées.

### 2. Modélisation de l'Entrepôt de Données

Un schéma en étoile a été choisi pour modéliser l'entrepôt de données. Les tables de faits et de dimensions ont été définies en fonction des questions spécifiques à répondre.

### 3. Nettoyage des Données

Une vérification des données a été effectuée pour identifier les éventuelles incohérences ou valeurs manquantes. Des décisions ont été prises quant au nettoyage nécessaire pour garantir la qualité des données.

### 4. Mise en Place de l'Environnement Docker

Docker a été utilisé pour créer un environnement isolé, assurant la portabilité du projet. Cela facilite le partage et l'exécution cohérente du code sur différentes machines.

### 5. Utilisation de DBeaver pour l'Exploration des Données

DBeaver a été choisi comme outil de gestion de base de données pour explorer les données, exécuter des requêtes SQL et visualiser les résultats. Cela simplifie l'analyse des données pendant le développement.

### 6. Création d'un DAG Airflow

Airflow a été utilisé pour automatiser le processus d'extraction, transformation et chargement (ETL). Un DAG (Directed Acyclic Graph) a été créé pour définir les dépendances entre les différentes tâches du flux de travail.

### 7. Ajout de Fonctionnalités au DAG

Des fonctionnalités supplémentaires ont été ajoutées au DAG, telles que des tests de qualité sur les données sources, ainsi qu'un callback pour envoyer un email en cas d'échec ou de réussite du DAG.

### 8. Organisation du Projet avec Git

Un repository Git a été créé pour organiser le code du projet. Un fichier `readme.md` fournit des instructions détaillées sur la procédure à suivre pour lancer le DAG Airflow.

## Utilité des Outils Utilisés

### Docker

Docker permet d'encapsuler l'ensemble de l'environnement de développement, garantissant une exécution cohérente indépendamment du système d'exploitation. Il simplifie également le déploiement et la gestion des dépendances.

### DBeaver

DBeaver offre une interface utilisateur conviviale pour explorer et interagir avec différentes bases de données. C'est un outil essentiel pour comprendre la structure des données et tester des requêtes SQL.

![Capture d'écran 2023-11-24 180359](https://github.com/sarah-bouzabal/Porojet_IDD/assets/96074783/e451f07a-1247-46c6-a783-0a917ead34f9)


### Airflow

Airflow est un outil puissant pour la gestion des workflows et l'automatisation des tâches ETL. En définissant un DAG, les différentes étapes du processus sont orchestrées de manière efficace et planifiée.

![Capture d'écran 2023-11-24 192218](https://github.com/sarah-bouzabal/Porojet_IDD/assets/96074783/8cc5fa7c-8543-4bc7-8915-a6b2b569c40b)


## Conclusion

Le projet "Entrepôt de Données" combine l'utilisation judicieuse de Docker, DBeaver et Apache Airflow pour créer un environnement de développement robuste, explorer les données, et automatiser le flux de travail ETL. Ces outils contribuent à la réussite du projet en assurant la cohérence, la qualité et l'efficacité du processus de gestion des données.

