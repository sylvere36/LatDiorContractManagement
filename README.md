# Projet Spark - Service Vente

Ce projet utilise Scala pour manipuler et analyser des données de vente. Les différentes étapes du traitement incluent la lecture des données à partir de divers formats (CSV, JSON), des transformations pour préparer les données, et des tests unitaires pour vérifier la précision des transformations appliquées.

## Structure du Projet

### Dossiers et Fichiers Principaux
- **src/main/scala/sda/reader** : Contient les classes pour lire les fichiers de données.
    - `CsvReader.scala` : Lit les fichiers CSV et retourne un DataFrame Spark.
    - `JsonReader.scala` : Lit les fichiers JSON et retourne un DataFrame Spark.
    - `XmlReader.scala` : Lit les fichiers XML et retourne un DataFrame Spark.

- **src/main/scala/sda/traitement** : Contient les classes de traitement des données.
    - `ServiceVente.scala` : Définit des transformations sur les données, telles que le calcul du montant TTC, l'extraction des informations des contrats, et l'ajout d'un statut de contrat.

- **src/test/scala/sda/traitement** : Contient les tests unitaires des classes de traitement.
    - `ServiceVenteTest.scala` : Tests unitaires pour les différentes méthodes de la classe `ServiceVente`.

## Fonctionnalités

1. **Lecture des Données**
    - Les classes `CsvReader`, `JsonReader`, et `XmlReader` sont utilisées pour lire des données à partir de différents formats de fichiers et les charger dans un DataFrame Spark.

2. **Transformations des Données**
    - **Formatter** : La méthode `formatter()` divise la colonne `HTT_TVA` en deux nouvelles colonnes `HTT` (hors taxes) et `TVA` (taxe).
    - **Calcul du TTC** : La méthode `calculTTC()` calcule le montant total TTC en utilisant les colonnes `HTT` et `TVA`.
    - **Extraction des Informations de Contrat** : La méthode `extractDateEndContratVille()` extrait les informations sur la date de fin du contrat et la ville du client à partir de la colonne `MetaData`.
    - **Statut du Contrat** : La méthode `contratStatus()` ajoute une colonne `Contrat_Status` pour indiquer si le contrat est expiré ou actif.

3. **Tests Unitaires**
    - Les tests unitaires, situés dans `ServiceVenteTest.scala`, vérifient le bon fonctionnement des transformations des données.
    - Chaque test crée un DataFrame d'exemple, applique une transformation, et vérifie que le résultat est conforme aux attentes.

## Installation et Exécution

### Prérequis
- **Java** : JDK 8 ou supérieur doit être installé.
- **Scala** : Le projet est basé sur Scala, assurez-vous qu'il est correctement installé.
- **SBT** : Utilisé pour la compilation et la gestion des dépendances.

### Étapes d'Installation
1. **Cloner le Dépôt**
   ```sh
   git clone <url_du_dépôt>
   cd <nom_du_dépôt>
   ```

2. **Compiler le Projet**
   ```sh
   sbt compile
   ```

3. **Exécuter les Tests**
   ```sh
   sbt test
   ```

## Utilisation
Le point d'entrée principal du projet est la classe `MainBatch`. Elle utilise les lecteurs de fichiers pour charger les données et applique les transformations définies dans `ServiceVente`. Pour exécuter le traitement complet, utilisez SBT ou IntelliJ pour lancer `MainBatch`.

## Contribuer
Les contributions sont les bienvenues. Si vous souhaitez ajouter des fonctionnalités ou corriger des bogues, merci de créer une branche à partir de `main` et de soumettre une Pull Request.

## Auteur
- **AKAMBI Olouwashegun Sylvère** 
- **OUOBA Kampari Diomède**
- **Oumou Kadija Goudiaby** 
- **Esso-Lizam ASSOTI**

