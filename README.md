Spark RDD – Lab 2

Ce dépôt regroupe l’ensemble des scripts réalisés dans le cadre du Lab 2 dédié aux RDD (Resilient Distributed Datasets) avec Apache Spark.
L’objectif est de comprendre en profondeur le fonctionnement des RDD, leurs transformations, leurs actions et les mécanismes d’optimisation.

🚀 Objectifs du Lab

Comprendre la structure et le rôle des RDD dans Spark.

Manipuler les transformations essentielles : map, flatMap, filter, reduceByKey, etc.

Travailler avec les Pair RDDs (RDDs clé-valeur).

Gérer le partitionnement personnalisé et analyser son impact.

Explorer la persistance, le caching et leurs performances.

Détecter et corriger les problèmes de skew (déséquilibre des données).

Développer une intuition sur la performance des traitements distribués.

📁 Contenu du Lab 2
🔹 Exploration & Manipulation

lab2_explore_data.py — Découverte du dataset + opérations basiques.

lab2_filter.py — Filtrage simple et avancé.

lab2_map_operations.py — Transformations courantes (map, flatMap…).

lab2_map_practice.py — Exercices pratiques.

🔹 Pair RDD & Joins

lab2_keyvalue.py — Manipulations clé-valeur (pair RDD).

lab2_joins.py — Différents types de jointures entre RDDs.

🔹 Partitionnement & Optimisation

lab2_partitions.py — Analyse, création et optimisation des partitions.

lab2_custom_partitioner.py — Implémentation d’un partitionneur personnalisé.

lab2_skew.py — Détection et correction du data skew.

lab2_performance_challenge.py — Défi de performance (approche + résolution).

🔹 Persistance

lab2_persistence.py — Cache, persist, memory/disk, et impact réel.

🔹 Outputs

lab2_output.txt — Résultats produits par certains scripts.

🛠️ Technologies utilisées

PySpark

Apache Spark

Python 3.x

📦 Prérequis

Avant d’exécuter les scripts :

pip install pyspark


Assurez-vous également que Java (JDK 8 ou +) est installé.

▶️ Exécution d’un script
spark-submit lab2_map_operations.py


Ou dans un notebook :

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD-Lab2").getOrCreate()

📚 Ce que vous apprendrez

✔ Les bases solides des RDD
✔ Les transformations clés et leurs comportements
✔ Le fonctionnement interne du partitionnement
✔ Les bonnes pratiques de performance Spark
✔ La gestion du caching et de la persistance
✔ La manipulation des données distribuées à grande échelle
