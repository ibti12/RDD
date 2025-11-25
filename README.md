
# Spark E-Commerce Lab - RDD Exercises

Ce dépôt contient plusieurs scripts et fichiers relatifs à l'apprentissage des **RDD et des partitionnements avec PySpark**, dans le cadre du laboratoire e-commerce.

---

## Contenu du dépôt

### Scripts Python

1. **lab2_custom_partitioner.py**
   - Démonstration de différents partitioners personnalisés :
     - **Default Hash Partitioner** : distribution standard
     - **Range Partitioner** : regroupement par ID client
     - **Modulo Partitioner** : répartition uniforme
     - **Geography-Based Partitioner** : par pays
     - **Custom Hash Partitioner** : MD5 pour distribution uniforme
     - **Priority-Based Partitioner** : exemple d’usage métier
   - Comparaison de performances pour l’agrégation des commandes.
   - Analyse des bénéfices de chaque partitionneur.

2. **lab2_partitions.py**
   - Expérimentations sur les partitions des RDD.
   - Observation de la distribution des données dans chaque partition.
   - **Output inclus** : fichiers `Output from lab2_partitions.py with observations`.

3. **lab2_map_operations.py**
   - Exercices sur les opérations `map()` et `mapPartitions()`.
   - Compréhension de la différence entre traitement par élément et traitement par partition.
   - **Output inclus** : fichier `Output from lab2_map_operations.py`.

4. **lab2_map_practice.py**
   - Contient 5 exercices complétés (+ bonus) sur `map()` et transformations RDD.
   - Solutions optimisées pour la pratique.

---

## Observations et Notes

- **Quand utiliser `map()` vs `mapPartitions()` :**  
  - `map()` : transformation simple élément par élément, facile à comprendre et utiliser.  
  - `mapPartitions()` : traitement par partition complète, réduit l’overhead et améliore les performances pour des opérations lourdes.  
  - Idéal pour des calculs qui peuvent être vectorisés ou accumulés par partition, comme des agrégations locales avant un `reduceByKey`.  

- **Optimal partition sizing :**
  - Trop peu de partitions → certaines partitions deviennent très grandes → surcharge mémoire et latence.  
  - Trop de partitions → overhead du scheduler augmente → ralentissement.  
  - Observation pratique : choisir un nombre de partitions proche du **nombre de cores disponibles × 2 à 4** pour un bon équilibre entre parallélisme et overhead.

---

## Usage

1. Cloner le dépôt :

```bash
git clone https://github.com/ibti12/RDD.git
````

2. Se placer dans le dossier :

```bash
cd spark-ecommerce-lab
```

3. Installer PySpark si nécessaire :

```bash
pip install pyspark
```

4. Lancer les scripts selon vos besoins :

```bash
python lab2_custom_partitioner.py
python lab2_partitions.py
python lab2_map_operations.py
python lab2_map_practice.py
```

---

## Auteur

* Ibti12



Veux‑tu que je fasse ça ?
```
