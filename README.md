

# Spark E-Commerce Lab

Ce projet contient des exemples de partitionnement personnalisé avec PySpark pour un laboratoire e-commerce.

## Fonctionnalités
- Utilisation de différents partitioners :
  - Hash par défaut
  - Range (par ID client)
  - Modulo (répartition uniforme)
  - Géographique (par pays)
  - Custom Hash (MD5 pour clés string)
- Analyse de la distribution des données par partition
- Comparaison des performances des différents partitioners
- Cas pratiques : priorité, géographie, time-series

## Fichier principal
- `lab2_custom_partitioner.py` : script Python complet avec toutes les démonstrations

## Usage
1. Cloner le dépôt :
```bash
git clone https://github.com/ibti12/RDD.git
````

2. Aller dans le dossier :

```bash
cd spark-ecommerce-lab
```

3. Exécuter le script :

```bash
python lab2_custom_partitioner.py
```

## Remarques

* Nécessite PySpark installé (`pip install pyspark`)
* Compatible Python 3.10+

## Auteur

* Ibti12

```
