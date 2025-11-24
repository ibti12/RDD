# LAB4 - Spark E-commerce Log Analysis

## Description
Ce projet est un laboratoire d'analyse de logs web pour un site e-commerce en utilisant **Apache Spark**.  
Il comprend la lecture, le parsing, le traitement, l'analyse statistique, et la visualisation des données de logs.

Le projet inclut également la résolution de plusieurs challenges analytiques typiques : taux de rebond, funnel de conversion, heures de pointe, détection des IPs suspectes et identification des endpoints lents.

---

## Contenu du projet

| Fichier | Description |
|---------|-------------|
| `lab4_log_analysis.py` | Script de base pour l'analyse des logs (parsing, statistiques simples). |
| `lab4_challenges.py` | Scripts pour résoudre les challenges analytiques spécifiques. |
| `lab4_full_analysis.py` | Script complet combinant parsing, statistiques et challenges. |
| `lab4_full_analysis_visual.py` | Version complète avec **visualisations** (Matplotlib/Seaborn) pour le rapport. |
| `lab4_caching_demo.py` | Démonstration de caching avec Spark pour améliorer les performances. |
| `generate_logs.py` | Script pour générer des logs simulés pour tester les scripts. |
| `spark-data/ecommerce/` | Contient les logs bruts et les fichiers traités (CSV et Parquet). |
| `*.png` | Graphiques générés par `lab4_full_analysis_visual.py` : trafic horaire, funnel, endpoints lents. |

---

## Prérequis

- Python 3.8+  
- Apache Spark 3.x  
- Bibliothèques Python : `pyspark`, `matplotlib`, `seaborn`, `pandas`

Installation rapide des dépendances :

```bash
pip install pyspark matplotlib seaborn pandas
