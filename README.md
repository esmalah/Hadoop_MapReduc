# Hadoop MapReduce and HDFS Tp

## Introduction
Ce projet utilise Hadoop pour gérer des fichiers dans HDFS et exécuter des tâches MapReduce. Toutes les opérations (HDFS et MapReduce) sont automatisées dans un deux fixhiers script Python.

---

## Prérequis
- **Hadoop** configuré et actif
- **Python 3.x** installé
- **HDFS** opérationnel

---

## Instructions
Exécutez simplement le script principal pour charger les fichiers, effectuer les tâches HDFS et lancer les programmes MapReduce.

```bash
python3 Hdfs_MapReduce.py

## Structure
Projet/
├── Input_files/             # Fichiers d'entrée (ex: logs.txt, sales.txt, etc.)
├── hdfs_commands.sh         # Commandes Bash pour HDFS
├── Hdfs_MapReduce.py        # Script principal (HDFS + MapReduce)
