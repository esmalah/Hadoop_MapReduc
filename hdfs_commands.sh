#!/bin/bash

# Créer un répertoire dans HDFS pour les fichiers d'entrée
hadoop fs -mkdir -p /data/input

# Charger les fichiers locaux dans HDFS
hadoop fs -put input_files/* /data/input/

# Vérifier les fichiers dans HDFS
hadoop fs -ls /data/input
