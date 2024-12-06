# ----------------------------------------------
# PARTIE 1 : COMMANDES HDFS
# ----------------------------------------------


# Exercice 1 : Filtrage des données

#!/bin/bash
INPUT_DIR=$1
OUTPUT_FILE=$2
KEYWORD=$3
# Rep de sortie
hadoop fs -mkdir -p $(dirname $OUTPUT_FILE)
# Filtrage des lignes contenant le mot-clé
hadoop fs -cat $INPUT_DIR/* | grep "$KEYWORD" | hadoop fs -put - $OUTPUT_FILE
echo "Les lignes contenant '$KEYWORD' ont été extraites dans $OUTPUT_FILE."

# Commande exécutée :
./filter_keyword.sh /data/input /data/output/output.txt hello
# Resultas :
hello monsieur
ok hello toto
hello madame


# Eercice 2 : Fusionner plusieurs fichiers

#!/bin/bash
INPUT_DIR=$1
OUTPUT_FILE=$2
# Fusion
hadoop fs -cat $INPUT_DIR/* | hadoop fs -put - $OUTPUT_FILE
echo "Les fichiers de $INPUT_DIR ont été fusionnés dans $OUTPUT_FILE."

# Commande exécutée :
./merge_files.sh /data/input /data/output/merged.txt

