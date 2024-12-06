# ----------------------------------------------
# PARTIE 2 : MAPREDUCE
# ----------------------------------------------


#Exercice 1 : Analyse des logs
#Description : Compter les occurrences de chaque adresse IP dans un fichier de logs.

#Mapper (logs_mapper.py) :
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    ip = line.split()[0]
    print(f"{ip}\t1")

#Reducer (logs_reducer.py) :
#!/usr/bin/env python3
import sys
from collections import defaultdict

counts = defaultdict(int)

for line in sys.stdin:
    ip, count = line.strip().split("\t")
    counts[ip] += int(count)

for ip, count in counts.items():
    print(f"{ip}\t{count}")

#Command 
cat logs.txt | python3 logs_mapper.py | sort | python3 logs_reducer.py

#Sortie
192.168.0.1    2
192.168.0.2    1


#Exercice 2 : Regroupement des ventes

#Mapper (sales_mapper.py) :
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    region, sales = line.strip().split(",")
    print(f"{region}\t{sales}")


#Reducer
#!/usr/bin/env python3
import sys
from collections import defaultdict

totals = defaultdict(int)

for line in sys.stdin:
    region, sales = line.strip().split("\t")
    totals[region] += int(sales)

for region, total in totals.items():
    print(f"{region}\t{total}")

#Command
cat sales.txt | python3 sales_mapper.py | sort | python3 sales_reducer.py

#Sortie
Region1    400
Region2    200
Region3    400



#Exercice 3 : Trouver le mot le plus fréquent

#Mapper (wordcount_mapper.py) :
#!/usr/bin/env python3
import sys
import re

for line in sys.stdin:
    words = re.findall(r"\w+", line.lower())
    for word in words:
        print(f"{word}\t1")

#Reducer (wordcount_reducer.py) :#!/usr/bin/env python3
import sys
from collections import defaultdict

counts = defaultdict(int)

for line in sys.stdin:
    word, count = line.strip().split("\t")
    counts[word] += int(count)

most_frequent = max(counts, key=counts.get)
print(f"{most_frequent}\t{counts[most_frequent]}")

#Command
cat text.txt | python3 wordcount_mapper.py | sort | python3 wordcount_reducer.py


#Exercice 4 : Calculer la moyenne des notes

#Mapper (grades_mapper.py) :
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    student, subject, grade = line.strip().split(",")
    print(f"{student}\t{grade}")

#Reducer (grades_reducer.py) :
#!/usr/bin/env python3
import sys
from collections import defaultdict

grades = defaultdict(list)

for line in sys.stdin:
    student, grade = line.strip().split("\t")
    grades[student].append(float(grade))

for student, grade_list in grades.items():
    avg = sum(grade_list) / len(grade_list)
    print(f"{student}\t{avg:.2f}")

#Command
cat grades.csv | python3 grades_mapper.py | sort | python3 grades_reducer.py



#Exercice 5 : Recherche d’un mot spécifique

#Mapper (search_mapper.py) :
#!/usr/bin/env python3
import sys
import re

target_word = "hadoop"

for line in sys.stdin:
    words = re.findall(r"\w+", line.lower())
    for word in words:
        if word == target_word:
            print(f"{word}\t1")


#Reducer (search_reducer.py) :
#!/usr/bin/env python3
import sys
from collections import defaultdict

counts = defaultdict(int)

for line in sys.stdin:
    word, count = line.strip().split("\t")
    counts[word] += int(count)

for word, count in counts.items():
    print(f"{word}\t{count}")

#Command
cat text.txt | python3 search_mapper.py | sort | python3 search_reducer.py



#Exercice 6 : Détection des doublons

#Mapper (duplicates_mapper.py) :
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    name = line.strip()
    print(f"{name}\t1")

#Reducer (duplicates_reducer.py) :
#!/usr/bin/env python3
import sys
from collections import defaultdict

counts = defaultdict(int)

for line in sys.stdin:
    name, count = line.strip().split("\t")
    counts[name] += int(count)

for name, count in counts.items():
    print(f"{name}\t{count}")

#Command
cat names.txt | python3 duplicates_mapper.py | sort | python3 duplicates_reducer.py










