"""Nombre d'avis et note moyenne par affection, sur les avis de Drugs.com.

Usage : spark-submit notes_par_affection.py <fichier TSV des avis> <dossier de sortie>
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

entree, sortie = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.appName("notes-par-affection").getOrCreate()
avis = spark.read.csv(
    entree, sep="\t", header=True, inferSchema=True, multiLine=True, escape='"'
)
notes = (
    avis.groupBy("condition")
    .agg(F.count("*").alias("avis"), F.round(F.avg("rating"), 2).alias("note"))
    .where(F.col("avis") >= 1000)
    .orderBy(F.desc("avis"))
)
notes.show(10, truncate=False)
notes.write.mode("overwrite").parquet(sortie)
spark.stop()
