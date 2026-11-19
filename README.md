# Spark dans Docker

Le projet des travaux pratiques *Spark dans Docker*. Leur énoncé est ci-dessous, pour les faire dans un Codespace GitHub ou sur un poste où Docker est installé. Colab ne lance pas de conteneurs&nbsp;: ces travaux pratiques n'ont pas de notebook.

Nous allons faire tourner Spark dans des conteneurs Docker&nbsp;: d'abord l'image officielle de Spark, seule, puis une image à nous, qui ajoute à Spark nos bibliothèques et notre code, et enfin, avec Docker Compose, un petit cluster Spark (un maître, des travailleurs, un serveur d'historique) et un JupyterLab qui s'y connecte. C'est l'environnement de travail&nbsp;: la même image partout, sur le poste, en intégration continue et sur le cluster.

## Mise en place de l'environnement de travail

Ouvrez le dépôt dans un Codespace&nbsp;: sur GitHub, **Code** > **Codespaces** > **Create codespace on main**, ou [ce lien](https://codespaces.new/shuuchuu/spark-docker). Docker et Docker Compose y sont installés. Sur votre poste, il vous faut [Docker Engine](https://docs.docker.com/engine/install/) et son greffon Compose, ou [Docker Desktop](https://docs.docker.com/desktop/), environ 4&nbsp;Go de mémoire libre et 3&nbsp;Go de disque, puis un clone du dépôt.

Les commandes ci-dessous se lancent dans un terminal à la racine du dépôt (**Terminal** > **Nouveau terminal** dans le Codespace). Les fichiers dont le code est donné sont à créer à l'endroit indiqué.

Les données sont les avis de patients sur des médicaments de [Drugs.com](https://archive.ics.uci.edu/dataset/462/drug+review+dataset+drugs+com) (Gräßer et al., UCI Machine Learning Repository, licence CC BY 4.0), ceux de l'ensemble d'entraînement&nbsp;: 161&nbsp;297 avis, un fichier TSV de 84&nbsp;Mo. Téléchargez-les dans `donnees/`, que git ignore&nbsp;:

```console
mkdir -p donnees
curl -fsSL -o donnees/drugsComTrain_raw.tsv https://github.com/shuuchuu/datasets/raw/refs/heads/main/drug-reviews/drugsComTrain_raw.tsv
```

Le job Spark que nous allons faire tourner est déjà écrit&nbsp;: `jobs/notes_par_affection.py` compte les avis et calcule la note moyenne par affection (`condition`), garde les affections d'au moins 1&nbsp;000 avis, en affiche les dix premières et les écrit en Parquet dans le dossier qu'on lui donne.

## Un premier conteneur Spark

Lancez le shell PySpark de l'image officielle de Spark, `spark:4.3.0-python3`, dans un conteneur interactif qui disparaît à sa sortie. Docker télécharge l'image au premier lancement (1,3&nbsp;Go). Dans le shell, affichez la version de Spark, le *master* de la session, son parallélisme par défaut et la version de Python, puis faites un calcul, la somme des entiers de 0 à 10<sup>8</sup>. Pendant que le shell tourne, listez les conteneurs actifs dans un second terminal, puis quittez le shell (`exit()`) et listez-les tous, arrêtés compris.

Sous quel utilisateur tourne le conteneur&nbsp;? Que vaut le parallélisme par défaut, et d'où vient-il&nbsp;?

**Documentation utile**

- [`docker run`](https://docs.docker.com/reference/cli/docker/container/run/)
- [`docker ps`](https://docs.docker.com/reference/cli/docker/container/ls/)
- [L'image officielle de Spark](https://hub.docker.com/_/spark)

<details>
<summary>Conseils supplémentaires</summary>

`-it` relie le terminal au conteneur, `--rm` le supprime à sa sortie. Spark est installé dans `/opt/spark`, mais son dossier `bin/` n'est pas dans le `PATH` de l'image&nbsp;: donnez le chemin complet de `pyspark`. Le shell crée lui-même `spark` (la session) et `sc` (son `SparkContext`). La commande `id`, lancée dans un conteneur de l'image, donne son utilisateur.

```console
docker run -it --rm spark:4.3.0-python3 /opt/spark/bin/...
```

```pycon
>>> spark.version
>>> sc.master
>>> sc.defaultParallelism
>>> spark.range(...).selectExpr("sum(id)").show()
>>> import sys; sys.version
```

```console
docker ps
docker ps -a
docker run --rm spark:4.3.0-python3 id
```

</details>

<details>
<summary>Solution</summary>

```console
docker run -it --rm spark:4.3.0-python3 /opt/spark/bin/pyspark
```

```pycon
>>> spark.version
'4.3.0'
>>> sc.master
'local[*]'
>>> sc.defaultParallelism
8
>>> spark.range(10**8).selectExpr("sum(id)").show()
+----------------+
|         sum(id)|
+----------------+
|4999999950000000|
+----------------+

>>> import sys; sys.version
'3.10.12 (main, Aug 31 2026, 10:18:17) [GCC 11.4.0]'
>>> exit()
```

```console
$ docker ps
$ docker ps -a
$ docker run --rm spark:4.3.0-python3 id
uid=185(spark) gid=185(spark) groups=185(spark)
```

`docker ps` montre le conteneur, au nom tiré au hasard, tant que le shell tourne&nbsp;; `docker ps -a` ne le montre plus après `exit()`&nbsp;: `--rm` l'a supprimé, avec tout ce qu'on y aurait écrit. L'image, elle, reste (`docker images`).

Le conteneur tourne sous l'utilisateur `spark`, d'uid 185, pas sous `root`. La session tourne en mode local (`local[*]`)&nbsp;: un seul processus, un fil d'exécution par cœur. Le parallélisme par défaut est le nombre de cœurs de la machine hôte (8 sur la nôtre, 2 dans un Codespace de base)&nbsp;: un conteneur n'est pas une machine virtuelle, il voit les processeurs de l'hôte et partage son noyau.

</details>

## Un job sur des données montées

Lancez le job `jobs/notes_par_affection.py` avec `spark-submit`, dans un conteneur de la même image, sur le fichier des avis&nbsp;: le conteneur ne voit pas les fichiers de l'hôte, montez-y les dossiers `donnees/` et `jobs/` (ce dernier en lecture seule). Le job écrit son résultat dans `donnees/notes`.

Le tableau s'affiche, puis le job échoue&nbsp;: pourquoi&nbsp;? Comparez l'uid du conteneur et le propriétaire de `donnees/` (`ls -lnd`).

**Documentation utile**

- [Les montages de dossiers (*bind mounts*)](https://docs.docker.com/engine/storage/bind-mounts/)
- [`spark-submit`](https://spark.apache.org/docs/latest/submitting-applications.html)

<details>
<summary>Conseils supplémentaires</summary>

`-v <dossier de l'hôte>:<chemin dans le conteneur>` monte un dossier, `:ro` en lecture seule&nbsp;; le chemin de l'hôte est absolu (`"$PWD/donnees"`). Le job prend deux arguments&nbsp;: le fichier d'entrée et le dossier de sortie, vus du conteneur.

```console
docker run --rm -v "$PWD/donnees:/donnees" -v "$PWD/jobs:/jobs:ro" spark:4.3.0-python3 \
  /opt/spark/bin/spark-submit /jobs/notes_par_affection.py ... ...
ls -lnd donnees
```

</details>

<details>
<summary>Solution</summary>

```console
$ docker run --rm -v "$PWD/donnees:/donnees" -v "$PWD/jobs:/jobs:ro" spark:4.3.0-python3 \
    /opt/spark/bin/spark-submit /jobs/notes_par_affection.py \
    /donnees/drugsComTrain_raw.tsv /donnees/notes
+---------------+-----+----+
|condition      |avis |note|
+---------------+-----+----+
|Birth Control  |28788|6.09|
|Depression     |9069 |7.1 |
|Pain           |6145 |7.63|
|Anxiety        |5904 |7.69|
|Acne           |5588 |7.37|
|Bipolar Disorde|4224 |7.15|
|Insomnia       |3673 |6.72|
|Weight Loss    |3609 |8.05|
|Obesity        |3568 |7.74|
|ADHD           |3383 |7.35|
+---------------+-----+----+
only showing top 10 rows
...
java.io.IOException: Mkdirs failed to create file:/donnees/notes/_temporary/0/...
$ ls -lnd donnees
drwxr-xr-x 2 1000 1000 4096 ... donnees
```

(`Bipolar Disorde` est tronqué dans les données elles-mêmes.) La lecture marche, l'écriture échoue&nbsp;: un dossier monté garde ses propriétaires et ses droits, exprimés en uid. `donnees/` appartient à l'uid 1000, le vôtre (celui de `vscode` dans un Codespace), et le conteneur tourne sous l'uid 185, qui n'a pas le droit d'y écrire. Docker Desktop, sur macOS et Windows, partage les dossiers autrement et peut ne pas montrer l'erreur.

Lancer le conteneur sous votre uid (`--user "$(id -u):$(id -g)"`) ne suffit pas&nbsp;: Spark (Hadoop, en fait) cherche le nom de l'utilisateur et échoue sur un uid que le fichier `/etc/passwd` de l'image ne connaît pas (`Invalid UID, could not determine effective user`). La partie suivante donne à l'utilisateur `spark` de l'image votre uid.

</details>

## Une image à nous

Créez ce `Dockerfile`, celui des diapositives, pour une image `spark-avis`&nbsp;: il installe pandas et PyArrow à des versions fixées, donne à l'utilisateur `spark` l'uid de l'hôte, passé à la construction par l'argument `UID_HOTE` (1000 par défaut), crée `/opt/spark/work`, où un travailleur Spark écrit ses fichiers, et le lui donne, comme `/opt/spark/work-dir`, le dossier de travail de l'image, met `/opt/spark/bin` dans le `PATH`, puis revient à l'utilisateur `spark` et copie le dossier `jobs/` dans `/jobs`.

```dockerfile
FROM spark:4.3.0-python3
USER root
RUN pip install --no-cache-dir pandas==2.3.3 pyarrow==25.0.1
ARG UID_HOTE=1000
RUN usermod --uid "$UID_HOTE" spark \
  && mkdir /opt/spark/work \
  && chown spark /opt/spark/work /opt/spark/work-dir
ENV PATH="/opt/spark/bin:$PATH"
USER spark
COPY jobs /jobs
```

Écrivez aussi un `.dockerignore` qui garde `donnees/` et `carnets/` hors du contexte de construction. Construisez l'image avec votre uid, relancez le job avec elle (seul `donnees/` est à monter), puis modifiez le job (le seuil de 1&nbsp;000 avis, par exemple) et reconstruisez&nbsp;: quelles étapes Docker refait-il&nbsp;?

L'image doit aussi faire tourner JupyterLab 4.6.6, à la dernière partie. Lancez `python3 -c "import pyspark"` dans un conteneur de l'image&nbsp;: pourquoi échoue-t-il, alors que `spark-submit` lance le job&nbsp;? Complétez le `Dockerfile` jusqu'à ce que cet import passe et que `jupyter lab --ip 0.0.0.0`, lancé dans un conteneur interactif de l'image, démarre JupyterLab&nbsp;: que faut-il ajouter, et pourquoi&nbsp;?

**Documentation utile**

- [Le `Dockerfile`](https://docs.docker.com/reference/dockerfile/)
- [`docker build`](https://docs.docker.com/reference/cli/docker/buildx/build/)
- [Le contexte de construction et `.dockerignore`](https://docs.docker.com/build/concepts/context/)
- [Le cache de construction](https://docs.docker.com/build/cache/)
- [PySpark sans `pip`&nbsp;: le `PYTHONPATH`](https://spark.apache.org/docs/latest/api/python/getting_started/install.html#manually-downloading)
- [Les dossiers de Jupyter](https://docs.jupyter.org/en/latest/use/jupyter-directories.html)

<details>
<summary>Conseils supplémentaires</summary>

`docker build` construit l'image, `--build-arg` remplit l'argument `UID_HOTE`. Le contexte de construction est tout le dossier passé à `docker build` (`.`), envoyé au moteur de Docker avant la première étape&nbsp;: sans `.dockerignore`, les 84&nbsp;Mo de données en feraient partie.

PySpark est livré avec Spark, dans `/opt/spark/python`, et Py4J, qui relie Python à la JVM de Spark, dans `/opt/spark/python/lib`&nbsp;: `spark-submit` les donne lui-même au Python qu'il lance. Un Python lancé seul, comme le noyau d'un notebook, ne cherche ses modules que dans ses dossiers d'installation et dans son `PYTHONPATH`. Une fois JupyterLab installé, la dernière ligne de l'erreur de `jupyter lab` dit ce qui lui manque encore&nbsp;; `getent passwd spark` donne le dossier personnel de l'utilisateur. Une seule instruction `ENV` peut définir plusieurs variables.

```console
docker build --build-arg UID_HOTE="$(id -u)" -t spark-avis .
docker run --rm -v "$PWD/donnees:/donnees" spark-avis spark-submit /jobs/notes_par_affection.py ... ...
docker run --rm spark-avis python3 -c "import pyspark"
docker run --rm spark-avis ls /opt/spark/python/lib
docker run --rm spark-avis getent passwd spark
docker run -it --rm spark-avis jupyter lab --ip 0.0.0.0
```

```dockerfile
RUN pip install --no-cache-dir \
  ... pandas==2.3.3 pyarrow==25.0.1
ARG UID_HOTE=1000
RUN usermod --uid "$UID_HOTE" --home ... spark \
  && mkdir ... /opt/spark/work \
  && chown spark ... /opt/spark/work /opt/spark/work-dir
ENV PATH="/opt/spark/bin:$PATH" \
  PYTHONPATH="..."
```

</details>

<details>
<summary>Solution</summary>

`.dockerignore`&nbsp;:

```text
donnees/
carnets/
```

```console
$ docker build --build-arg UID_HOTE="$(id -u)" -t spark-avis .
$ docker run --rm -v "$PWD/donnees:/donnees" spark-avis \
    spark-submit /jobs/notes_par_affection.py /donnees/drugsComTrain_raw.tsv /donnees/notes
$ ls -ln donnees/notes
-rw-r--r-- 1 1000 185 1849 ... part-00000-....snappy.parquet
-rw-r--r-- 1 1000 185    0 ... _SUCCESS
```

Le job écrit cette fois son résultat, sous votre uid&nbsp;: les fichiers vous appartiennent sur l'hôte. Le nom de l'argument n'est pas `UID`&nbsp;: Bash réserve cette variable, en lecture seule.

Après une modification du job, Docker ne refait que la dernière étape, `COPY jobs /jobs`&nbsp;: les précédentes n'ont pas changé, il reprend leurs couches de son cache. C'est pourquoi le code se copie en dernier, après l'installation des bibliothèques, qui change rarement. L'argument `UID_HOTE` se déclare lui aussi après cette installation&nbsp;: chaque `RUN` qui suit un `ARG` dépend de sa valeur, et une construction avec un autre uid réinstallerait sinon les bibliothèques. Les versions écrites dans le `Dockerfile`, celle de l'image de Spark comme celles des bibliothèques, font qu'une reconstruction, un mois plus tard ou sur une autre machine, donne la même image.

```console
$ docker run --rm spark-avis python3 -c "import pyspark"
...
ModuleNotFoundError: No module named 'pyspark'
$ docker run --rm spark-avis ls /opt/spark/python/lib
py4j-0.10.9.9-src.zip
PY4J_LICENSE.txt
pyspark.zip
$ docker run --rm spark-avis getent passwd spark
spark:x:1000:185::/nonexistent:/bin/sh
```

Le Python de l'image n'a pas PySpark&nbsp;: Spark le livre à part, dans `/opt/spark/python`, avec Py4J dans `/opt/spark/python/lib`, et `spark-submit` met les deux dans le `PYTHONPATH` du Python qu'il lance. Le noyau d'un notebook, lancé par JupyterLab, n'y passe pas&nbsp;: c'est à l'image de les mettre dans son `PYTHONPATH`. Installer PySpark par `pip` marcherait aussi, mais ajouterait à l'image une seconde copie de Spark, à garder à la même version que la première.

JupyterLab installé, `jupyter lab` échoue encore&nbsp;: `PermissionError: [Errno 13] Permission denied: '/nonexistent'`. Il écrit ses fichiers dans le dossier personnel de l'utilisateur (`~/.local/share/jupyter`), et celui de `spark`, dans l'image officielle, est `/nonexistent`, un dossier qui n'existe pas. Un job lancé par `spark-submit` n'en a pas besoin&nbsp;; JupyterLab, si&nbsp;: `usermod --home` lui en donne un, que le `Dockerfile` crée et donne à `spark`.

`Dockerfile`&nbsp;:

```dockerfile
FROM spark:4.3.0-python3
USER root
RUN pip install --no-cache-dir \
  jupyterlab==4.6.6 pandas==2.3.3 pyarrow==25.0.1
ARG UID_HOTE=1000
RUN usermod --uid "$UID_HOTE" --home /home/spark spark \
  && mkdir /home/spark /opt/spark/work \
  && chown spark /home/spark /opt/spark/work /opt/spark/work-dir
ENV PATH="/opt/spark/bin:$PATH" \
  PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip"
USER spark
COPY jobs /jobs
```

```console
$ docker build --build-arg UID_HOTE="$(id -u)" -t spark-avis .
$ docker run --rm spark-avis python3 -c "import pyspark; print(pyspark.__version__)"
4.3.0
$ docker run -it --rm spark-avis jupyter lab --ip 0.0.0.0
...
    http://127.0.0.1:8888/lab?token=...
```

JupyterLab démarre, dans un conteneur qui ne publie pas son port&nbsp;: la dernière partie le lancera dans le cluster. `Ctrl-C` deux fois l'arrête.

</details>

## Un cluster Spark avec Docker Compose

Créez ce `compose.yaml`, celui des diapositives&nbsp;: un cluster Spark *standalone* en deux services de l'image `spark-avis`, qu'il construit, le maître, `maitre`, qui publie son interface web sur le port 8080, et deux travailleurs, `travailleur`, d'un cœur et de 1&nbsp;Go de mémoire chacun, qui s'inscrivent auprès du maître à l'adresse `spark://maitre:7077`. Ce que les services partagent (la construction, l'image, le dossier `donnees/` monté) s'écrit une fois, dans l'extension `x-spark`, qu'une ancre YAML (`&spark`) fusionne dans chaque service (`<<: *spark`). L'argument `UID_HOTE` se lit dans l'environnement&nbsp;: si votre uid n'est pas 1000, faites `export UID_HOTE=$(id -u)` avant de lancer Compose.

```yaml
x-spark: &spark
  build:
    context: .
    args: { UID_HOTE: "${UID_HOTE:-1000}" }
  image: spark-avis
  volumes: ["./donnees:/donnees"]

services:
  maitre:
    <<: *spark
    command: spark-class org.apache.spark.deploy.master.Master
    ports: ["8080:8080"]
  travailleur:
    <<: *spark
    command: spark-class org.apache.spark.deploy.worker.Worker spark://maitre:7077
    environment: [SPARK_WORKER_CORES=1, SPARK_WORKER_MEMORY=1g]
    deploy: { replicas: 2 }
    depends_on: [maitre]
```

Démarrez le cluster en arrière-plan, listez ses conteneurs, puis ouvrez l'interface du maître (port 8080, onglet **Ports** dans un Codespace)&nbsp;: combien de travailleurs, de cœurs et de mémoire&nbsp;? Lisez enfin le journal des travailleurs.

L'interface du maître ne garde les applications terminées que tant qu'il tourne, et celle d'une application (port 4040) s'arrête avec elle. Ajoutez au cluster un serveur d'historique&nbsp;: un service `historique`, de la même image, qui publie sur le port 18080 l'interface des applications terminées, reconstruite d'après le journal d'événements que chacune écrit. Ces journaux vont dans un volume nommé, `evenements`, monté en `/evenements`. Relancez le cluster et ouvrez l'interface du serveur d'historique&nbsp;: elle reste vide jusqu'à la première application, à la partie suivante.

**Documentation utile**

- [Spark en mode *standalone*](https://spark.apache.org/docs/latest/spark-standalone.html)
- [Le serveur d'historique de Spark](https://spark.apache.org/docs/latest/monitoring.html#viewing-after-the-fact)
- [Le fichier `spark-defaults.conf`](https://spark.apache.org/docs/latest/configuration.html#dynamically-loading-spark-properties)
- [Les services d'un fichier Compose](https://docs.docker.com/reference/compose-file/services/)
- [Les volumes d'un fichier Compose](https://docs.docker.com/reference/compose-file/volumes/)
- [Les volumes de Docker](https://docs.docker.com/engine/storage/volumes/)
- [`docker compose up`](https://docs.docker.com/reference/cli/docker/compose/up/)
- [`docker compose logs`](https://docs.docker.com/reference/cli/docker/compose/logs/)

<details>
<summary>Conseils supplémentaires</summary>

`spark-class org.apache.spark.deploy.history.HistoryServer` lance le serveur d'historique. Une application écrit son journal d'événements si `spark.eventLog.enabled` vaut `true`, dans le dossier `spark.eventLog.dir`&nbsp;; le serveur lit celui de `spark.history.fs.logDirectory`. Ces trois propriétés s'écrivent dans `/opt/spark/conf/spark-defaults.conf`, une par ligne, suivie de sa valeur&nbsp;: `spark-submit` et le serveur le lisent à leur démarrage. Copiez-le dans l'image.

Un volume nommé se déclare sous la clé `volumes` du premier niveau du `compose.yaml`, et se monte comme un dossier, son nom à la place du chemin de l'hôte. Docker remplit un volume neuf avec le contenu du dossier de l'image où il est monté, propriétaire compris&nbsp;; si ce dossier n'existe pas dans l'image, le volume appartient à `root`, et une application qui y écrit échoue à son démarrage (`chmod: cannot access '/evenements/eventlog_v2_...'`).

`spark-defaults.conf`&nbsp;:

```text
spark.eventLog.enabled         ...
spark.eventLog.dir             file:...
spark.history.fs.logDirectory  file:...
```

```dockerfile
  && mkdir /home/spark /opt/spark/work ... \
  && chown spark /home/spark /opt/spark/work /opt/spark/work-dir ...
...
COPY spark-defaults.conf ...
```

```yaml
x-spark: &spark
  ...
  volumes: ["./donnees:/donnees", ...]

services:
  ...
  historique:
    <<: *spark
    command: ...
    ports: [...]

volumes:
  evenements:
```

```console
docker compose up -d --build
docker compose ps
docker compose logs travailleur
docker compose logs historique
```

</details>

<details>
<summary>Solution</summary>

```console
$ docker compose up -d --build
$ docker compose ps
NAME                         IMAGE        ...   SERVICE       ...   PORTS
spark-docker-maitre-1        spark-avis   ...   maitre        ...   0.0.0.0:8080->8080/tcp
spark-docker-travailleur-1   spark-avis   ...   travailleur   ...
spark-docker-travailleur-2   spark-avis   ...   travailleur   ...
$ docker compose logs travailleur
```

L'interface du maître montre deux travailleurs vivants (*Workers: 2 Alive*), deux cœurs et 2&nbsp;Go de mémoire en tout, et aucune application. Chaque travailleur écrit dans son journal qu'il s'est inscrit auprès du maître (`Successfully registered with master spark://...:7077`). Les liens de l'interface vers les travailleurs pointent vers leurs adresses dans le réseau de Compose&nbsp;: dans un Codespace, votre navigateur ne les joint pas.

Sans `/opt/spark/work` à lui, un travailleur s'arrête au démarrage (`Failed to create directory /opt/spark/work`)&nbsp;: après `usermod --uid`, `spark` n'est plus le propriétaire de `/opt/spark` et ne peut plus y créer `work`, ce que le travailleur fait lui-même sous l'uid 185&nbsp;; le `Dockerfile` le crée et le lui donne.

`spark-defaults.conf`&nbsp;:

```text
spark.eventLog.enabled         true
spark.eventLog.dir             file:/evenements
spark.history.fs.logDirectory  file:/evenements
```

Le `Dockerfile` crée `/evenements` et le donne à `spark`, puis copie la configuration dans l'image, avant le code, qui change plus souvent&nbsp;:

```dockerfile
FROM spark:4.3.0-python3
USER root
RUN pip install --no-cache-dir \
  jupyterlab==4.6.6 pandas==2.3.3 pyarrow==25.0.1
ARG UID_HOTE=1000
RUN usermod --uid "$UID_HOTE" --home /home/spark spark \
  && mkdir /home/spark /opt/spark/work /evenements \
  && chown spark /home/spark /opt/spark/work /opt/spark/work-dir /evenements
ENV PATH="/opt/spark/bin:$PATH" \
  PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip"
COPY spark-defaults.conf /opt/spark/conf/
USER spark
COPY jobs /jobs
```

`compose.yaml`&nbsp;:

```yaml
x-spark: &spark
  build:
    context: .
    args: { UID_HOTE: "${UID_HOTE:-1000}" }
  image: spark-avis
  volumes: ["./donnees:/donnees", "evenements:/evenements"]

services:
  maitre:
    <<: *spark
    command: spark-class org.apache.spark.deploy.master.Master
    ports: ["8080:8080"]
  travailleur:
    <<: *spark
    command: spark-class org.apache.spark.deploy.worker.Worker spark://maitre:7077
    environment: [SPARK_WORKER_CORES=1, SPARK_WORKER_MEMORY=1g]
    deploy: { replicas: 2 }
    depends_on: [maitre]
  historique:
    <<: *spark
    command: spark-class org.apache.spark.deploy.history.HistoryServer
    ports: ["18080:18080"]

volumes:
  evenements:
```

```console
$ docker compose up -d --build
$ docker compose logs historique
...
historique-1  | ... INFO HistoryServer: Bound HistoryServer to 0.0.0.0, and started at http://...:18080
```

Le journal d'événements d'une application est écrit par son pilote, qui tourne dans le conteneur d'où on la lance (ici, celui du maître, puis celui de JupyterLab)&nbsp;: le volume, dans `x-spark`, est monté dans tous les services, travailleurs compris, pour faire simple. Le serveur d'historique ne fait que lire ces journaux&nbsp;: il ne dépend ni du maître ni des applications, et peut tourner sans eux.

</details>

## Soumettre le job au cluster

Soumettez le job au cluster depuis le conteneur du maître, avec `spark-submit` et l'adresse du maître. Dans l'interface du maître, retrouvez l'application terminée&nbsp;: combien de cœurs a-t-elle eus, et combien de temps a-t-elle duré&nbsp;? Retrouvez-la dans l'interface du serveur d'historique, et ouvrez-la&nbsp;: ses jobs, ses étapes, ses tâches. Passez ensuite à un, puis à trois travailleurs, et relancez le job à chaque fois&nbsp;: va-t-il plus vite&nbsp;?

Que se passerait-il si les travailleurs ne montaient pas `donnees/`&nbsp;?

**Documentation utile**

- [`docker compose exec`](https://docs.docker.com/reference/cli/docker/compose/exec/)
- [Les adresses de *master*](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls)
- [L'interface web de Spark](https://spark.apache.org/docs/latest/web-ui.html)

<details>
<summary>Conseils supplémentaires</summary>

`docker compose exec <service> <commande>` lance une commande dans le conteneur d'un service qui tourne. `--master` donne le *master*&nbsp;; sans lui, le job tournerait en local, dans le conteneur. `--scale travailleur=<n>` change le nombre de travailleurs, le temps d'un `up`. Les applications terminées sont, dans l'interface du maître, sous *Completed Applications*&nbsp;; le serveur d'historique relit les journaux toutes les dix secondes.

```console
docker compose exec maitre spark-submit --master ... /jobs/notes_par_affection.py \
  /donnees/drugsComTrain_raw.tsv /donnees/notes
docker compose up -d --scale travailleur=...
```

</details>

<details>
<summary>Solution</summary>

```console
$ docker compose exec maitre spark-submit --master spark://maitre:7077 \
    /jobs/notes_par_affection.py /donnees/drugsComTrain_raw.tsv /donnees/notes
$ docker compose up -d --scale travailleur=1
$ docker compose exec maitre spark-submit --master spark://maitre:7077 ...
$ docker compose up -d --scale travailleur=3
$ docker compose exec maitre spark-submit --master spark://maitre:7077 ...
```

L'application `notes-par-affection` apparaît sous *Completed Applications*, avec deux cœurs, un par travailleur. Sur notre machine, occupée par d'autres calculs pendant la mesure, elle a duré 39&nbsp;s avec deux travailleurs, 28&nbsp;s avec un et 51&nbsp;s avec trois&nbsp;: plus de travailleurs ne va pas plus vite. Le fichier, lu avec `multiLine=True`, ne se découpe pas&nbsp;: sa lecture est une seule tâche, sur un seul cœur, et le reste est trop petit pour que le travail réparti compense le lancement des exécuteurs et les échanges entre eux. Surtout, les conteneurs se partagent les processeurs de la même machine&nbsp;: un cluster dans Docker Compose reproduit l'architecture d'un vrai cluster, pour développer et tester, pas sa puissance.

Le serveur d'historique liste les trois exécutions, chacune avec l'interface qu'elle avait sur le port 4040 pendant qu'elle tournait&nbsp;: ses jobs, ses étapes, dont la lecture du fichier en une seule tâche, et ses exécuteurs.

Le pilote (*driver*) tourne ici dans le conteneur du maître (mode `client`, celui par défaut), les exécuteurs dans ceux des travailleurs. Chaque exécuteur lit sa part des données au chemin que le job donne&nbsp;: sans `donnees/` monté sur les travailleurs, leurs tâches échouent (`java.io.FileNotFoundException: File file:/donnees/drugsComTrain_raw.tsv does not exist`), et le job avec elles, après quatre essais par tâche. Sur un vrai cluster, les données sont sur un stockage que tous les nœuds lisent&nbsp;: HDFS, S3 ou un autre stockage objet.

</details>

## JupyterLab, l'environnement de travail

Ajoutez au `compose.yaml` un service `jupyter`, de la même image, qui lance JupyterLab sur le port 8888, avec pour dossier de notebooks `/carnets`, monté depuis `carnets/`&nbsp;; il publie aussi le port 4040, celui de l'interface de l'application Spark que le notebook lancera&nbsp;: ses notebooks seront le pilote. Démarrez-le, ouvrez JupyterLab (l'adresse et son jeton sont dans le journal du service), puis, dans un notebook&nbsp;:

1. créez une session sur le cluster&nbsp;;
2. lisez le résultat du job, `/donnees/notes`, et comptez ses lignes&nbsp;;
3. vérifiez que le pilote et les exécuteurs ont la même version de Python&nbsp;;
4. ouvrez l'interface de l'application (port 4040).

Pendant que la session est ouverte, soumettez à nouveau le job depuis un terminal&nbsp;: que se passe-t-il&nbsp;? Arrêtez enfin tout avec `docker compose down`&nbsp;: que reste-t-il&nbsp;? Relancez le cluster&nbsp;: le serveur d'historique montre-t-il encore les applications&nbsp;? Et après `docker compose down -v`&nbsp;?

**Documentation utile**

- [Lancer JupyterLab](https://jupyterlab.readthedocs.io/en/stable/getting_started/starting.html)
- [`SparkSession.builder.master`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.builder.master.html)
- [`docker compose down`](https://docs.docker.com/reference/cli/docker/compose/down/)

<details>
<summary>Conseils supplémentaires</summary>

`jupyter lab --ip 0.0.0.0` écoute sur toutes les interfaces du conteneur, sans quoi les ports publiés n'y mènent pas. La clé `volumes` d'un service remplace celle de `x-spark`, sans s'y ajouter&nbsp;: le service `jupyter` liste tous ses montages, le volume `evenements` compris, pour que le serveur d'historique voie ses sessions. Un `parallelize` sur quatre partitions, dont chaque tâche renvoie `sys.version`, montre la version de Python des exécuteurs.

```yaml
  jupyter:
    <<: *spark
    command: jupyter lab --ip 0.0.0.0 --notebook-dir ...
    ports: [...]
    volumes: [...]
    depends_on: [maitre]
```

```console
mkdir -p carnets
docker compose up -d
docker compose logs jupyter
```

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("exploration").master(...).getOrCreate()
```

```console
docker compose down
docker volume ls
docker compose up -d
docker compose down -v
```

</details>

<details>
<summary>Solution</summary>

Dans `compose.yaml`, après `historique`&nbsp;:

```yaml
  jupyter:
    <<: *spark
    command: jupyter lab --ip 0.0.0.0 --notebook-dir /carnets
    ports: ["8888:8888", "4040:4040"]
    volumes: ["./donnees:/donnees", "evenements:/evenements", "./carnets:/carnets"]
    depends_on: [maitre]
```

```console
$ mkdir -p carnets
$ docker compose up -d
$ docker compose logs jupyter
...
jupyter-1  |     http://127.0.0.1:8888/lab?token=...
```

Dans un notebook&nbsp;:

```python
import sys

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("exploration")
    .master("spark://maitre:7077")
    .getOrCreate()
)
print(spark.read.parquet("/donnees/notes").count())
print(sys.version)
print(spark.sparkContext.parallelize(range(4), 4).map(lambda _: sys.version).collect())
```

Le résultat du job a 32 lignes, une par affection d'au moins 1&nbsp;000 avis. Le pilote et les exécuteurs ont le même Python, 3.10.12&nbsp;: ils tournent dans la même image. La fonction que reçoivent les exécuteurs renvoie leur `sys.version`, pas celle du pilote&nbsp;: cloudpickle, qui l'envoie, transmet le module `sys` par son nom, et chaque exécuteur importe le sien. Avec des Python différents (le pilote sur le poste, les exécuteurs dans un conteneur, par exemple), Spark refuse d'exécuter une fonction Python (`PYTHON_VERSION_MISMATCH`). Les exécuteurs joignent le pilote à l'adresse qu'il leur annonce, le nom d'hôte de son conteneur&nbsp;: son identifiant court, que le réseau de Compose résout.

Le job soumis pendant que la session est ouverte attend (`Initial job has not accepted any resources`)&nbsp;: la session a pris tous les cœurs du cluster, et les garde tant qu'elle vit. Il démarre dès que le notebook appelle `spark.stop()`. Pour partager un cluster, une application limite ses cœurs (`spark.cores.max`).

```console
$ docker compose down
$ docker volume ls
DRIVER    VOLUME NAME
local     spark-docker_evenements
$ docker compose up -d
$ docker compose down -v
$ docker compose up -d
```

`docker compose down` arrête et supprime les conteneurs et le réseau&nbsp;: ce qu'on n'a écrit que dans un conteneur est perdu. Restent l'image `spark-avis`, que le prochain `up` réutilise, le volume nommé `evenements`, et, sur l'hôte, ce que les dossiers montés ont reçu&nbsp;: `donnees/notes` et les notebooks de `carnets/`. Après un nouveau `up`, l'interface du maître n'a plus aucune application, mais le serveur d'historique les montre toutes, d'après les journaux que le volume a gardés. `docker compose down -v` supprime aussi les volumes nommés du projet&nbsp;: au `up` suivant, l'historique est vide. Les dossiers montés de l'hôte, eux, restent.

</details>

## Solution

Le projet terminé est sur la [branche `solution`](https://github.com/shuuchuu/spark-docker/tree/solution) du dépôt.
