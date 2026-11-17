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
