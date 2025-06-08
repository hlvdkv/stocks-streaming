# Stocks Streaming z Apache Spark + Kafka + Delta Lake (na Dataproc)

---

## 1. Utwórz klaster Dataproc z Kafką

```bash
export CLUSTER_NAME=stocks-spark-cluster
export REGION=europe-west1
export PROJECT_ID=twoj-projekt-id

gcloud dataproc clusters create ${CLUSTER_NAME} \
  --enable-component-gateway --region ${REGION} --subnet default \
  --master-machine-type n1-standard-4 --master-boot-disk-size 50 \
  --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
  --image-version 2.1-debian11 --optional-components DOCKER,ZOOKEEPER \
  --project ${PROJECT_ID} --max-age=2h \
  --metadata "run-on-master=true" \
  --initialization-actions \
  gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh
```

---

## 2. Przygotuj dane 

```bash
git clone https://github.com/hlvdkv/stocks-streaming.git
cd stocks-streaming/

export BUCKET=twoj_bucket

# Pobierz dane testowe i metadane (najpierw wrzuć na swojego bucketa)
gsutil -m cp -r gs://$BUCKET/stocks_result ./
gsutil cp gs://$BUCKET/symbols_valid_meta.csv ./

# Przygotuj środowisko
chmod +x reset_env.sh run_spark.sh
./reset_env.sh
pip install kafka-python
```

---

## 3. Uruchom producenta danych (Kafka)

```bash
python producer.py --dir stocks_result
```

---

## 4. Uruchom streamowanie Spark

```bash
./run_spark.sh
```

---

## Sprawdź wyniki zapisane w Delta Lake

```bash
export OUTPUT_URI=gs://$BUCKET/stocks/output

spark-submit \
  --master yarn \
  --deploy-mode client \
  --packages io.delta:delta-core_2.12:1.2.1 \
  read_results.py
```

---

## Struktura danych w GCS

```
gs://$BUCKET/stocks/
├── output/
│   ├── realtime_bars/      ← główna tabela z agregacjami
│   └── anomalies/          ← wykryte anomalie 
├── checkpoints/
└── symbols_valid_meta.csv
```

---


Po uruchomieniu `read_results.py` powinieneś zobaczyć agregaty i anomalie na podstawie danych giełdowych przesyłanych przez Kafkę.
