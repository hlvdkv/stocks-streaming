import csv, os, sys, time, argparse
from pathlib import Path
from typing import Iterable
from kafka import KafkaProducer


def rows(path: Path) -> Iterable[str]:
    with path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)          # pomiń nagłówek
        for r in reader:
            if r:
                yield ",".join(r)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir",      default="data", help="Katalog z plikami *.csv")
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    ap.add_argument("--topic",    default=os.getenv("KAFKA_TOPIC", "stocks-topic"))
    ap.add_argument("--delay",    type=float, default=0.02, help="Opóźnienie (s) między rekordami")
    args = ap.parse_args()

    paths = sorted(Path(args.dir).glob("*.csv"))
    if not paths:
        sys.exit(f"[Producer] Brak plików CSV w {args.dir}")

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap],
        value_serializer=str.encode,
        linger_ms=5
    )


    try:
        for p in paths:
            print(f"[Producer] → {p.name}")
            for line in rows(p):
                producer.send(args.topic, value=line)
                time.sleep(args.delay)
            print(f"[Producer] ✔ {p.name}")
        producer.flush()
    finally:
        producer.close()
        print("[Producer] Zakończono wysyłanie wszystkich plików")


if __name__ == "__main__":
    main()
