import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BROKER = "localhost:9092"
TOPIC = "exchange-rates"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

URL = "https://api.frankfurter.app/latest?from=EUR"

def fetch_exchange_rates():
    try:
        response = requests.get(URL, timeout=5)
        response.raise_for_status()
        data = response.json()

        # Validation du contenu
        if "rates" not in data:
            raise ValueError("Réponse API invalide (rates manquant)")

        return data

    except Exception as e:
        print("API error:", e)
        return None

def send_to_kafka(message):
    producer.send(TOPIC, message)
    producer.flush()

if __name__ == "__main__":
    print("Producer Exchange Rates démarré...")

    while True:
        data = fetch_exchange_rates()

        if data is not None:
            message = {
                "base": data.get("base", "EUR"),
                "date": data.get("date", datetime.utcnow().strftime("%Y-%m-%d")),
                "rates": data["rates"],
                "ingestion_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            }

            send_to_kafka(message)
            print("Message envoyé :", message["date"])

        else:
            print("Message ignoré (API invalide)")

        time.sleep(15)