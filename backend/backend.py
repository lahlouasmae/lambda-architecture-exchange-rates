from flask import Flask, jsonify
from flask_cors import CORS
import os
import json

app = Flask(__name__)
CORS(app)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "spark-data")

def read_spark_json(folder):
    files = os.listdir(folder)
    json_file = [f for f in files if f.endswith(".json")][0]

    records = []
    with open(os.path.join(folder, json_file), "r") as f:
        for line in f:
            records.append(json.loads(line))
    return records

@app.route("/health")
def health():
    return jsonify({"status": "ok", "message": "API is running"})

@app.route("/api/currencies")
def currencies():
    data = []
    for d in os.listdir(DATA_DIR):
        if d.startswith("predictions_") and os.path.isdir(os.path.join(DATA_DIR, d)):
            currency = d.replace("predictions_", "")
            records = read_spark_json(os.path.join(DATA_DIR, d))
            data.append({"code": currency, "count": len(records)})
    return jsonify(data)

@app.route("/api/predictions/<currency>")
def predictions(currency):
    folder = os.path.join(DATA_DIR, f"predictions_{currency.upper()}")
    if not os.path.exists(folder):
        return jsonify({"error": "Currency not found"}), 404
    return jsonify(read_spark_json(folder))

@app.route("/api/latest/<currency>")
def latest(currency):
    folder = os.path.join(DATA_DIR, f"predictions_{currency.upper()}")
    if not os.path.exists(folder):
        return jsonify({"error": "Currency not found"}), 404

    data = read_spark_json(folder)
    latest = sorted(data, key=lambda x: x["rate_date"], reverse=True)[0]

    error = abs(latest["rate"] - latest["predicted_rate"])
    error_pct = (error / latest["rate"]) * 100 if latest["rate"] != 0 else 0

    return jsonify({
        "currency": currency.upper(),
        "date": latest["rate_date"],
        "actual_rate": latest["rate"],
        "predicted_rate": latest["predicted_rate"],
        "error": error,
        "error_pct": error_pct
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
