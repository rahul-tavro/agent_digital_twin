import json
import threading
from flask import Flask, request, jsonify
from process import process_card

app = Flask(__name__)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        event = request.get_json(force=True)
        print(f"Received MinIO event: {json.dumps(event, indent=2)}")

        records = event.get("Records", [])
        if not records:
            return jsonify({"status": "no records"}), 200

        for record in records:
            event_name = record.get("eventName", "")

            # Only process PUT/object creation events
            if "ObjectCreated" not in event_name:
                print(f"Skipping event: {event_name}")
                continue

            bucket = record["s3"]["bucket"]["name"]
            key    = record["s3"]["object"]["key"]

            if not key.endswith(".json"):
                print(f"Skipping non-JSON file: {key}")
                continue

            print(f"Triggering pipeline for s3://{bucket}/{key}")

            # Run in background thread so webhook returns immediately
            thread = threading.Thread(
                target=process_card,
                args=(bucket, key),
                daemon=True
            )
            thread.start()

        return jsonify({"status": "accepted"}), 202

    except Exception as e:
        print(f"Webhook error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    print("Starting webhook listener on port 8000...")
    app.run(host="0.0.0.0", port=8000, debug=False)