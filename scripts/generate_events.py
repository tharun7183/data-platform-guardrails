import json
import os
import random
from datetime import datetime, timedelta
from uuid import uuid4

EVENT_TYPES = ["view", "click", "vote", "comment", "signup"]
SUBREDDITS = ["python", "dataengineering", "movies", "gaming", "news", "sports", "music"]

def main():
    out_dir = os.environ.get("RAW_OUT_DIR", "data/raw")
    os.makedirs(out_dir, exist_ok=True)

    run_id = os.environ.get("RUN_ID", datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"))
    n = int(os.environ.get("N_EVENTS", "20000"))
    dt = datetime.utcnow().date().isoformat()
    out_path = os.path.join(out_dir, f"events_{dt}_{run_id}.jsonl")

    start = datetime.utcnow() - timedelta(minutes=30)

    with open(out_path, "w") as f:
        for _ in range(n):
            ts = start + timedelta(seconds=random.randint(0, 1800))
            user_id = random.randint(1, 2000)
            event = {
                "event_id": str(uuid4()),
                "ts": ts.isoformat() + "Z",
                "dt": ts.date().isoformat(),
                "user_id": user_id,
                "session_id": f"{user_id}-{random.randint(1, 50)}",
                "event_type": random.choice(EVENT_TYPES),
                "subreddit": random.choice(SUBREDDITS),
                "device": random.choice(["ios", "android", "web"]),
                "country": random.choice(["US", "IN", "CA", "GB", "DE"]),
                "ingested_at": datetime.utcnow().isoformat() + "Z",
            }

            if random.random() < 0.003:
                event["user_id"] = None
            if random.random() < 0.002:
                event["event_type"] = "UNKNOWN"

            f.write(json.dumps(event) + "\n")

    print(f"Wrote {n} events to {out_path}")

if __name__ == "__main__":
    main()
