import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC = "account-events"
TRANSACTION_TYPES = ["deposit", "withdraw", "transfer"]

print(f"계좌 거래 Producer 시작... {TOPIC} 토픽으로 이벤트 전송 (Ctrl+C로 종료)")

while True:
    event = {
        "account_id": f"acc_{random.randint(1, 20)}",
        "type": random.choice(TRANSACTION_TYPES),
        "amount": random.randint(10000, 5000000),
        "timestamp": time.time()
    }

    # 일부 이벤트는 amount 필드가 누락된 상태로 전송됨 (실습용 오류 상황)
    if random.random() < 0.15:
        del event["amount"]

    payload = json.dumps(event, ensure_ascii=False).encode('utf-8')
    producer.send(TOPIC, value=payload)
    print(f"전송: {event}")

    time.sleep(1)
