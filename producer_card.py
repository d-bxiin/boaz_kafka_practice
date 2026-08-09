import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC = "card-events"
MERCHANTS = ["스타벅스", "쿠팡", "GS25", "배달의민족", "SK주유소"]

print(f"카드 결제 Producer 시작... {TOPIC} 토픽으로 이벤트 전송 (Ctrl+C로 종료)")

while True:
    event = {
        "card_id": f"card_{random.randint(1, 15)}",
        "merchant": random.choice(MERCHANTS),
        "amount": random.randint(3000, 300000),
        "timestamp": time.time()
    }

    # 일부 이벤트는 형식이 깨진 상태(JSON 파싱 불가)로 전송됨 (실습용 오류 상황)
    if random.random() < 0.15:
        payload = f'{{"card_id": "{event["card_id"]}", "merchant": "{event["merchant"]}"...'.encode('utf-8')
    else:
        payload = json.dumps(event, ensure_ascii=False).encode('utf-8')

    producer.send(TOPIC, value=payload)
    print(f"전송: {event}")

    time.sleep(1)
