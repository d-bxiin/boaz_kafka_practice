import json
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    "account-events", "card-events",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='stream-processor-group'
)

producer = KafkaProducer(bootstrap_servers='localhost:9092')

print("스트림 프로세서 시작... (Ctrl+C로 종료)")

for message in consumer:
    topic = message.topic
    raw = message.value
    print(f"\n수신: topic={topic}, partition={message.partition}, offset={message.offset}")
    print(f"원본: {raw[:80]}...")

    try:
        data = json.loads(raw)

        if topic == "account-events":
            if "amount" not in data:
                raise ValueError("account-events에는 amount가 필수입니다")
            normalized = {
                "source": "account",
                "user_id": data["account_id"],
                "type": data["type"],
                "amount": data["amount"],
                "timestamp": data["timestamp"]
            }

        elif topic == "card-events":
            normalized = {
                "source": "card",
                "user_id": data["card_id"],
                "type": "payment",
                "amount": data["amount"],
                "timestamp": data["timestamp"]
            }

        producer.send(
            "risk-check-events",
            value=json.dumps(normalized, ensure_ascii=False).encode('utf-8')
        )
        print("처리 성공 → risk-check-events")
        print(f"변환된 데이터: {normalized}")

    except Exception as e:
        print(f"처리 실패: {e}")
        producer.send("transaction-dlq", value=raw)
