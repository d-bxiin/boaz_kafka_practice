import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "risk-check-events",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='risk-consumer-group'
)

HIGH_AMOUNT_THRESHOLD = 1_000_000  # 100만원 이상 → 고액 거래로 표시

print("최종 Consumer 시작... (이상 거래 모니터링)")

for message in consumer:
    data = json.loads(message.value)
    flag = "⚠️ 고액 거래" if data["amount"] >= HIGH_AMOUNT_THRESHOLD else "정상"
    print(f"[{flag}] source={data['source']}, user_id={data['user_id']}, amount={data['amount']:,}원")
