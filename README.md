# Kafka 실습: 여러 금융 이벤트 소스를 하나로 처리해보기

## 실습 목표

Kafka를 이용해 **여러 Producer → 스트림 처리 → 하나의 Consumer** 로 이어지는

기본적인 스트리밍 파이프라인 구조를 직접 실행해보는 것이 목표! 

<실습을 통해 경험할 수 있는 것>

1. 여러 Producer가 동시에 Kafka로 데이터를 보낼 수 있다
2. Kafka는 중간에서 데이터를 가공하는 역할도 할 수 있다
3. 처리 중 오류가 발생해도 시스템이 멈추지 않게 만들 수 있다

## 실습 시나리오

- 은행 시스템에서 계좌 거래 이벤트가 발생한다. (`producer_account.py`)
- 카드사에서 카드 결제 이벤트가 발생한다. (`producer_card.py`)
- 두 이벤트는 서로 다른 Topic으로 Kafka에 저장된다.
- 중간 스트림 프로세서가 이벤트를 읽어 하나의 형태로 정리한다. (`stream_processor.py`)
- 최종 Consumer는 정리된 거래만 받아서, 고액 거래인지 확인한다. (`consumer_risk.py`)

---

## 1단계: 환경 구성

### 1. 실습 코드 다운로드

```bash
git clone https://github.com/d-bxiin/boaz_kafka_practice.git
cd boaz_kafka_practice/
```

### 2. Kafka 서버 실행

```bash
docker-compose up -d
docker ps
```

`kafka`, `zookeeper` 컨테이너가 `Up` 상태인지 확인하기!

### 3. 파이썬 실행 환경 준비

```bash
python3 -m venv kafka-env
source kafka-env/bin/activate
pip install -r requirements.txt
```

> 💡 **참고**: `requirements.txt`는 `kafka-python`이 아니라 `kafka-python-ng`를 사용합니다. 원조 `kafka-python`은 관리가 멈춰서 Python 3.12 이상에서 `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'` 에러가 나기 때문에, 관리가 계속되고 있는 후속 프로젝트인 `kafka-python-ng`로 대체했습니다. 코드에서 `import` 하는 방식은 완전히 동일합니다 (`from kafka import KafkaProducer` 그대로).

---

## 2단계: 실습 진행

총 **4개의 터미널**을 사용하고, 모든 터미널에서 아래 명령을 먼저 실행한다.

```bash
cd boaz_kafka_practice/
source kafka-env/bin/activate
```

### 1️⃣ 계좌 거래 Producer 실행

```bash
python3 producer_account.py
```

- `account-events` 토픽으로 이벤트 전송
- 일부 이벤트는 `amount` 필드가 누락된 상태로 전송됨

### 2️⃣ 카드 결제 Producer 실행

```bash
python3 producer_card.py
```

- `card-events` 토픽으로 이벤트 전송
- 일부 이벤트는 형식이 깨진 상태로 전송됨

### 3️⃣ 스트림 프로세서 실행

```bash
python3 stream_processor.py
```

스트림 프로세서의 역할:
- `account-events`, `card-events` 두 토픽을 읽는다
- 이벤트를 간단한 공통 포맷으로 변환한다
- 정상 이벤트 → `risk-check-events`
- 처리 실패 이벤트 → `transaction-dlq`

📌 이 프로세서는 **Consumer이자 Producer** 역할을 동시에 한다.

### 4️⃣ 최종 Consumer 실행

```bash
python3 consumer_risk.py
```

- `risk-check-events` 토픽을 읽는다
- 스트림 프로세서에서 정리된 거래만 출력된다
- 100만원 이상 거래는 `⚠️ 고액 거래`로 표시된다

---

## 3단계: 결과 확인

### 1. 파이프라인 흐름 확인

- 두 Producer의 이벤트가 동시에 출력된다
- Consumer는 하나의 토픽만 읽는데도 모든 거래를 확인할 수 있다

👉 Kafka가 중간 처리 파이프라인으로 사용될 수 있음을 확인

### 2. DLQ 동작 확인

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic transaction-dlq \
  --from-beginning
```

- 깨진 이벤트만 DLQ 토픽에 저장됨
- 정상 이벤트는 포함되지 않음
