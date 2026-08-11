# BOAZ Kafka 실습: 은행 거래 실시간 처리

## 실습 목표

Kafka를 이용해 **여러 Producer → 스트림 처리 → 하나의 Consumer** 로 이어지는

기본적인 스트리밍 파이프라인 구조를 직접 실행해보는 것이 목표! 

## 실습 환경

- Apache Kafka (Docker / Docker Compose)
- Python 3
- kafka-python-ng

## 실습 내용

- 여러 Producer(계좌 거래 / 카드 결제)가 동시에 Kafka로 이벤트를 전송
- 스트림 프로세서가 두 이벤트를 하나의 포맷으로 정리
- 처리 실패한 이벤트는 DLQ(Dead Letter Queue)로 분리
- 최종 Consumer가 정리된 거래를 받아 고액 거래 여부 확인
- 과제: 위험도(`risk_level`) 판단 로직 추가해보기
