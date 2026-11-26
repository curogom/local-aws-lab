# 1편. SQS와 Kafka 장애 복구

[시리즈 목차](../README.md#시리즈-읽기-가이드) · [한국어 · KeyFlow](https://www.keyflow.me/@curogom/post/2026-11-15-%EB%A1%9C%EC%BB%AC%EC%97%90%EC%84%9C-%EC%8B%A4%ED%96%89%ED%95%98%EB%8A%94-aws-%EC%8B%A4%ED%97%98%EC%8B%A4-1-sqs%EC%99%80-kafka-%EC%9E%A5%EC%95%A0-%EB%B3%B5%EA%B5%AC) · [English · Medium](https://curogom.dev/running-aws-locally-part-1-sqs-and-kafka-crash-recovery-69736e38363c)

메시지 처리가 끝난 시점과 브로커에 완료를 알린 시점 사이에서 프로세스가 멈추면 무엇이 다시 처리되는지 살펴봅니다. SQS는 AWS Emulator, Kafka는 별도의 실제 브로커를 사용합니다.

## 무엇을 보면 좋을까요?

- SQS의 ACK(메시지 삭제) 전 실패는 visibility timeout 이후의 재전달로, Kafka의 offset 미커밋은 같은 소비자 그룹의 재처리로 나타납니다.
- 결과가 이미 SQLite에 저장된 뒤 다시 메시지를 받아도 중복 결과 행이 생기지 않는지 확인하세요. 저장과 중복 억제는 같은 트랜잭션 경계에 있습니다.
- 기본 검증의 no-ACK 모사와 고급 시나리오의 실제 자식 프로세스 종료를 구분하세요. 실습에서 본 큐 적재 한도나 지연 시간은 실제 AWS SQS 한도나 Kafka와의 성능 비교에 쓰지 않습니다.

## 실습과 결과

저장소 루트에서 [준비 사항과 격리 규칙](../README.md#빠른-시작)을 확인한 뒤 `./setup.sh`와 `./run-all.sh`를 실행합니다. 이 명령에는 2편 S3 실습도 포함됩니다. 실제 종료·재시작과 추가 시나리오는 루트에서 `./run-all.sh --advanced`로 확인합니다.

- 코드: [기본 실행](../lab.py) · [SQS 고급 시나리오](../sqs_scenarios.py) · [Kafka 고급 시나리오](../kafka_scenarios.py)
- 관측: [SQS 샘플](../examples/results/sqs.json) · [Kafka 샘플](../examples/results/kafka.json) · [해석과 한계](../FINDINGS.md)

다음: [2편 S3 파일 처리와 재시도](../part2/README.md)
