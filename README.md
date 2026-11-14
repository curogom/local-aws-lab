# Queue Reliability Lab

SQS와 Kafka에서 **결과 저장 후 ACK/offset commit 전에 실패하면 무엇이 남는가**를 살펴보는 작은 실습입니다.
AWS Emulator의 SQS와 실제 Apache Kafka를 사용합니다. 처리량 순위나 상용 대용량 운영 검증이 아닙니다.

## 빠른 시작

필수: 실행 중인 Docker 엔진, Python 3.10 이상, Bash, curl. `-O`/`PYTHONOPTIMIZE`는 지원하지 않습니다. ARM64 Mac에서 검증했습니다.
프로젝트는 Docker에 파일 공유된 경로(Colima 기본값에서는 홈 디렉터리)에 clone하세요. 공유되지 않은 `/tmp`에서는 인증서 파일이 호스트에 보이지 않아 readiness가 실패할 수 있습니다.
Docker VM은 2 CPU/4 GiB로 시험했으며, 다른 무거운 작업과 병행하지 않는 것을 권장합니다.
최초 실행에는 공식 이미지/패키지와 Python 의존성을 다운로드할 인터넷이 필요합니다.

```bash
git clone https://github.com/curogom/queue-reliability-lab.git
cd queue-reliability-lab
./setup.sh
./run-all.sh
```

`setup.sh`는 로컬 `.venv`와 작업 디렉터리를 만듭니다.
`run-all.sh`는 단위 테스트 → 전용 AWS Emulator 시작 → SQS/한도 관측 → 에뮬레이터 정지 → Kafka 테스트 → Kafka 정지 순서로 실행합니다.
실패한 검증은 0이 아닌 종료 코드를 반환합니다. JSON/SQLite는 `results/`에 생성됩니다.

### 격리와 안전

- SQS 연결은 코드에서 `https://127.0.0.1:9298`로 고정합니다. 실제 AWS endpoint로 바꾸는 옵션은 없습니다.
- 에뮬레이터가 생성한 `.runtime/emulator/9298/`의 테스트 자격증명과 CA만 사용합니다. 실제 AWS 프로필이나 credential chain을 사용하지 않습니다.
- `queue-reliability-emulator`와 `queue-reliability-kafka`는 이 실습 전용입니다. 기존 `aws-emulator-local`은 중지하지 않습니다.
- 기존 동명 컨테이너는 소유 label을 확인합니다. 실행 중인 실습 컨테이너가 있으면 전체 실행은 거부합니다.
- AWS Emulator는 종료 시 모든 메모리 리소스가 없어집니다. **이 전용 인스턴스에는 보관할 데이터나 다른 작업을 넣지 마세요.**
- 포트는 localhost에만 공개합니다. telemetry OFF. 인증서 검증을 끄지 않습니다.
- Kafka는 단일 브로커, RF1, heap512MiB, 메모리1.5GiB, 3개 테스트 파티션입니다. 컨테이너는 재시작 가능하도록 남습니다.
- SQS 큐는 finally에서, Kafka 토픽은 성공 시 정리합니다. 실패 중단 후 Kafka 토픽이 남을 수 있으며 retention은 메시지만 만료시킵니다.
- 외부 결제/메일/LLM API 호출은 없습니다. 문자열 대문자 변환이 합성 작업입니다.

## 개별 실행

```bash
./scripts/emulator-start.sh
.venv/bin/python lab.py sqs --count 500
.venv/bin/python probe_limit.py
./scripts/emulator-stop.sh

./kafka-start.sh
.venv/bin/python lab.py kafka --count 500
docker stop queue-reliability-kafka
```

두 실습은 자원 절약을 위해 순차 실행합니다. count는 10~900이며 10% 중복 이벤트가 추가됩니다.
Kafka의 로컬 포트는 `19092`입니다. 이미 사용 중이면 다른 서비스를 중단하지 말고 충돌을 먼저 해결하세요.

## 검증 범위

- SQS visibility timeout 만료 후 재전달과 native DLQ (`maxReceiveCount=2`).
- 워커 1/2/4개, 500 unique + 50 duplicate 입력에서 결과 500행과 중복 억제.
- 결과 저장 후 ACK하지 않는 실패 경계, Kafka offset 미커밋 후 consumer 재시작.
- Kafka offset commit 후 lag 0, 별도 그룹과 seek를 이용한 550개 replay.
- SQLite PRIMARY KEY와 결과 INSERT를 하나의 transaction으로 처리해 중복 효과 억제.

## 결과를 해석할 때

- 강제 OS kill이 아니라 close/no-ack로 실패 경계를 **모사**합니다.
- 외부 DB/결제/메일 부작용의 exactly-once를 증명하지 않습니다.
- Kafka rebalance·복제 장애·다중 AZ, IAM·실제 AWS quota·부하 테스트는 하지 않습니다.
- p95는 handler+ack 시간이며 end-to-end latency가 아닙니다. elapsed에는 빈 poll 대기가 포함됩니다.
- 실제 Kafka와 에뮬레이터 SQS의 속도를 제품 간 벤치마크로 비교하지 마세요.

## 관측된 에뮬레이터 제한

AWS Emulator **1.0.367**에서 큐당 1,000개 메시지 사전 적재 후 추가 전송에 `OverLimit`이 발생했습니다.
이는 **실제 AWS SQS 한도가 아닙니다.** `probe_limit.py`로 별도 확인하며 향후 버전에서 달라질 수 있습니다.
기본 실습은 550개로 제한합니다. Dockerfile은 AWS 공식 `LTS/latest` 패키지를 받으므로 새 빌드 시 버전이 달라질 수 있습니다.
정확한 비교에는 실행 버전을 기록하세요. 저장된 관측 결과는 `examples/results/`, 설명은 `FINDINGS.md`에 있습니다.

## 공식 근거

- [AWS Emulator 범위와 제한](https://docs.aws.amazon.com/emulator/latest/userguide/what-is-service.html)
- [AWS Emulator Docker 설치](https://docs.aws.amazon.com/emulator/latest/userguide/getting-started-docker.html)
- [SQS Standard](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html)
- [Visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [Dead-letter queue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [Kafka Quickstart](https://kafka.apache.org/quickstart/)

MIT License. 제3자 Docker 이미지·AWS Emulator 패키지에는 해당 프로젝트의 라이선스가 적용됩니다.
