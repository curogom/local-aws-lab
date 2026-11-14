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
실패한 검증은 0이 아닌 종료 코드를 반환합니다.
실제 프로세스 종료·동시 소비자·지속 유입까지 실행하려면 다음 명령을 사용합니다(POSIX 환경).

```bash
./run-all.sh --advanced
```

전체 실행도 전용 컨테이너만 순차적으로 시작/정지하며, 기존 9297 에뮬레이터에는 손대지 않습니다. JSON/SQLite는 `results/`에 생성됩니다.

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

- 기본 `lab.py`는 close/no-ack 모사입니다. 고급 시나리오는 실제 자식 프로세스 SIGKILL과 새 프로세스 재시작을 검증합니다.
- 외부 DB/결제/메일 부작용의 exactly-once를 증명하지 않습니다.
- 고급 Kafka 시나리오는 2개 동시 소비자의 join/leave 재할당을 검증합니다. 복제 장애·다중 AZ, IAM·실제 AWS quota·부하 테스트는 하지 않습니다.
- 기본 예제 p95는 handler+ack이며 elapsed에 빈 poll 대기가 포함됩니다. 고급 stream p50/p95/p99는 송신 admission 시점부터 결과 저장+ACK 완료까지 동일 호스트 시계로 측정하며, admission을 기다린 시간은 제외합니다.
- 실제 Kafka와 에뮬레이터 SQS의 속도를 제품 간 벤치마크로 비교하지 마세요.

## 관측된 에뮬레이터 제한

AWS Emulator **1.0.367**에서 큐당 1,000개 메시지 사전 적재 후 추가 전송에 `OverLimit`이 발생했습니다.
이는 **실제 AWS SQS 한도가 아닙니다.** `probe_limit.py`로 별도 확인하며 향후 버전에서 달라질 수 있습니다.
기본 실습은 550개로 제한합니다. Dockerfile은 AWS 공식 `LTS/latest` 패키지를 받으므로 새 빌드 시 버전이 달라질 수 있습니다.
시작 스크립트는 예상 버전 `1.0.367`과 다르면 실행을 거부합니다. 공식 배포 URL은 latest이므로 바이너리 자체를 영구 고정한 것은 아닙니다. 버전 업데이트 시 동작을 검토하고 `EXPECTED_EMULATOR_VERSION`을 명시적으로 바꾸세요. 실행 버전·이미지 ID는 `results/environment.json`에 기록합니다. 저장된 관측 결과는 `examples/results/`, 설명은 `FINDINGS.md`에 있습니다.

## 공식 근거

- [AWS Emulator 범위와 제한](https://docs.aws.amazon.com/emulator/latest/userguide/what-is-service.html)
- [AWS Emulator Docker 설치](https://docs.aws.amazon.com/emulator/latest/userguide/getting-started-docker.html)
- [SQS Standard](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html)
- [Visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [Dead-letter queue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [Kafka Quickstart](https://kafka.apache.org/quickstart/)

MIT License. 제3자 Docker 이미지·AWS Emulator 패키지에는 해당 프로젝트의 라이선스가 적용됩니다.

## 고급 실험 구성

| 관점 | 모듈 | 실제 검증 |
|---|---|---|
| SQS process crash | `sqs_crash.py` | commit 전/후 SIGKILL, 새 워커 재시작, 결과 1행 |
| Retry/DLQ | `sqs_retry.py` | 일시 실패 2회차 성공, poison 3회 후 native DLQ, 수동 수리·재전송 |
| 지속 유입/backpressure | `sqs_stream.py` | 1,200건 concurrent 생산/소비, admission 64개 상한, 느린 워커 회복 |
| Kafka process crash | `kafka_crash.py` | DB commit 후 offset 미커밋 SIGKILL, 중복 억제·lag 0 |
| Kafka group rebalance | `kafka_rebalance.py` | A 단독→A/B 분할→B 퇴장→A 전체 재할당, 180건 정합성 |
| 측정·CI | `record_environment.py`, workflows | 버전/환경·재현 결과, 단위 검사와 Docker 통합 실행 |

SQS 고급 CLI: `.venv/bin/python sqs_scenarios.py crash|retry|stream|all`.
Kafka 고급 CLI: `.venv/bin/python kafka_scenarios.py --case crash|rebalance|all`.
해당 전용 브로커가 실행 중일 때 개별 CLI를 사용합니다.

Backpressure는 SQS 자체의 기능이 아니라 **애플리케이션의 bounded admission semaphore**입니다.
최대 outstanding은 승인된 송신 수에서 효과 저장 및 ACK 완료 수를 뺀 값이며 AWS ApproximateNumberOfMessages가 아닙니다.
정상 스트림에서 64개 상한과 최종 0을 검증합니다. 이 in-memory admission 제어 자체의 프로세스 장애 복구는 아직 검증하지 않았습니다.
Retry/DLQ에서 실패 메시지를 수리한 후 재전송하는 단계는 앱의 수동 requeue이며, AWS redrive task API 지원을 주장하지 않습니다.

고급 시나리오 결과는 `results/sqs-advanced-*.json`, `results/qlab-process-*/report.json`에 저장됩니다.
CI는 Linux/Python3.12에서 실제 Docker 통합을 실행하고 합성 JSON만 artifact로 보관합니다.
데이터량 1,200건은 단일 에뮬레이터의 순간 적재 한도를 넘겨 스트리밍한 기능 실험일 뿐, 상용 대용량 처리 성능의 증거가 아닙니다.
