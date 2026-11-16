# 실제 실행 기록과 블로그용 근거

검증 기기: Mac mini M4 10core, RAM16GiB. Docker/Colima 2CPU4GiB. AWS Emulator1.0.367. Apache Kafka4.4.0 ARM64. Python3.14. boto3/kafka-python 버전은 requirements.txt.
작성/실행: 2026-11-15 KST (UTC 기준 11-14). 과거 회사 경험이 아닌 새로운 로컬 실습.

## 첫 실패: 1,000 메시지 제한
1,000 unique + 100 duplicate를 한 큐에 사전 적재하려던 첫 실행에서 SendMessageBatch가 OverLimit을 반환했다.
오류: `This emulator holds at most 1000 messages per queue; ... cannot take 10 more. Delete messages from it first.`
이 제한은 설치된 AWS Emulator의 제한이다. 실제 SQS의 큐 저장량 제한으로 일반화하지 않는다.
기능 검증을 500 unique+50 duplicates로 축소했다. 생산과 소비를 동시에 실행하는 대량 스트리밍 실험은 이번에 하지 않았다.

## SQS 결과
- visibility timeout 1초 설정 후 1.5초 뒤 같은 MessageId 재전달 확인.
- native RedrivePolicy maxReceiveCount2에서 poison message가 DLQ로 이동. 애플리케이션이 복사한 것이 아님.
- 작업 효과를 저장한 뒤 DeleteMessage하지 않고 상태를 닫았다가 재처리. 같은 id가 다시 들어와도 SQLite 결과1행 유지.
- 1/2/4 workers 각각550개 입력에서500개 효과+50개 중복 억제, 저장500행.
- 기록된 처리율은 매우 작은 로컬 실습의 관측치이며, 마지막 빈 poll 대기2회가 elapsed에 포함된다. 병렬 성능 향상이나 AWS 처리량을 추정할 자료가 아니다.
- p95는 SQLite effect+delete ack 처리 시간이며, 큐 대기시간을 포함한 end-to-end latency가 아니다.

## Kafka 결과
- 실제 공식 Apache Kafka4.4.0 단일 브로커. 3partitions/RF1.
- 550 events 생산 후 첫 효과1개를 저장하고 offset commit 없이 consumer를 닫았다.
- 같은 group 재시작 후499개 새 효과+51개 중복 억제. DB500행.
- offsets commit 후 각 partition lag0.
- 독립 audit group550개 읽기, seek_to_beginning550개 재처리.
- 이는 consumer close/restart 실험이지 강제 프로세스 kill, 브로커 장애, multi-AZ, rebalance 검증이 아니다.
- 최초 Docker 설정에서 process.roles 누락 오류를 만났고 KRaft 설정을 명시해 수정했다. 최종 스크립트에 반영했다.

## 멱등성의 정확한 범위
SQLite PRIMARY KEY(event_id)와 결과 저장이 하나의 INSERT/transaction이다. 작업 효과와 멱등 레코드를 분리하지 않았다.
외부 메일/결제/API의 exactly-once를 증명하지 않으며, 그 경우 outbox/inbox 혹은 상대 시스템 idempotency key 등 추가 설계가 필요하다.
문서 분석은 문자열 upper()로 대체한 합성 작업이다. 실제 RAG/LLM 호출과 비용은 없다.

## 실제 자원 관측
첫 Kafka 테스트 후 docker stats: 332.3MiB/1.5GiB. CPU3.02%의 일회 snapshot.
호스트 memory pressure 정상(level1), swap0. 이것도 지속 부하 성능 결과가 아니다.

## 최초 실습 이후의 후보 (아래 보강으로 일부 완료)
- 실제 강제 종료·재시작, bounded backpressure
- offset commit 경계와 외부 DB 트랜잭션을 outbox/inbox로 확장
- 여러 consumer 동시 group join/leave와 rebalance
- SQS production과 consumption을 동시에 진행하여 한도 이하 backlog로 대량 흘리기
- 반복 측정·warm-up·E2E latency·queue age·lag time-series
- 실제 AWS 소규모 검증 (별도 사용자 승인 및 비용 상한 필요)

공식 문서 근거는 README에 기록. 발행 전 사용자 검토 필요.

## 공개 전 재검증 (2026-11-15 KST)

처음 예제의 외부 폴더와 기존 가상환경 의존성을 제거했다. 새 소스 복사본과 새 Python 가상환경에서 setup 및 전체 통합 테스트를 다시 실행했다.
전용 AWS Emulator(9298)와 label이 있는 전용 Kafka를 사용한다. 기존 AWS Emulator(9297)는 중지하지 않았다.
깨끗한 홈 경로에서 전체 실습이 성공했으며, Colima에 공유되지 않은 /tmp 경로는 bind mount 인증서가 보이지 않아 실패했다. README에 파일 공유 조건을 명시했다.
Kafka 성공 판정에 offset/lag 0 검증을 추가하고, 정상 완료한 테스트 토픽은 삭제한다. 실패 시 토픽은 남을 수 있다.
실행 결과 샘플은 examples/results에 합성 JSON만 포함하고, 로컬 경로 로그·SQLite·인증서·자격증명·미게시 블로그는 공개하지 않는다.
CI는 단위 테스트와 구문 검사만 실행한다. Docker 통합 테스트는 Mac mini의 실제 로컬 실행 결과이며 CI 통합 성공을 주장하지 않는다.

존재하지 않는 AWS_PROFILE과 외부 AWS_ENDPOINT_URL을 설정한 별도 실행에서도 전체 실습이 성공했다. 런타임 config/credentials는 프로젝트 전용 파일로 고정했다.
Python 최적화 모드(-O)는 이번 테스트 하네스에서 지원하지 않으며 시작 시 명시적으로 거부한다.


## 실제 프로세스·동시 처리 보강

기존 기본 실습과 별도로 `./run-all.sh --advanced`를 Mac mini에서 통합 실행하여 성공했다.
- SQS: 실제 worker를 SIGKILL(-9)한 뒤 새 프로세스로 복구. commit 전에는 0→1행, commit 후 ACK 전에는 1→1행이며 큐가 비었다.
- SQS retry: 일시 오류는 2회째 성공, poison은 3회 뒤 native DLQ 도착. 합성 오류를 수정한 뒤 애플리케이션 수동 재전송하여 총 2행 확인. AWS redrive API 검증은 아니다.
- SQS 동시 유입: 1,200건을 producer와 worker가 동시에 처리, 애플리케이션 admission 한도 64건을 넘지 않고 모두 저장. 이 실행은 4.37초, E2E p50 77.98ms / p95 905.71ms / p99 1272.02ms였다. 단일 로컬 실행 수치로 실제 AWS 처리량을 추정하면 안 된다.
- Kafka: DB commit 후 offset 전 실제 SIGKILL, 새 worker에서 재전달을 중복 억제하고 최종 lag 0 확인.
- Kafka: 두 consumer가 같은 그룹에서 3개 partition을 나눠 처리하고, 한 consumer 종료 후 나머지가 전부 재할당받아 총 180건 및 lag 0 확인.

E2E는 admission 후 송신 시각부터 effect+ACK 완료까지로, admission 대기 시간은 제외한다. timeline은 애플리케이션 outstanding이며 broker queue age가 아니다. backpressure credit은 메모리 기반이므로 producer 재시작을 견디는 영속 admission 설계를 증명하지 않는다. 반복 benchmark·외부 부작용 outbox·실제 AWS 검증은 아직 하지 않았다.

통합 CI 작업을 추가했다. 실제 CI 실행 상태는 GitHub Actions에서 확인해야 하며 위 수치는 Mac mini 로컬 관측이다. AWS Emulator LTS/latest 빌드의 실제 버전을 기록하고 당시 예상 버전 1.0.367 불일치 시 시작을 거부하도록 했다. 이는 binary digest 고정과는 다르다.

2026-11-16에는 공식 LTS/latest가 1.0.385로 변경되어 CI가 버전 검사에서 중단됐다. 전용 이미지를 새로 빌드하고 1.0.385에서 전체 고급 시나리오를 로컬 재검증한 뒤 기대 버전을 갱신했다. 위 1.0.367 수치는 당시 관측 기록으로 유지한다.
