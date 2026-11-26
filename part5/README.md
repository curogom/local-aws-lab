# 로컬에서 실행하는 AWS 실험실 5. 초기화 후 같은 실험으로 확인하기

4회차 S3 → SQS → DynamoDB 실습을 **새 에뮬레이터 두 개**에서 순차 실행한다. 2·3회차 전체, 실제 AWS, CI, 성능 또는 장애 복구 시간의 검증은 아니다. `scenario.py`는 이전 실습을 복사한 독립 실행용 스냅샷이다.

## 실행

Apple Silicon Mac에서 Docker Desktop(실행 중), Python 3.10 이상, AWS CLI v2, curl, GnuPG(gpg/gpgv), shasum이 필요합니다. Docker가 공유하는 홈 아래에 저장소를 복제하세요. `19498` 포트가 비어 있어야 합니다.

```bash
git clone https://github.com/curogom/local-aws-lab.git
cd local-aws-lab/part5
python3 -B -m unittest -v test_replay
(cd agent && python3 -B -m unittest -v test_replay)

./build.sh
image="$(docker --host "unix://$HOME/.docker/run/docker.sock" image inspect \
  --format '{{.Id}}' local-aws-lab-part5-emulator:1.0.490)"
python3 -B replay.py --image "$image" --expected-image-id "$image" --expect-version 1.0.490
```

`build.sh`는 공식 ARM64 1.0.490 RPM을 그때 내려받아 SHA-256, AWS 공개키 지문 및 분리 서명을 검증합니다. Docker 빌드에서 RPM 내부 서명도 검증합니다. AWS가 과거 버전 배포를 중단하거나 서명키 검증에 실패하면 빌드는 중단됩니다. RPM이나 이미지 바이너리는 이 저장소에 포함되지 않습니다. 이미지 ID는 빌드마다 다를 수 있으므로 위 명령에서 직접 얻습니다. 1.0.490은 실험용 버전으로 고정했으며 운영 보안 권고가 아닙니다. [패키지 버전 안내](https://docs.aws.amazon.com/emulator/latest/userguide/versioning.html) · [AWS 공식 서명 검증](https://docs.aws.amazon.com/emulator/latest/userguide/getting-started-linux.html#verify-the-package-before-installing).

Colima는 기본 VM 대신 전용 `part5-repro` 프로필에서 검증했습니다. 기존 동일 이름 프로필이 자신의 실습용인지 먼저 확인하세요. Homebrew Docker가 `/opt/homebrew/bin/docker`에 있어야 합니다.

```bash
colima start --profile part5-repro --activate=false --cpu 2 --memory 3
export PATH="$PWD/colima:$PATH"
```

같은 셸에서 위의 `./build.sh`부터 실행하고, 끝나면 `colima stop --profile part5-repro`로 전용 VM을 중지하세요. 이 어댑터는 Docker 소켓만 전용 프로필로 돌리며 실습 코드·입력·포트는 바꾸지 않습니다. 포트는 loopback으로만 묶고 실제 AWS 프로필이나 자격증명은 사용하지 않습니다. 임시 테스트 자격증명과 실행 증거는 공개하지 마세요.

잘못된 버전이 시나리오 전에 차단되는지도 볼 수 있습니다. 같은 활성 환경에서 아래 명령의 의도된 종료 코드는 1입니다.

```bash
python3 -B replay.py --image "$image" --expected-image-id "$image" --expect-version 0.0.0
```

## 무엇을 비교하는가

1. 각 실행의 새 컨테이너가 준비되면 S3 버킷·SQS 큐·DynamoDB 테이블 목록을 읽어 모두 비어 있는지 확인한다. 설치 버전이 다르거나 작은 읽기 호출에서 실패하면 실습을 시작하지 않는다.
2. 정상 업로드의 실제 S3 알림, 알림을 끈 파일의 누락과 보정, 이후 수동으로 넣은 동일 파일의 메시지 두 건, 존재하지 않는 객체의 결과 미생성을 검증한다. 정상 경로가 수동 이벤트로 대체되면 실패다.
3. 두 실행의 고유 ID가 다르고, 최종 행 두 건의 키·바이트 수·SHA-256·처리 경로와 검증 판정이 같아야 한다. 두 번 다 틀린 결과도 통과하지 않는다. 무작위 메시지 ID·실행 ID·관측 시간은 내용 비교에서 제외한다.
4. 실행기의 이름과 소유 라벨이 일치하는 컨테이너의 로그를 저장한 뒤 확인한 ID만 정리한다. 포트 충돌 시 다른 컨테이너를 종료하지 않는다. 정리 확인에 실패하면 두 번째 실행을 진행하지 않는다. 강제 종료나 데몬 중단 상태에서는 정리가 보장되지 않으므로 보고서의 컨테이너 이름과 소유 라벨을 먼저 확인한다.

표준 출력에 표시한 `evidence/<session>/`에 `comparison.json`, 가능한 경우 각 `run-*.json`과 로그가 남는다. 자동 테스트에 포함된 `fixtures/archived-p4-run-*.json`은 예전 4회차 기록이며 새 컨테이너 재실행 증거가 아니다.

## AI Agent는 별도 실습

[agent/README.md](agent/README.md)의 읽기 전용 진단기는 생성된 증거만 해석한다. 빌드, Docker, AWS 호출이나 재시도를 자동 실행하지 않는다. 실행 판정은 위 결정적 비교기 결과가 기준이다.

## 두 Mac에서 확인한 결과

[정규화한 증거](examples/cross-host-comparison.json)에 맥북과 맥미니의 서로 다른 이미지 ID, 동일한 실행 코드 지문, 판정 상태를 남겼습니다.

- MacBook / Docker Desktop 캐시 이미지와 Mac mini / Colima 별도 빌드 이미지에서 각각 새 컨테이너 두 개를 실행했습니다. 네 실행 모두 S3·SQS·DynamoDB 사전 목록이 0/0/0, 기본 판정 6/6, 컨테이너 정리 성공이었습니다.
- 네 실행의 최종 두 행은 키, 바이트 수, SHA-256, 처리 경로가 일치했습니다.
- 두 호스트 모두 기대 버전 `0.0.0`에서 실제 설치 버전 `1.0.490`을 읽고 시나리오 시작 전에 중단했습니다. 종료 코드 1, 두 컨테이너 모두 정리 완료였습니다.
- Python 단위 테스트는 실행기 8개와 읽기 전용 에이전트 4개입니다. 실환경 AWS, 다른 아키텍처, 성능 또는 시리즈 전체의 재현을 뜻하지 않습니다.
