# 2편. S3 파일 처리와 재시도

[시리즈 목차](../README.md#시리즈-읽기-가이드) · [한국어 · KeyFlow](https://www.keyflow.me/@curogom/post/2026-11-17-%EB%A1%9C%EC%BB%AC%EC%97%90%EC%84%9C-%EC%8B%A4%ED%96%89%ED%95%98%EB%8A%94-aws-%EC%8B%A4%ED%97%98%EC%8B%A4-2-s3-%ED%8C%8C%EC%9D%BC-%EC%B2%98%EB%A6%AC-%ED%8C%8C%EC%9D%B4%ED%94%84%EB%9D%BC%EC%9D%B8%EA%B3%BC-%EC%9E%AC%EC%8B%9C%EB%8F%84) · [English · Medium](https://curogom.dev/running-aws-locally-part-2-an-s3-file-processing-pipeline-and-retries-7b0fc9570c1a)

S3에 원본을 올린 뒤 워커가 어느 단계에서 멈췄는지에 따라 재시도 직전의 상태가 달라집니다. 상태는 달라도 재시도 후 출력이 같은지 확인합니다.

## 무엇을 보면 좋을까요?

- 업로드 URL 발급, S3 객체 저장, 앱의 바이트 검증은 서로 다른 단계입니다. 실제 presigned PUT 뒤 읽어온 바이트와 SHA-256을 확인하세요.
- 쓰기 전 실패에는 결과 객체가 없고, 쓰기 후 응답 전 실패에는 이미 결과가 남습니다. 두 경우의 재시도 전후 객체 수와 해시를 비교합니다.
- 같은 요청 ID로 다른 바이트가 들어오는 경우 원본을 보존합니다. 이 순차 검사는 동시 업로드의 원자성이나 exactly-once를 보장하지 않습니다.

## 실습과 결과

[루트 준비 사항](../README.md#빠른-시작)을 확인한 뒤 저장소 루트에서 `./setup.sh`, `./scripts/s3-run.sh` 순으로 실행하세요. 이 실행은 전용 에뮬레이터만 사용합니다.

- 코드: [S3 파이프라인](../s3_pipeline.py) · [실제 워커 종료 검증](../s3_crash_probe.py)
- 관측: [기본 10개 사례](../examples/results/s3-pipeline.json) · [종료 전후 사례](../examples/results/s3-crash-probe.json) · [상세 실험 기록](../docs/part2-s3.md)

이전: [1편](../part1/README.md) · 다음: [3편](../part3/README.md)
