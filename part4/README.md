# 4편. S3 알림 누락과 보정

[시리즈 목차](../README.md#시리즈-읽기-가이드) · [한국어 · KeyFlow](https://www.keyflow.me/@curogom/post/2026-11-24-%EB%A1%9C%EC%BB%AC%EC%97%90%EC%84%9C-%EC%8B%A4%ED%96%89%ED%95%98%EB%8A%94-aws-%EC%8B%A4%ED%97%98%EC%8B%A4-4-s3-%EC%95%8C%EB%A6%BC%EC%9D%B4-%EB%B9%A0%EC%A7%84-%ED%8C%8C%EC%9D%BC%EC%9D%80-%EC%96%B4%EB%96%BB%EA%B2%8C-%EC%B0%BE%EC%9D%84%EA%B9%8C%EC%9A%94) · [English · Medium](https://curogom.dev/running-aws-locally-part-4-how-do-you-find-a-file-when-its-s3-notification-is-missing-fb8affe5dd7c)

S3 업로드 알림이 SQS를 거쳐 워커로 도착하는 경로와, 알림이 빠진 파일을 S3 원본 목록과 DynamoDB 결과로 대조해 찾는 경로를 비교합니다.

## 무엇을 보면 좋을까요?

- 정상 파일은 수동 이벤트가 아닌 실제 S3 객체 알림을 통해 결과를 남깁니다.
- 알림을 끄고 올린 파일은 **보정 전 결과 행이 없어야** 누락 재현이 성립합니다. 이후 원본을 대조해 해당 키만 보정합니다.
- 보정 뒤 같은 키의 늦은 메시지가 도착해도 조건부 쓰기로 기존 결과가 유지되는지, 없는 객체에는 결과가 생기지 않는지 봅니다.

## 이 저장소에서 볼 수 있는 범위

원래 4편의 독립 실행 디렉터리는 공개 `main`에 없습니다. 같은 핵심 실습은 [5편에 포함된 시나리오 스냅샷](../part5/scenario.py)을 [5편 README의 절차](../part5/README.md)로 실행해 볼 수 있습니다. 4편의 별도 진단 예제 전체나 실제 AWS 운영 환경을 재현하는 것은 아닙니다.

이전: [3편](../part3/README.md) · 다음: [5편](../part5/README.md)
