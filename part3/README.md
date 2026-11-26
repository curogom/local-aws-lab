# 3편. RAG 문서 중복 처리

[시리즈 목차](../README.md#시리즈-읽기-가이드) · [한국어 · KeyFlow](https://www.keyflow.me/@curogom/post/2026-11-19-%EB%A1%9C%EC%BB%AC%EC%97%90%EC%84%9C-%EC%8B%A4%ED%96%89%ED%95%98%EB%8A%94-aws-%EC%8B%A4%ED%97%98%EC%8B%A4-3-rag%EC%97%90-%EA%B0%99%EC%9D%80-%EB%AC%B8%EC%84%9C%EB%A5%BC-%EB%91%90-%EB%B2%88-%EB%84%A3%EB%8A%94%EB%8B%A4%EB%A9%B4) · [English · Medium](https://curogom.dev/running-aws-locally-part-3-what-if-a-rag-pipeline-receives-the-same-document-twice-8e7b714c18a3)

같은 문서를 다시 받은 경우는 기존 결과를 재사용하고, 내용이나 처리 설정이 바뀐 경우에는 새 버전으로 처리하는 RAG 문서 파이프라인 실습입니다. DynamoDB는 AWS Emulator에서, PostgreSQL과 pgvector는 실제 로컬 DB 컨테이너에서 검증했습니다.

## 무엇을 보면 좋을까요?

- **같은 작업과 새 버전**: 작업 ID, 입력 해시와 모델 리비전이 재시도·재처리를 어떻게 구분하는지 보세요.
- **처리 권한과 저장 경계**: DynamoDB 조건부 쓰기와 PostgreSQL의 고유 제약·트랜잭션을 비교합니다. 두 저장소를 함께 쓸 때 청크 저장과 완료 처리가 서로 다른 커밋 경계에 있다는 점이 중요합니다.
- **검색 노출 조건**: 활성 버전과 완료 상태뿐 아니라 테넌트와 모델이 맞는 결과만 검색에 나타나는지 확인합니다. 합성 문서 검색 결과를 일반 검색 정확도나 인증·격리 보장으로 확대 해석하지 않습니다.

## 이 저장소에서 볼 수 있는 범위

이 회차의 독립 실습 코드는 공개 `main`에 포함되어 있지 않습니다. 여기서는 발행 글의 실험 절차, 표, 관측과 한계를 따라 읽을 수 있습니다. 루트 빠른 시작이나 5편 실행기로 3편 RAG 실험이 실행되지는 않습니다.

이전: [2편](../part2/README.md) · 다음: [4편](../part4/README.md)
