# E-Commerce Backend

주문 -> 재고 차감 -> 결제 -> 실패 시 보상 처리까지의 이커머스 주문 흐름을
MSA와 이벤트 기반 아키텍처로 직접 설계하고 구현한 백엔드 프로젝트 입니다.

단순 CRUD 구현보다 실제 분산 환경에서 발생할 수 있는

* 재고 동시성 문제
* 중복 API 요청
* Kafka 메시지 중복 소비
* DB 저장과 이벤트 발행 간 정합성
* 서비스 간 트랜잭션 분리
* 결제 실패 시 보상 처리

와 같은 문제를 직접 구현하고 해결하는 데 초점을 맞췄습니다.

---

## Architecture

```text
 Client
  │
  ▼
API Gateway
  ├── /orders/**   → Order Service
  └── /products/** → Product Service

Order Service
  └─ order-created → Kafka → Product Service

Product Service
  ├─ inventory-decreased      → Kafka → Order Service
  └─ inventory-decrease-failed → Kafka → Order Service

Order Service
  ├─ 재고 성공 → RESERVED → order-payment → Kafka → Payment Service
  └─ 재고 실패 → FAILED

Payment Service
  ├─ payment-success → Kafka → Order Service → COMPLETED
  └─ payment-failed  → Kafka → Order Service → FAILED
                                      │
                                      └─ increase-inventory → Kafka → Product Service
```

---

# 핵심 설계

| 문제                  | 적용 방식                  |
|---------------------| ---------------------- |
| 서비스 구조              | Hexagonal Architecture |
| 서비스 간 결합            | Kafka 기반 이벤트 통신        |
| 분산 트랜잭션 처리          | Saga Orchestration     |
| 결제 실패 후 재고 복구       | 보상 이벤트                 |
| DB 저장 후 Kafka 발행 실패 | Transactional Outbox   |
| 주문 API 중복 요청        | `Idempotency-Key`      |
| Kafka 메시지 중복 소비     | `eventId` 기반 Inbox 처리  |
| 동시에 발생하는 재고 차감      | 조건부 UPDATE             |
| 외부 서비스 접근           | API Gateway            |

---

# Hexagonal Architecture

Order / Product / Payment Service에 Hexagonal Architecture를 적용했습니다.

```text
adapter.in
   ↓
application.port.in
   ↓
application.service
   ↓
application.port.out
   ↓
adapter.out
```

비즈니스 로직이 Kafka, JPA, REST API 같은 외부 기술에 직접 의존하지 않도록
Port와 Adapter를 기준으로 역할을 분리했습니다.


---

# Saga Orchestration

MSA에서는 Order, Product, Payment Service가 각각 독립적인 DB 트랜잭션을 사용하기 때문에
하나의 `@Transactional`로 서비스 전체 주문 과정을 처리할 수 없습니다.

따라서 Order Service를 중심으로 각 처리 결과를 받아 다음 작업을 결정하는
 Orchestration 기반 Saga 흐름을 구성했습니다.

```text
Order CREATED
     ↓
재고 차감
     ↓
Order RESERVED
     ↓
결제 요청
     ↓
결제 성공
     ↓
Order COMPLETED
```

각 서비스는 로컬 Transaction만 처리하고
처리 결과를 Kafka 이벤트로 전달합니다.

---

## 실패 처리

### 재고 차감 실패

```text
Order CREATED
    ↓
Product Service
    ↓
재고 부족
    ↓
inventory-decrease-failed
    ↓
Order FAILED
```

재고 차감 이전에는 변경된 데이터가 없기 때문에 별도의 보상 트랜잭션 없이 주문을 실패 처리합니다.

### 결제 실패

```text
재고 차감 성공
    ↓
Order RESERVED
    ↓
결제 요청
    ↓
Payment FAILED
    ↓
increase-inventory
    ↓
재고 복구
    ↓
Order FAILED
```

이미 재고가 차감된 이후 결제가 실패한 경우
이전에 성공했던 작업을 역순으로 되돌리는 **보상 트랜잭션**을 수행합니다.

---

# Transactional Outbox

처음에는 주문 저장 후 바로 Kafka 메시지를 발행했습니다.

```text
Order DB 저장
     ↓
Kafka Publish
```

하지만 DB 저장 이후 Kafka 발행 전에 서버가 종료되거나 에러가 발행하게 된다면

```text
DB
Order 저장 성공

Kafka
이벤트 발행 실패
```

상태가 발생할 수 있습니다.

이를 해결하기 위해 Order DB 저장과 Outbox Event 저장을 하나의 DB Transaction으로 처리했습니다.

```text
@Transactional

Order 저장
+
Outbox Event(PENDING) 저장

        ↓

Outbox Publisher

        ↓

Kafka Publish 성공

        ↓

Outbox SENT
```

Kafka 장애가 발생해도 Outbox에 `PENDING` 이벤트가 남기 때문에 다시 발행할 수 있도록 구성했습니다.

---

# API 멱등성 (Idempotency)

사용자의 더블 클릭이나 네트워크 재시도로 동일한 주문 요청이 여러 번 들어올 수 있습니다.

이를 방지하기 위해 주문 API에

```http
Idempotency-Key
```

를 사용했습니다.

```text
POST /orders

Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000
```

Order 테이블에 `idempotencyKey`를 Unique 값으로 저장하고
동일한 Key가 다시 전달되면 새로운 주문을 생성하지 않고 기존 주문 결과를 반환합니다.

```text
동일 요청 3번

POST /orders
POST /orders
POST /orders

        ↓

Order 1개만 생성
```

---

# Kafka Consumer 멱등성 (Idempotency)

Kafka는 Consumer가 메시지를 처리한 후 Offset Commit 전에 장애가 발생하면
동일한 메시지를 다시 전달할 수 있습니다.

재고 차감과 같은 작업이 두 번 실행되면 데이터가 잘못 변경될 수 있기 때문에
이벤트마다 고유한 `eventId`를 부여했습니다.

```text
OrderCreatedEvent

eventId
orderId
goodsId
quantity
```

Product Service에서는 처리한 이벤트를 저장합니다.

```text
Kafka Event
    ↓
eventId 확인
    ↓
이미 처리됨 → return
    ↓
처리되지 않음
    ↓
재고 차감
    ↓
eventId 저장
```

동일한 Kafka 메시지가 다시 전달되더라도
재고가 중복 차감되지 않도록 구성했습니다.

---

# 재고 동시성 처리

동시에 여러 주문이 들어오는 상황에서

```text
재고 = 100

동시에 120개의 차감 요청
```

과 같은 테스트를 수행했습니다.

조회 후 차감하는 방식에서 Lost Update를 확인했고, 조건부 UPDATE로 변경해 재고 확인과 차감을 한 번에 처리했습니다.

```sql
UPDATE products
SET stocks = stocks - :quantity
WHERE id = :productId
AND stocks >= :quantity
```

Update된 Row가 존재하는 경우에만 재고 차감 성공으로 판단합니다.

```text
UPDATE 결과 = 1
→ 재고 차감 성공

UPDATE 결과 = 0
→ 재고 부족
```

Application에서 재고를 조회한 후 수정하는 방식보다
DB에서 한 번의 연산으로 검증과 차감을 수행하도록 구성했습니다.

---

# API Gateway

분산된 서비스의 외부 요청 경로를 하나로 통합하기 위해
API Gateway를 외부 진입점으로 사용했습니다.

```text
Client
   ↓
API Gateway
   ├── /orders/**   → Order Service
   └── /products/** → Product Service
```


현재는 서비스 라우팅을 담당하며
향후 인증/인가와 공통 요청 처리 영역을 Gateway에서 관리할 수 있도록 구성했습니다.

---

# 서비스 구성

### Order Service

주문의 전체 흐름을 관리하는 서비스입니다.

주요 구현:

* 주문 생성 및 상태 관리
* Hexagonal Architecture
* `Idempotency-Key` 기반 API 멱등성
* Transactional Outbox
* Kafka Producer / Consumer
* 재고 결과에 따른 주문 상태 변경
* Payment 요청
* Saga 흐름 및 보상 트랜잭션 제어

GitHub
https://github.com/Jundev21/order-service

---

### Product Service

상품 정보와 재고를 관리합니다.

주요 구현:

* 상품 / 재고 관리
* 조건부 UPDATE 기반 재고 차감
* 재고 동시성 테스트
* Kafka Producer / Consumer
* `eventId` 기반 Consumer 멱등성
* 재고 차감 성공 / 실패 이벤트
* 결제 실패 시 재고 복구 (보상 이벤트)

GitHub
https://github.com/Jundev21/product-service

---

### Payment Service

재고 확보가 완료된 주문의 결제를 처리합니다.

주요 구현:

* Payment 상태 관리
* Kafka 기반 결제 요청 처리
* 결제 성공 / 실패 이벤트
* 결제 실패에 따른 보상 이벤트

GitHub
https://github.com/Jundev21/payment-service

---

### API Gateway

외부 요청의 단일 진입점입니다.

주요 구현:

* `/orders/**` 라우팅
* `/products/**` 라우팅
* 내부 서비스 주소 은닉
* 서비스 접근 경로 통합

---

# 주문 상태

```text
CREATED
   │
   │ 재고 차감 성공
   ▼
RESERVED
   │
   │ 결제 성공
   ▼
COMPLETED
```

실패하는 경우

```text
CREATED
   │
   │ 재고 부족
   ▼
FAILED
```

또는

```text
RESERVED
   │
   │ 결제 실패
   ▼
재고 복구
   │
   ▼
FAILED
```

---

# 기술 스택

### Backend

* Java 21
* Spring Boot
* Spring Data JPA
* Spring Cloud Gateway

### Messaging

* Apache Kafka

### Database

* MySQL

### Test

* JUnit 5
* Mockito

### Build

* Gradle

---

# 프로젝트에서 다룬 문제

이 프로젝트에서는 기능 구현 자체보다
 왜 문제가 발생하고 어떤 방식으로 해결할 수 있는지 를 직접 확인하는 것을 목표로 했습니다.

```text
재고 동시성
-> 조건부 UPDATE

API 중복 요청
-> Idempotency-Key

Kafka 중복 소비
-> eventId 기반 멱등성 처리

DB / Kafka 정합성
-> Transactional Outbox

서비스 간 분산 트랜잭션
-> Saga Orchestration

결제 실패
-> 보상 Transaction

서비스 내부 의존성
-> Hexagonal Architecture
```

---

# 프로젝트를 통해 학습한 내용

MSA에서는 단순히 서비스를 나누는 것보다
서비스가 분리되면서 발생하는 데이터 정합성과 실패 처리를 어떻게 설계하는지가 중요하다는 것을 경험했습니다.

특히 하나의 주문 과정에서도

```text
주문
-> 재고
-> 결제
```

각 과정이 서로 다른 로컬 Transaction으로 실행되기 때문에

* 중복 요청
* 중복 이벤트
* 메시지 재전송
* 이벤트 발행 실패
* 일부 서비스 처리 실패

를 고려해야 했습니다.

이를 해결하기 위해 멱등성, Outbox Pattern, Saga 및 보상 트랜잭션을 직접 적용하면서
이벤트 기반 MSA의 주문 처리 흐름을 구현했습니다.
