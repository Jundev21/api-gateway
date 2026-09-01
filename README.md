# E-Commerce Backend

## 프로젝트 목적

이커머스 백엔드 주문, 상품, 재고 처리를 MSA 구조로 직접 구현해보기 위해 만든 학습 프로젝트입니다.

단순 CRUD 기능을 만드는 것보다 실제 주문 처리 과정에서 발생할 수 있는 동시성, 중복 요청, Kafka 메시지 재처리와 같은 문제를 직접 확인하고 해결하는 방식으로 진행하고 있습니다.

현재도 계속 기능을 추가하면서 공부 중인 프로젝트입니다.

---

## 기술 스택

* Java 21
* Spring Boot
* Spring Cloud Gateway
* Spring Data JPA
* MySQL
* Apache Kafka
* JUnit5
* Mockito
* Gradle

---

## 전체 구조

현재는 아래 3개의 서비스로 구성되어 있습니다.

```text
Client
   ↓
API Gateway
   │
   ├── /orders/**
   │       ↓
   │   Order Service
   │
   └── /products/**
           ↓
       Product Service
```

서비스 간 내부 처리는 Kafka 이벤트를 사용합니다.

```text
Client
   ↓
API Gateway
   ↓
Order Service
   ↓
order-created
   ↓
Kafka
   ↓
Product Service
   ↓
재고 처리
   ↓
inventory-decreased
또는
inventory-decrease-failed
   ↓
Kafka
   ↓
Order Service
   ↓
주문 상태 변경
```

---

## 서비스 구성

### API Gateway

외부 요청을 각 서비스로 전달하는 진입점 역할을 합니다.

```text
/orders/**
→ Order Service

/products/**
→ Product Service
```

현재는 기본적인 라우팅을 적용했고 이후 인증/인가와 공통 처리를 추가할 예정입니다.

---

### Order Service

주문 생성과 주문 상태를 관리합니다.

주문 생성 시 DB에 주문을 저장한 뒤 Kafka를 통해 Product Service에 재고 처리를 요청합니다.

주요 학습 내용:

* Hexagonal Architecture
* 주문 생성 및 상태 관리
* Kafka Producer / Consumer
* `Idempotency-Key`를 이용한 API 중복 요청 방지
* Product Service의 재고 처리 결과 반영

자세한 내용은 `order-service/README.md`에 정리했습니다.

---

### Product Service

상품 정보와 재고를 관리합니다.

Order Service에서 전달받은 주문 이벤트를 기준으로 재고를 차감하고 처리 결과를 다시 Kafka로 전달합니다.

주요 학습 내용:

* 재고 동시성 문제
* 조건부 UPDATE
* 동시성 테스트
* Kafka Producer / Consumer
* `eventId`를 이용한 Kafka 이벤트 중복 처리 방지

자세한 내용은 `product-service/README.md`에 정리했습니다.

---

## API Gateway

현재 Gateway에서는 요청 Path를 기준으로 각 서비스에 라우팅합니다.

```yaml
spring:
  cloud:
    gateway:
      server:
        webmvc:
          routes:
            - id: order-service
              uri: http://localhost:8081
              predicates:
                - Path=/orders/**

            - id: product-service
              uri: http://localhost:8082
              predicates:
                - Path=/products/**
```

클라이언트에서는 각 서비스의 Port를 직접 알 필요 없이 Gateway를 통해 요청하도록 구성했습니다.

```text
Client

POST /orders
GET /products/1

        ↓

API Gateway

        ↓

각 Service로 전달
```

---

## 주문 처리 흐름

### 1. 주문 요청

```text
Client
   ↓
API Gateway
   ↓
Order Service
   ↓
Order 저장
status = CREATED
```

### 2. 재고 처리 요청

```text
Order Service
   ↓
OrderCreatedEvent
   ↓
Kafka
   ↓
Product Service
```

### 3. 재고 차감

Product Service에서는 재고가 충분한 경우에만 조건부 UPDATE를 실행합니다.

```sql
UPDATE products
SET stocks = stocks - :quantity
WHERE id = :productId
  AND stocks >= :quantity
```

### 4. 처리 결과 반환

```text
재고 성공
→ inventory-decreased

재고 실패
→ inventory-decrease-failed
```

### 5. 주문 상태 변경

```text
inventory-decreased
→ COMPLETED

inventory-decrease-failed
→ FAILED
```

---

## 멱등성

프로젝트를 진행하면서 API와 Kafka에서 각각 발생할 수 있는 중복 처리 문제도 구현했습니다.

### 주문 API

사용자의 중복 클릭이나 네트워크 재시도로 같은 주문 요청이 여러 번 들어올 수 있습니다.

```text
Idempotency-Key
```

를 이용해 동일한 요청으로 주문이 여러 개 생성되지 않도록 처리했습니다.

### Kafka Consumer

Consumer가 메시지를 처리한 후 Offset Commit 전에 장애가 발생하면 동일한 Kafka 메시지를 다시 소비할 수 있습니다.

각 Kafka 이벤트에:

```text
eventId
```

를 추가하고 Product Service에서 처리한 이벤트를 저장하여 재고가 중복 차감되지 않도록 했습니다.

```text
API 요청 중복
→ Idempotency-Key

Kafka 이벤트 중복
→ eventId
```

---

## 현재 진행 상태

현재 프로젝트는 계속 기능을 추가하며 학습 중입니다.

* [x] Order Service 구현
* [x] Product Service 구현
* [x] API Gateway 추가
* [x] Kafka 기반 서비스 간 비동기 통신
* [x] 재고 동시성 문제 확인
* [x] 조건부 UPDATE 적용
* [x] 재고 동시성 테스트
* [x] 재고 성공 / 실패 이벤트 처리
* [x] 주문 상태 변경
* [x] 주문 API 멱등성 처리
* [x] Kafka Consumer 멱등성 처리
* [ ] Kafka Retry / DLT
* [ ] Transactional Outbox Pattern
* [ ] Payment Service
* [ ] 결제 실패 시 재고 복구
* [ ] 주문 취소 / 보상 처리
* [ ] 인증 / 인가
* [ ] 부하 테스트
