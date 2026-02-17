# go-saga-engine

Leia em outros idiomas: [English](README.md)

Um motor de orquestração SAGA robusto, determinístico e testável escrito em Go.

Este projeto fornece um motor SAGA genérico projetado para workflows transacionais distribuídos, com garantias fortes de:

- execução ordenada (DAG)
- paralelismo
- políticas de retry
- idempotência
- compensação
- cancelamento
- observabilidade (histórico / inspetor)

Ele é agnóstico de domínio e pode ser reutilizado para faturas, liquidações, chargebacks, fluxos de onboarding etc.

---

## O que este motor é (e o que não é)

### Este motor É

- Um orquestrador SAGA
- Um coordenador de transações distribuídas
- Um motor de workflow determinístico
- Um motor de execução baseado em DAG
- Seguro para retries, reinícios e falhas
- Projetado para sistemas fintech / backend em produção

### Este motor NÃO É

- Um motor BPMN
- Um agendador de jobs
- Um sistema de cron
- Um message broker
- Um framework de domínio

O motor não conhece sua lógica de negócio.  
Ele apenas garante as semânticas de execução.

---

## Conceitos Centrais

### 1. Saga

Uma Saga representa uma transação distribuída.

Cada saga tem:

- um SagaID único
- um ciclo de vida (PENDING -> RUNNING -> COMPLETED | FAILED)
- um State mutável compartilhado
- um histórico de execução

---

### 2. State

O state.State é a única fonte de verdade.

```go
type State struct {
    SagaID         string
    Name           string
    Status         Status
    Payload        map[string]any

    ExecutedStages map[string]bool
    History        []HistoryEntry

    Error          *ErrorInfo
    CreatedAt      time.Time
    UpdatedAt      time.Time
}
```

- Payload é o contrato compartilhado entre estágios
- ExecutedStages garante idempotência
- History permite inspeção, debug e auditoria

---

### 3. Stage

Um Stage é um passo da saga.

```go
type Stage interface {
    Name() string
    Execute(ctx context.Context, state *state.State) error
    Compensate(ctx context.Context, state *state.State) error
}
```

Suporte opcional a timeout:

```go
type TimedStage interface {
    Stage
    Timeout() time.Duration
}
```

Regras:

- Execute deve ser idempotente
- Compensate deve ser best-effort
- O motor nunca inspeciona o payload

---

### 4. DAG (Grafo Acíclico Direcionado)

A ordem de execução é definida usando Nodes:

```go
type Node struct {
    Stage     stage.Stage
    DependsOn []string
    Parallel  bool
}
```

Exemplo:

``` text
A
|-- B (parallel)
`-- C (parallel)
    |
    `-- D
```

As dependências são estritamente aplicadas.

---

## Semântica de Retry

O comportamento de retry é totalmente plugável:

```go
type Policy interface {
    ShouldRetry(err error, saga *state.State) bool
    NextDelay(attempt int) time.Duration
}
```

- Apenas erros retryable devem ser reexecutados
- As tentativas de retry são rastreadas por estágio
- O retry é disparado via evento (event.Retry)

---

## Semântica de Compensação

A compensação é executada quando ocorre uma falha fatal:

- Apenas estágios marcados como ExecutedStages == true são compensados
- A compensação roda em ordem lógica reversa
- A compensação é best-effort, não transacional

---

## Semântica de Cancelamento (IMPORTANTE)

Este motor usa cancelamento de contexto para parar a execução.

### O que é garantido

- Nenhum novo estágio iniciará após uma falha fatal
- Todos os estágios em execução recebem ctx.Done()
- A saga termina em estado FAILED
- A compensação é executada
- O evento DeadLetter é emitido

### O que NÃO é garantido (por design)

- Um estágio paralelo que já iniciou pode rodar brevemente
- O cancelamento não é preemptivo em Go

Regra chave:  
Um estágio pode iniciar, mas não deve ser marcado como EXECUTED se ocorrer uma falha fatal.

---

## Inspetor de Saga (Histórico)

Todo evento significativo é registrado:

``` text
EXECUTING
EXECUTED
FAILED
COMPENSATED
COMPLETED
```

Exemplo de histórico:

``` text
2026-02-01T17:18:29 | identify-client        | EXECUTED
2026-02-01T17:18:30 | generate-bank-invoice | FAILED
2026-02-01T17:18:30 | identify-client        | COMPENSATED
2026-02-01T17:18:31 | SAGA                   | FAILED
```

Isso permite:

- debug
- auditoria
- inspetores de UI
- ferramentas de suporte

---

## Filosofia de Testes

O motor foi projetado para ser totalmente testável.

Ferramentas fornecidas:

- repository/memory para testes rápidos
- EventBus plugável
- execução determinística
- design seguro para concorrência

Regra importante de teste:

Não teste:

``` text
"stage X should never start"
```

Em vez disso, teste:

``` text
"stage X must not be marked as EXECUTED"
```

Isso se alinha com garantias reais de concorrência.

---

## Integração com Event Bus

O motor é orientado a eventos.

Eventos obrigatórios:

- event.Next
- event.Retry
- event.DeadLetter

Você pode plugar:

- bus in-memory
- RabbitMQ
- Kafka
- NATS

O motor não depende de um broker específico.

---

## Exemplos de Uso

- Geração de faturas
- Fechamento financeiro mensal
- Pipelines de liquidação
- Workflows de onboarding
- Ciclos de chargeback
- Fluxos de minting / burning de tokens

---

## Garantias de Design

| Recurso            | Garantia |
| ------------------ | -------- |
| Determinismo       | SIM      |
| Idempotência       | SIM      |
| Segurança de retry | SIM      |
| Execução paralela  | SIM      |
| Cancelamento       | SIM      |
| Compensação        | SIM      |
| Observabilidade    | SIM      |
| Testabilidade      | SIM      |

---

## Nota Final

Este motor é intencionalmente pequeno, explícito e rigoroso.

A complexidade vive no seu domínio,  
não na camada de orquestração.

Se você entende este README, você entende o motor.

## Licença

Este projeto é distribuído sob a Licença MIT. Veja o arquivo [LICENSE](LICENSE) para detalhes.

## Autor

2026, Thiago Zilli Sarmento :heart:
