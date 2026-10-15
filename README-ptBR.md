# go-saga-engine

Leia em outros idiomas: [English](README.md)

Um motor de orquestração SAGA compacto e agnóstico de domínio, escrito em Go.

> **Estado do desenvolvimento:** os blocos 1 a 4 estão concluídos: DAG
> determinístico, waves paralelas isoladas, retries persistidos, concorrência
> otimista, leases renováveis, outbox transacional, testes de resiliência e CI
> fortalecido. O repositório está preparado para a versão `v0.2.0`. Acompanhe o
> [plano de atualização](ROADMAP.md) e o [changelog](CHANGELOG.md).

O motor coordena workflows transacionais distribuídos sem conhecer suas regras
de negócio. Casos típicos incluem faturas, liquidações, chargebacks e fluxos de
onboarding.

Go 1.27.0 é a versão mínima suportada. O CI verifica continuamente a versão
patch mais recente do Go 1.27.

## O que o motor fornece

- execução por DAG validado;
- ordenação topológica estável;
- execução isolada de stages paralelos;
- merge determinístico do payload;
- cancelamento por contexto e timeout opcional por stage;
- compensação em ordem reversa;
- histórico persistido da execução;
- contratos plugáveis para repositório, retry e event bus.

Ele é um orquestrador, não um motor BPMN, scheduler, cron, message broker ou
framework de domínio.

## Garantias e limitações atuais

| Capacidade | Estado |
| ---------- | ------ |
| Validação do DAG e ordem topológica estável | Implementado |
| Paralelismo isolado e merge determinístico | Implementado |
| Compensação reversa após encerrar a wave | Implementado |
| Cópia profunda no repositório em memória | Implementado |
| Limite, backoff, jitter e atraso máximo de retry | Implementado |
| Lease renovável e concorrência otimista | Implementado |
| Outbox transacional e recuperação de sagas paradas | Implementado |
| Suíte de resiliência e race gate no CI | Implementado |

A entrega é at-least-once. Consumidores devem deduplicar por `EventID`, e os
stages continuam responsáveis por tornar idempotentes seus efeitos externos.

## Conceitos centrais

### Estado da saga

`state.State` é a fonte de verdade persistida:

```go
type State struct {
    SagaID          string
    Name            string
    Status          Status
    CurrentStage    string
    Version         uint64
    Payload         map[string]any

    ExecutedStages  map[string]bool
    CompletedStages []string
    Attempts        map[string]int
    History         []HistoryEntry

    Retry           *RetryInfo
    Error           *ErrorInfo
    CreatedAt       time.Time
    UpdatedAt       time.Time
}
```

- `Payload` contém os dados de negócio trocados pelos stages.
- `ExecutedStages` rastreia os stages confirmados pelo motor.
- `CompletedStages` define a ordem reversa determinística de compensação.
- `Attempts` e `Retry` persistem o agendamento de retry por stage.
- `Version` é usada no compare-and-swap do repositório.
- `History` registra execução, falha e compensação.

### Stages

```go
type Stage interface {
    Name() string
    Execute(ctx context.Context, saga *state.State) error
    Compensate(ctx context.Context, saga *state.State) error
}

type TimedStage interface {
    Stage
    Timeout() time.Duration
}
```

`Execute` e `Compensate` devem ser idempotentes em relação a seus efeitos
externos. Em uma wave paralela, cada stage recebe uma cópia isolada do estado.
O motor incorpora apenas mudanças no payload; alterações nos metadados do ciclo
de vida são ignoradas.

Payloads devem ser acíclicos e usar mapas, slices, ponteiros ou valores
imutáveis. Valores opacos com estado mutável privado são copiados por valor e
devem ser tratados como imutáveis pelos stages paralelos.

Stages paralelos podem alterar chaves diferentes ou escrever o mesmo valor em
uma chave compartilhada. Valores diferentes para a mesma chave produzem
`ErrPayloadConflict` e falham a saga, evitando resultado não determinístico.

### Nodes do DAG

```go
type Node struct {
    Stage     stage.Stage
    DependsOn []string
    Parallel  bool
}
```

O construtor rejeita:

- grafo vazio;
- stage nil ou com nome vazio;
- nomes de stage duplicados;
- dependências ausentes ou repetidas;
- ciclos de dependência.

Nodes prontos seguem uma ordem topológica estável derivada da ordem declarada.
Um node não paralelo executa sozinho. Nodes prontos e consecutivos marcados
como `Parallel` formam uma wave.

## Criando e executando o motor

O construtor valida a configuração e o DAG completo:

```go
eng, err := engine.New(
    "invoice-saga",
    repo,
    bus,
    retryPolicy,
    logger,
    nodes...,
)
if err != nil {
    return err
}

saga, err := eng.Start(ctx, map[string]any{"invoice_id": "INV-2026-01"})
if err != nil {
    return err
}

if err := eng.HandleNext(ctx, saga.SagaID); err != nil {
    return err
}
```

`New` agora retorna `(*engine.Engine, error)` e `HandleNext` retorna erros
operacionais ou terminais de execução. Usuários existentes devem seguir o
[guia de migração para `v0.2.0`](MIGRATION.md).

## Semântica de execução

Em cada wave, o motor:

1. encontra os nodes prontos em ordem topológica estável;
2. registra `EXECUTING` nessa mesma ordem;
3. entrega uma cópia isolada do estado a cada stage;
4. cancela o contexto da wave quando um stage falha;
5. aguarda o retorno de todos os stages iniciados;
6. confirma os stages bem-sucedidos e mescla o payload deterministicamente;
7. persiste o estado resultante antes de continuar.

Um stage paralelo que termina com sucesso antes da falha de outro é confirmado
e incluído na compensação. Assim, seu efeito externo não fica sem rastreamento.

## Semântica de retry durável

```go
type Policy interface {
    ShouldRetry(err error, saga *state.State) bool
    NextDelay(attempt int) time.Duration
}
```

Em uma falha, o motor incrementa a tentativa do stage antes de consultar a
política. A política exponencial aplica `MaxAttempts`, dobra a partir de
`BaseDelay`, limita em `MaxDelay` e pode adicionar jitter. O `RetryInfo` e o
evento de retry na outbox são confirmados atomicamente antes da publicação.

Eventos de retry carregam `Attempt` e `NotBefore`. O consumidor não deve chamar
o motor antes de `NotBefore`; uma chamada antecipada retorna
`ErrRetryNotReady` sem executar nenhum stage.

## Concorrência e recuperação de crashes

`HandleNext` adquire um lease renovável para o `SagaID`. Outro worker recebe
`repository.ErrLeaseHeld` e não inicia stages para a mesma saga. Cada save usa
`Version` como condição de compare-and-swap e retorna
`repository.ErrVersionConflict` para estado antigo.

`RecoverStalled(ctx, updatedBefore, limit)` cria eventos `Next` duráveis para
sagas `RUNNING` cujo lease expirou e que não foram atualizadas após o cutoff.
Stages ainda precisam de efeitos externos idempotentes porque o processo pode
cair depois do efeito externo e antes de salvar seu resultado.

A aplicação deve chamar `RecoverStalled` periodicamente a partir de um job de
manutenção agendado; ele intencionalmente não é uma goroutine interna.

## Semântica de compensação

Em uma falha terminal:

- todos os stages da wave atual param antes da compensação;
- os stages confirmados são compensados na ordem reversa de `CompletedStages`;
- a compensação continua sendo best-effort;
- sucesso registra `COMPENSATED`;
- falha registra `COMPENSATION_FAILED` e seu erro é retornado;
- a saga termina como `FAILED` e emite `event.KindDeadLetter`.

## Semântica de cancelamento

Cancelamento em Go é cooperativo, não preemptivo. Quando um stage falha, os
demais recebem `ctx.Done()`, mas ainda podem terminar. Nenhuma nova wave começa
depois da falha.

Stages devem observar o contexto regularmente e não iniciar trabalho
irreversível depois do cancelamento.

## Integração com EventBus

Eventos são persistidos como `event.Envelope`, contendo `EventID`, `Kind`,
`SagaID`, stage, tentativa e timestamps. O motor emite:

- `event.KindNext` quando a saga é criada;
- `event.KindRetry` quando a política aceita uma falha;
- `event.KindDeadLetter` após persistir uma falha terminal.

```go
type EventBus interface {
    Publish(ctx context.Context, envelope event.Envelope) error
}
```

Alterações de estado e envelopes novos são confirmados atomicamente pelo
repositório. `DispatchPending` publica a outbox por um `EventBus` com contexto e
só então marca o evento como publicado. Uma queda entre essas operações pode
duplicar a entrega; consumidores devem deduplicar por `EventID`.

RabbitMQ, Kafka, NATS ou uma implementação em memória podem fornecer o contrato
pequeno de `EventBus`.

## Exemplos

- [`examples/simple`](examples/simple) demonstra a API básica do motor e um
  stage com retry.
- [`examples/order-fulfillment`](examples/order-fulfillment) simula validação
  do pedido, antifraude e estoque em paralelo, pagamento, retry da entrega,
  notificação do cliente e compensação reversa após rejeição do pagamento.

## Verificação

O bloco atual é verificado com:

```text
go test ./...
go test -race -count=20 ./...
go vet ./...
golangci-lint run ./...
govulncheck ./...
```

Os testes cobrem grafos inválidos, ordem estável, merge isolado de payload,
escritas conflitantes, compensação reversa, cópias do repositório, limites e
temporização de retry, recuperação da outbox, leases, conflitos de versão,
timeout, cancelamento, restart de waves persistidas e recuperação de sagas
paradas. O CI repete a suíte com o detector de corrida, e o validador do DAG
também possui um alvo de fuzzing.

## Versionamento

O projeto segue Versionamento Semântico. Antes da `v1.0.0`, mudanças
incompatíveis na API pública incrementam a versão minor. A partir da `v1.0.0`,
mudanças incompatíveis incrementam a versão major; funcionalidades compatíveis
incrementam minor e correções incrementam patch. Releases e instruções de
atualização são registradas no [changelog](CHANGELOG.md).

## Licença

Distribuído sob a Licença MIT. Veja [LICENSE](LICENSE).

## Autor

2026, Thiago Zilli Sarmento :heart:
