# Migration to v0.2.0 / Migração para v0.2.0

## English

`v0.2.0` changes the public engine, repository and event bus contracts. Upgrade
all implementations together and run the full test suite before deploying.

1. Update engine construction and handle the validation error:

   ```go
   eng, err := engine.New(name, repo, bus, retryPolicy, logger, nodes...)
   if err != nil {
       return err
   }
   ```

2. Update `EventBus.Publish` to accept `context.Context` and `event.Envelope`
   and return an error. Consumers must deduplicate deliveries by `EventID`.
3. Implement the expanded `repository.Repository` contract. State writes must
   enforce `Version`, preserve the lease owner and append outbox events in the
   same transaction.
4. Handle errors returned by `HandleNext`, `DispatchPending` and
   `RecoverStalled`. Schedule `RecoverStalled` from one maintenance job.
5. Make `Stage.Execute` and `Stage.Compensate` idempotent. A process can stop
   after an external effect and before the stage result is persisted.
6. Compile and test with Go 1.27.0 or a newer Go 1.27 patch release.

Before deploying, migrate persistent state by initializing missing fields:
`Version`, `Attempts`, `CompletedStages` and `Retry`. Existing repository
implementations should perform this migration atomically according to their
database transaction model.

## Português (Brasil)

A `v0.2.0` altera os contratos públicos do motor, repositório e event bus.
Atualize todas as implementações em conjunto e execute a suíte completa antes
do deploy.

1. Atualize a construção do motor e trate o erro de validação:

   ```go
   eng, err := engine.New(name, repo, bus, retryPolicy, logger, nodes...)
   if err != nil {
       return err
   }
   ```

2. Atualize `EventBus.Publish` para receber `context.Context` e
   `event.Envelope` e retornar erro. Consumidores devem deduplicar entregas pelo
   `EventID`.
3. Implemente o contrato ampliado de `repository.Repository`. A persistência
   deve validar `Version`, preservar o dono do lease e incluir eventos da
   outbox na mesma transação.
4. Trate os erros de `HandleNext`, `DispatchPending` e `RecoverStalled`.
   Agende `RecoverStalled` em um único job de manutenção.
5. Torne `Stage.Execute` e `Stage.Compensate` idempotentes. O processo pode
   parar depois de um efeito externo e antes de persistir o resultado do stage.
6. Compile e teste com Go 1.27.0 ou uma versão patch mais recente do Go 1.27.

Antes do deploy, migre o estado persistido inicializando os campos ausentes:
`Version`, `Attempts`, `CompletedStages` e `Retry`. Cada implementação de
repositório deve executar essa migração atomicamente conforme o modelo
transacional do banco utilizado.

