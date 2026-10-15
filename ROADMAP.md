# Plano de atualização

Este roadmap acompanha a evolução do motor sem apresentar como prontas
garantias que ainda dependem de implementação. Cada bloco deve terminar com
documentação, testes funcionais, `go vet` e `go test -race` atualizados.

## Bloco 1 — execução determinística e segura — concluído

- [x] validar nome, dependências obrigatórias e componentes do motor;
- [x] rejeitar DAG vazio, ciclos, dependências ausentes ou repetidas e stages duplicados;
- [x] gerar uma ordem topológica estável a partir da ordem declarada dos nodes;
- [x] executar stages paralelos sobre cópias isoladas do estado;
- [x] mesclar mudanças do payload de forma determinística;
- [x] rejeitar mudanças paralelas conflitantes na mesma chave do payload;
- [x] registrar a ordem de confirmação em `CompletedStages`;
- [x] aguardar toda a wave encerrar antes de iniciar compensações;
- [x] compensar na ordem reversa de confirmação e registrar falhas de compensação;
- [x] fazer cópia profunda no repositório em memória;
- [x] propagar erros de leitura e persistência por `HandleNext`;
- [x] manter README, exemplo e testes alinhados ao novo contrato.

## Bloco 2 — retry e durabilidade — concluído

- [x] persistir tentativas por stage;
- [x] aplicar limite de tentativas, exponential backoff, jitter e atraso máximo;
- [x] tornar a transição para retry durável antes de publicar o evento;
- [x] adicionar versão do estado e update com compare-and-swap;
- [x] criar lease/claim renovável para impedir dois workers no mesmo `SagaID`;
- [x] implementar outbox atômica e contrato de deduplicação por `EventID`;
- [x] adicionar contexto e retorno de erro ao EventBus;
- [x] recuperar sagas `RUNNING` abandonadas com `RecoverStalled`.

## Bloco 3 — testes de resiliência — concluído

- [x] substituir esperas temporais dos testes antigos por sincronização determinística;
- [x] cobrir timeout, cancelamento e falhas de persistência/broker;
- [x] testar duas chamadas concorrentes a `HandleNext`;
- [x] testar reinício antes e depois da persistência de uma wave e durante retry;
- [x] cobrir limite e temporização de retry;
- [x] adicionar fuzzing do validador de DAG;
- [x] executar testes repetidos e race detector no CI.

## Bloco 4 — toolchain, CI e publicação — concluído

- [x] definir Go 1.27.0 como versão mínima suportada;
- [x] atualizar GitHub Actions e restringir permissões;
- [x] adicionar formatação, vet, lint, cobertura e `govulncheck` ao CI;
- [x] configurar atualização automática e revisão de dependências;
- [x] documentar migração da API e política de versionamento semântico;
- [x] preparar uma versão `v0.2.0` depois dos blocos 2 e 3.

## Critérios antes de produção

O motor possui coordenação por lease, concorrência otimista, retries persistidos,
outbox at-least-once e uma suíte de resiliência executada com o detector de
corrida. Consumidores precisam deduplicar eventos por `EventID`, e os stages
continuam responsáveis pela idempotência dos próprios efeitos externos. A
publicação de `v0.2.0` depende apenas da revisão das mudanças, execução do CI no
GitHub e criação da tag de release.
