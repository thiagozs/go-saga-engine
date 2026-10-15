# Exemplo de fulfillment de pedido

Este exemplo representa um fluxo pequeno, mas próximo de um processamento real
de pedidos:

```text
validate order
      |
      +-- fraud check --------+
      |                       |
      +-- reserve inventory --+
                              |
                       charge payment
                              |
                       create shipment
                              |
                       notify customer
```

A análise antifraude e a reserva de estoque executam em paralelo. A
transportadora simulada falha na primeira chamada, então o motor persiste e
agenda um retry antes de concluir a entrega.

Execute o cenário de sucesso:

```bash
go run ./examples/order-fulfillment
```

Execute uma rejeição fatal do pagamento para observar a compensação reversa e a
liberação do estoque:

```bash
go run ./examples/order-fulfillment -reject-payment
```

Os serviços externos simulados usam `order_id` como chave de idempotência. Isso
é necessário porque um processo real pode parar depois de produzir um efeito
externo e antes de o motor persistir o resultado do stage.
