# Posto Automação

Base para uma aplicação real: um **servidor de automação de bombas de
combustível** com o broker AMQP 0-9-1 desta lib **embutido**, e **PDVs** na
rede da loja que enxergam as abastecidas e as lançam em vendas.

O desenho completo (estados, contrato de mensagens, regras de falha) está em
[`ARQUITETURA.md`](ARQUITETURA.md). Este README é só build + roteiro.

## O que ele demonstra

- **Broker embutido** numa app Delphi/FPC (`TAMQPServer`), com autenticador
  próprio (`automacao` + `pdv-*`).
- **Pub/sub** (exchange topic `posto.eventos`) para difundir cada mudança de
  estado a todos os PDVs + **RPC** (fila `posto.comandos`, `reply-to` +
  `correlation-id`) para os comandos.
- **Estado autoritativo no servidor** — o broker é transporte, não banco. Uma
  abastecida pertence a **uma venda só**; sai da reserva só por estorno,
  finalização ou liberação manual do supervisor.
- **Late-join** por snapshot + replay de eventos bufferizados, com detecção de
  salto de versão → re-snapshot.
- **Operação em contingência**: o PDV tem outbox local, opera offline e
  reconcilia na reconexão; conflito de venda dupla é **detectado e sinalizado**,
  nunca resolvido em silêncio.
- **Observabilidade da Fase 4.1** (`IAMQPEventSink`) alimentando o painel do
  supervisor com a presença de cada PDV — **sem** ação automática.

## Build

FPC / Lazarus (`lazbuild`):

```
lazbuild samples\PostoAutomacao\servidor\ServidorPosto.lpi
lazbuild samples\PostoAutomacao\pdv\PdvPosto.lpi
lazbuild samples\PostoAutomacao\smoke\SmokePosto.lpi
```

Delphi: abrir `ServidorPosto.dproj`, `PdvPosto.dproj` e `SmokePosto.dproj` no
IDE — ou o grupo `AMQP.groupproj` (a Community Edition não compila por linha
de comando). Os três compilam do mesmo fonte que a versão FPC.

> O `.res` (ícone padrão) é gerado pelo IDE/`lazbuild` e fica fora do git,
> como nos demais samples GUI.

## Smoke test

`samples\PostoAutomacao\smoke\SmokePosto` — console, mesmo fonte para FPC e
Delphi (como o `samples/SmokeTest`). Sobe a automação in-process (sem GUI, sem
bombas), conecta dois PDVs e exercita snapshot → lançar → recusa por
concorrência → finalizar → estornar → liberar forçado. Sai com código 0 se
tudo passou.

## Roteiro manual (as duas GUIs)

1. **Servidor**: rode `ServidorPosto`, ajuste a porta (default 5680), clique
   **Iniciar**. O simulador de bombas começa a gerar abastecidas.
2. **PDV 1**: rode `PdvPosto`, PDV = `pdv-01`, senha `pdv`, porta igual à do
   servidor, **Conectar**. A lista da esquerda enche com as abastecidas
   pendentes.
3. **PDV 2**: outra instância, PDV = `pdv-02`, conecta. Vê a **mesma** lista.
4. No PDV 1, selecione uma abastecida e **Adicionar à venda** → ela some da
   lista do PDV 2 (ficou `lancando`).
5. No PDV 1, **Remover item** → volta a aparecer nos dois.
6. No PDV 1, adicione de novo e **Finalizar venda** → some de vez.
7. Suba um **PDV 3** agora → recebe as pendentes por snapshot.
8. Feche o PDV 2 com uma venda aberta → o painel do servidor mostra
   `pdv-02 DESCONECTADO`, e a abastecida **continua reservada**. Selecione-a
   no painel e **Liberar forçado** → volta para os demais.
9. Contingência: no servidor, **Parar**; PDV 1 e PDV 2 lançam a **mesma**
   abastecida (cada um da sua lista congelada) e finalizam; **Iniciar** o
   servidor de novo → o primeiro outbox a chegar ganha, o outro marca o item
   como **CONFLITO**.
