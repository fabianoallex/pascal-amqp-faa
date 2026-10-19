#!/bin/sh
# Suítes FPCUnit no Linux (FPC 3.2.2, Debian bookworm), num container Docker,
# com heaptrc. Critério de aceite, por configuração: 0 erros, 0 falhas e
# "0 unfreed memory blocks".
#
# Configurações (as mesmas das validações Linux registradas no worklog):
#   unit | server, server-openssl | integration, integration-openssl |
#   acceptance e acceptance-durable (as duas passagens), em Default e openssl.
#
# Antes de compilar, confere os espelhos gerados (tools/gen_fpc_mirror.py
# --check) e o verificador do que o gerador não cobre
# (tests/tools/verifica_espelhos.py).
#
# A integração precisa de um RabbitMQ: o container roda na rede do
# RABBIT_CONTAINER (padrão: delphi-amqp-faa-rabbitmq, do docker compose do
# delphi-amqp-faa; os certs de TLS são os de docker/certs). Sem ele, passe
# SUITES sem integration (ex.: SUITES="unit server acceptance").
#
# O repositório é montado só para leitura e copiado dentro do container: nada
# é escrito na árvore de trabalho. Sem Lazarus: os runners FPCUnit só puxam a
# LCL no Windows.
#
# FPC_IMAGE: imagem com FPC 3.2.2 e libssl3 (padrão: pamqp-fpc322, construída
# aqui a partir do pacote fpc do bookworm se não existir).
# Heaptrc: o FPC do Debian só imprime o resumo com HEAPTRC=log=<arquivo>.
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${FPC_IMAGE:-pamqp-fpc322}"
RABBIT="${RABBIT_CONTAINER:-delphi-amqp-faa-rabbitmq}"
SUITES="${SUITES:-unit server integration acceptance}"
MOUNT="$ROOT"
command -v cygpath >/dev/null 2>&1 && MOUNT="$(cygpath -w "$ROOT")"

cd "$ROOT"
python tools/gen_fpc_mirror.py --check --suite tests/Unit --suite tests/Integration --suite tests/Server
python tests/tools/verifica_espelhos.py > /dev/null || { python tests/tools/verifica_espelhos.py; exit 1; }
[ -f external/pascal-common-faa/src/PascalCommon.Threading.pas ] \
  || { echo "external/pascal-common-faa vazio: git submodule update --init"; exit 1; }

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  docker build -q -t "$IMAGE" - <<'EOF' >/dev/null
FROM debian:bookworm
RUN apt-get update && apt-get install -y --no-install-recommends fpc libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
EOF
fi

NET=""
case " $SUITES " in
  *" integration "*) NET="--network container:$RABBIT" ;;
esac

MSYS_NO_PATHCONV=1 docker run --rm $NET -e SUITES="$SUITES" -v "$MOUNT:/src:ro" "$IMAGE" sh -c '
  set -e
  mkdir -p /t/external /t/bin /t/log
  cp -r /src/src /src/tests /src/docker /t/
  cp -r /src/external/pascal-common-faa /t/external/
  C=/t/external/pascal-common-faa/src
  # build <nome> <lpr> [defines]
  build() {
    mkdir -p /t/u/$1
    fpc -v0 $3 -Mdelphi -Fu/t/src -Fu/t/src/server -Fu$C -Fi/t/src -Fi$C -Fu/t/tests \
      -Fu$(dirname $2) -Fu/t/tests/Integration/fpc -FU/t/u/$1 -gh -gl -o/t/bin/$1 $2 \
      > /t/log/build-$1.log 2>&1 || { grep -iE "error|fatal" /t/log/build-$1.log | head -30; exit 1; }
  }
  FALHOU=0
  # run <nome> <binario> [VAR=valor ...]
  run() {
    N=$1; B=$2; shift 2
    (cd /t/tests && env "$@" HEAPTRC="log=/t/log/heap-$N.txt" /t/bin/$B --all --format=plain > /t/log/run-$N.log 2>&1) || true
    printf "%-22s %s | %s | %s | %s\n" "$N" \
      "$(grep -E "^Number of run tests" /t/log/run-$N.log | grep -oE "[0-9]+$")" \
      "$(grep -E "^Number of errors" /t/log/run-$N.log)" \
      "$(grep -E "^Number of failures" /t/log/run-$N.log)" \
      "$(grep -h "unfreed" /t/log/heap-$N.txt 2>/dev/null | head -1)"
    if ! grep -qE "^Number of errors: +0$" /t/log/run-$N.log \
       || ! grep -qE "^Number of failures: +0$" /t/log/run-$N.log \
       || ! grep -qE "^0 unfreed memory blocks" /t/log/heap-$N.txt; then
      FALHOU=1
      grep -A6 -E "^ +(Error|Failure)|Message:" /t/log/run-$N.log | head -40 || true
    fi
  }
  # O AMQP_TEST_HOST da integração: o RabbitMQ, na mesma rede do container.
  for S in $SUITES; do
    case $S in
      unit)
        build unit /t/tests/Unit/fpc/AMQPUnitTestsFpc.lpr
        run unit unit ;;
      server)
        build server /t/tests/Server/fpc/AMQPServerTestsFpc.lpr
        build server-openssl /t/tests/Server/fpc/AMQPServerTestsFpc.lpr -dAMQP_OPENSSL
        run server server
        run server-openssl server-openssl ;;
      integration)
        build integration /t/tests/Integration/fpc/AMQPIntegrationTestsFpc.lpr
        build integration-openssl /t/tests/Integration/fpc/AMQPIntegrationTestsFpc.lpr -dAMQP_OPENSSL
        run integration integration AMQP_TEST_HOST=127.0.0.1
        run integration-openssl integration-openssl AMQP_TEST_HOST=127.0.0.1 ;;
      acceptance)
        build acceptance /t/tests/Acceptance/fpc/AMQPAcceptanceFpc.lpr
        build acceptance-openssl /t/tests/Acceptance/fpc/AMQPAcceptanceFpc.lpr -dAMQP_OPENSSL
        run acceptance acceptance
        run acceptance-durable acceptance AMQP_ACEITACAO_DURAVEL=1
        run acceptance-openssl acceptance-openssl
        run acceptance-openssl-durable acceptance-openssl AMQP_ACEITACAO_DURAVEL=1 ;;
    esac
  done
  exit $FALHOU'
