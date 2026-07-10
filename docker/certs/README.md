# Certificado do broker TLS de desenvolvimento

O `docker-compose.tls.yml` espera `server.crt` e `server.key` neste diretório.
Eles **não são versionados** (chave privada não vai para o repositório, mesmo
sendo de dev). Gere um par self-signed local com:

```
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout server.key -out server.crt \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost"
```

Como o certificado é self-signed, use `TlsVerifyPeer := False` no cliente
(é o que `TAMQPConnectionParams.LocalhostTls` já faz).
