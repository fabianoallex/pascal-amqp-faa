# Development TLS broker certificate

`docker-compose.tls.yml` expects `server.crt` and `server.key` in this directory.
They are **not versioned** (a private key does not go into the repository, even
a dev one). Generate a local self-signed pair with:

```
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout server.key -out server.crt \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost"
```

Since the certificate is self-signed, use `TlsVerifyPeer := False` on the client
(which is what `TAMQPConnectionParams.LocalhostTls` already does).
