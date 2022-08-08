#!/bin/bash
set -e
set -u
openssl req -x509 \
            -sha256 -days 356 \
            -nodes \
            -newkey rsa:2048 \
            -subj "/CN=test.hazelcast.com/C=US/L=San Fransisco" \
            -keyout rootCA.key -out rootCA.crt

cat > csr.conf <<EOF
[ req ]
default_bits = 2048
prompt = no
default_md = sha256
req_extensions = req_ext
distinguished_name = dn

[ dn ]
C = US
ST = California
L = San Fransisco
O = Hazelcast Test
OU = Hazelcast Go Client Test
CN = test.hazelcast.com

[ req_ext ]
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = test.hazelcast.com
DNS.2 = www.test.hazelcast.com

EOF

cat > cert.conf <<EOF

authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = test.hazelcast.com

EOF

mkdir server
cd server
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -config ../csr.conf
openssl x509 -req \
    -in server.csr \
    -CA ../rootCA.crt -CAkey ../rootCA.key \
    -CAcreateserial -out server.crt \
    -days 365 \
    -sha256 -extfile ../cert.conf
cd ..

mkdir client
cd client &&
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr -config ../csr.conf
openssl x509 -req \
    -in client.csr \
    -CA ../rootCA.crt -CAkey ../rootCA.key \
    -CAcreateserial -out client.crt \
    -days 365 \
    -sha256 -extfile ../cert.conf
cd .. &&

# Creates keystore and truststore for server
openssl pkcs12 -export -name server-cert \
               -in server/server.crt -inkey server/server.key \
               -out serverkeystore.p12
keytool -importkeystore -destkeystore server.keystore \
        -srckeystore serverkeystore.p12 -srcstoretype pkcs12 \
        -alias server-cert

# Creates keystore and truststore for client
openssl pkcs12 -export -name client-cert \
               -in client/client.crt -inkey client/client.key \
               -out clientkeystore.p12
keytool -importkeystore -destkeystore client.keystore \
        -srckeystore clientkeystore.p12 -srcstoretype pkcs12 \
        -alias client-cert

# Add client and server certificate to server truststore
keytool -import -alias client-cert \
        -file client/client.crt -keystore server.truststore
keytool -import -alias server-cert \
        -file server/server.crt -keystore server.truststore

# Add client and server certificate to client truststore
keytool -import -alias server-cert \
        -file server/server.crt -keystore client.truststore
keytool -import -alias client-cert \
        -file client/client.crt -keystore client.truststore
