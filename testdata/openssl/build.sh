#!/bin/bash
set -e
set -u

password=123456

# common certificate signing request configuration
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

# certificate configuration for client and server
cat > cert.conf <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:NO
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = test.hazelcast.com
DNS.2 = www.test.hazelcast.com

EOF

# create root certificate authority
openssl genrsa -out rootCA.key 2048
openssl req -new -x509 -nodes -days 3650 \
   -config csr.conf \
   -extensions req_ext \
   -key rootCA.key \
   -out rootCA.crt

# creates server csr and certificate
mkdir -p server
cd server
openssl genrsa -out server.key 2048
openssl req -new -nodes -key server.key -out server.csr -config ../csr.conf
openssl x509 -req \
    -in server.csr \
    -CA ../rootCA.crt -CAkey ../rootCA.key \
    -CAcreateserial -out server.crt \
    -days 3650 \
    -sha256 -extfile ../cert.conf
cd ..

# reate client csr and certificate
mkdir -p client
cd client
openssl genrsa -out client.key 2048
openssl req -new -nodes -key client.key -out client.csr -config ../csr.conf
openssl x509 -req \
    -in client.csr \
    -CA ../rootCA.crt -CAkey ../rootCA.key \
    -CAcreateserial -out client.crt \
    -days 3650 \
    -sha256 -extfile ../cert.conf
cd ..

# create keystore and truststore for the server
openssl pkcs12 -export -name server-cert-store \
               -in server/server.crt -inkey server/server.key \
               -out serverkeystore.p12 \
               -password pass:$password

keytool -importkeystore -destkeystore server.keystore \
        -srckeystore serverkeystore.p12 -srcstoretype pkcs12 \
        -alias server-cert-store \
        -srcstorepass $password \
        -deststorepass $password

# add client certificate to server truststore
keytool -import -alias client-cert \
        -file client/client.crt -keystore server.truststore \
        -storepass $password \
        -noprompt

# add server certificate to server truststore
keytool -import -alias server-cert \
        -file server/server.crt -keystore server.truststore \
        -storepass $password \
        -noprompt

# delete unnecessary openssl files
rm rootCA.*
find . \( -name "*.conf" -o -name "*.p12" -o -name "*.csr" -o -name "*.srl" \)  -type f -delete