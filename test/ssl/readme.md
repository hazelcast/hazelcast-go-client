The pem files were taken from Java Client.

https://github.com/hazelcast/hazelcast/tree/master/hazelcast/src/test/resources/com/hazelcast/nio/ssl-mutual-auth


To generate cert.pem and privKey.pem:


To generate a private key:
```
openssl genrsa -out key.pem 1024
```

To encrypt the private key:

```
openssl rsa -in key.pem -des3 -out privKey.pem
```


Then enter a password when prompted.

To generate a self signed certificate:

```
openssl req 
-key privKey.pem 
-new 
-x509 -days 3650 -out cert.pem
```


