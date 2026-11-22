#!/bin/sh
# Generates a throwaway capture CA and a leaf certificate for chatgpt.com,
# auth.openai.com and ab.chatgpt.com, valid 30 days. Never commit the output:
# anything that trusts this CA can be intercepted by whoever holds ca.key.
#   fidelity/tools/make-capture-ca.sh <out-dir>
set -eu
out=${1:?usage: make-capture-ca.sh <out-dir>}
mkdir -p "$out"
cd "$out"
openssl ecparam -name prime256v1 -genkey -noout -out ca.key
printf 'basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\nsubjectKeyIdentifier=hash\n' > ca.ext
openssl req -new -key ca.key -subj "/CN=kin fidelity capture CA" -out ca.csr
openssl x509 -req -in ca.csr -signkey ca.key -days 30 -sha256 -extfile ca.ext -out ca.pem
openssl ecparam -name prime256v1 -genkey -noout -out leaf.key
printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:chatgpt.com,DNS:auth.openai.com,DNS:ab.chatgpt.com\nauthorityKeyIdentifier=keyid\n' > leaf.ext
openssl req -new -key leaf.key -subj "/CN=chatgpt.com" -out leaf.csr
openssl x509 -req -in leaf.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 30 -sha256 -extfile leaf.ext -out leaf.pem
openssl verify -CAfile ca.pem leaf.pem
chmod 600 ca.key leaf.key
