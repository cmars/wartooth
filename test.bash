#!/bin/bash

set -e

if [ -z "$ADDR" ]; then
	echo "ADDR not set"
	exit 1
fi

PORT=$(echo ${ADDR} | sed 's/.*://')
nc 127.0.0.1 ${PORT} <<EOF
set a 1
get a
set b 2
get b
set a 3
EOF

sleep 300
