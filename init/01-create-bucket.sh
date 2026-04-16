#!/bin/sh
set -eu
mc alias set local http://minio:9000 minioadmin minioadmin
mc mb -p local/warehouse
echo "Bucket created."