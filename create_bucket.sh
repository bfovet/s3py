#!/bin/bash

mc alias set minio http://localhost:9000 minio minioadmin123
mc admin accesskey create minio/ --access-key minio_user --secret-key minioadmin123
