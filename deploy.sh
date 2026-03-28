#!/usr/bin/env bash
# ================================================================
# deploy.sh — 百彩百中 服务器端部署脚本
# ================================================================
set -euo pipefail

PROJECT_DIR="/opt/baicaibaizhong"
CONTAINER_NAME="baicaibaizhong"
IMAGE_NAME="baicaibaizhong:latest"
PORT=88

cd "$PROJECT_DIR"

echo "══ 1. 拉取最新代码 ══"
git pull origin main

echo "══ 2. 构建 Docker 镜像 ══"
docker build -t "$IMAGE_NAME" .

echo "══ 3. 重启容器 ══"
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
docker run -d \
    --name "$CONTAINER_NAME" \
    --restart always \
    --network wanzi-net \
    -p ${PORT}:${PORT} \
    -v ${PROJECT_DIR}/project:/opt/project \
    -v ${PROJECT_DIR}/outputs:/opt/outputs \
    "$IMAGE_NAME"

echo "══ 4. 验证 ══"
sleep 2
curl -s http://localhost:${PORT}/health && echo ""
echo "✓ 部署完成！"
