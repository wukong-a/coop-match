#!/usr/bin/env bash
# ============================================================
# 一键更新脚本：备份 → 覆盖新文件 → 装依赖 → 重启 → 健康检查
# 健康检查失败会自动回滚到更新前版本并再次重启。
#
# 用法（在服务器上，coop-match 目录内或任意位置执行）：
#   bash scripts/update.sh /path/to/new-version/          # 新代码目录
#   bash scripts/update.sh                                # 不传目录 = 已用 git pull 拉好新代码，只做备份+重启+检查
#
# 可选环境变量：
#   PORT=3000                 健康检查端口（默认 3000）
#   SYSTEMD_SERVICE=coop-match  systemd 服务名（配置了就用 systemctl 重启）
#   （检测到 pm2 且有 coop-match 应用时优先用 pm2 restart）
# ============================================================
set -uo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="${1:-}"
PORT="${PORT:-3000}"
HEALTH_URL="http://127.0.0.1:${PORT}/api/info"
TS="$(date +%Y%m%d-%H%M%S)"
BACKUP_DIR="$APP_DIR/.backup/$TS"

# 需要备份/覆盖的文件（不包含 data/ —— 那是运行数据，永远不动）
ITEMS=(server.js package.json public lib scripts)

echo "==> 1/5 备份当前版本到 $BACKUP_DIR"
mkdir -p "$BACKUP_DIR"
for item in "${ITEMS[@]}"; do
  [ -e "$APP_DIR/$item" ] && cp -r "$APP_DIR/$item" "$BACKUP_DIR/"
done
echo "    备份完成（data/ 运行数据不在备份/覆盖范围，永远安全）"

echo "==> 2/5 覆盖新文件"
if [ -n "$SRC_DIR" ]; then
  if [ ! -d "$SRC_DIR" ]; then
    echo "    错误：新代码目录 $SRC_DIR 不存在"; exit 1
  fi
  for item in "${ITEMS[@]}"; do
    [ -e "$SRC_DIR/$item" ] && cp -r "$SRC_DIR/$item" "$APP_DIR/"
  done
  echo "    已从 $SRC_DIR 覆盖"
else
  echo "    未传新代码目录，假定你已用 git pull / 手动放好新文件，跳过覆盖"
fi

echo "==> 3/5 安装依赖（仅当 package.json 有变化时才真正联网）"
cd "$APP_DIR"
cp -f "$BACKUP_DIR/package.json" /tmp/coop-pkg-old.json 2>/dev/null || true
if ! cmp -s package.json /tmp/coop-pkg-old.json 2>/dev/null; then
  npm install --omit=dev || { echo "    npm install 失败，回滚"; ROLLBACK=1; }
else
  echo "    package.json 未变化，跳过"
fi

echo "==> 4/5 重启服务"
restart() {
  if command -v pm2 >/dev/null 2>&1 && pm2 list 2>/dev/null | grep -qE "coop-match|it2-match"; then
    pm2 restart coop-match && return 0
  fi
  if [ -n "${SYSTEMD_SERVICE:-}" ]; then
    systemctl restart "$SYSTEMD_SERVICE" && return 0
  fi
  echo "    未检测到 pm2 应用或 systemd 服务，请按你原来的方式手动重启（如：先 kill 旧进程，再带原环境变量重新启动），完成后按回车继续"
  read -r _
}
restart

echo "==> 5/5 健康检查（最多等 15 秒）"
check() {
  for i in $(seq 1 15); do
    if curl -sf --max-time 2 "$HEALTH_URL" >/dev/null 2>&1; then
      echo "    ✅ 服务健康：$HEALTH_URL"
      echo "    版本信息：$(curl -sf --max-time 2 "$HEALTH_URL")"
      echo ""
      echo "🎉 更新成功。旧版本备份保留在 $BACKUP_DIR（确认稳定后可删除）"
      exit 0
    fi
    sleep 1
  done
  return 1
}

if check; then :; else
  echo "    ❌ 健康检查失败，自动回滚到更新前版本..."
  for item in "${ITEMS[@]}"; do
    [ -e "$BACKUP_DIR/$item" ] && rm -rf "$APP_DIR/$item" && cp -r "$BACKUP_DIR/$item" "$APP_DIR/"
  done
  restart
  if check; then
    echo "    已回滚到旧版本并恢复运行。新代码有问题，请检查后再试。"
    exit 2
  else
    echo "    ⚠️ 回滚后仍不健康，请手动检查：备份在 $BACKUP_DIR，日志看服务输出"
    exit 3
  fi
fi
