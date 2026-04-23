#!/bin/bash
# TradingAgents-CN 重启脚本
# 适用于无 systemd 的容器环境

set -e

PROJECT_DIR="/projects/TradingAgents-CN"
MONGO_DBPATH="$PROJECT_DIR/data/mongodb"
MONGO_LOG="/var/log/mongodb/mongod.log"
REDIS_LOG="/var/log/redis.log"
BACKEND_LOG="/tmp/tradingagents-backend.log"
FRONTEND_LOG="/tmp/tradingagents-frontend.log"

echo ""
echo "========================================"
echo "  TradingAgents-CN 重启脚本"
echo "========================================"
echo ""

# ── 1. 停止旧进程 ────────────────────────────────────────────

echo "[1/5] 停止旧进程..."

# 停止前端
pkill -f "vite.*3000" 2>/dev/null && echo "  ✓ 前端已停止" || echo "  - 前端未运行"

# 停止后端
pkill -f "uvicorn app.main" 2>/dev/null && echo "  ✓ 后端已停止" || echo "  - 后端未运行"

# 停止 MongoDB
if pgrep -x mongod > /dev/null 2>&1; then
    mongod --shutdown --dbpath "$MONGO_DBPATH" 2>/dev/null && echo "  ✓ MongoDB 已停止" || pkill -x mongod 2>/dev/null
else
    echo "  - MongoDB 未运行"
fi

# 停止 Redis
pkill -x redis-server 2>/dev/null && echo "  ✓ Redis 已停止" || echo "  - Redis 未运行"

sleep 2

# ── 2. 启动 MongoDB ─────────────────────────────────────────

echo ""
echo "[2/5] 启动 MongoDB..."

mkdir -p "$MONGO_DBPATH" /var/log/mongodb /var/run/mongodb

mongod \
    --dbpath "$MONGO_DBPATH" \
    --logpath "$MONGO_LOG" \
    --fork \
    --bind_ip 127.0.0.1 \
    --port 27017 \
    --auth 2>&1 | tail -1

sleep 3

# 验证
if mongosh "mongodb://admin:tradingagents123@localhost:27017/admin" \
    --eval "db.runCommand({ping:1})" --quiet 2>/dev/null | grep -q '"ok": 1\|{ ok: 1'; then
    echo "  ✓ MongoDB 启动成功"
else
    echo "  ✗ MongoDB 启动失败，查看日志: $MONGO_LOG"
    exit 1
fi

# ── 3. 启动 Redis ────────────────────────────────────────────

echo ""
echo "[3/5] 启动 Redis..."

redis-server \
    --daemonize yes \
    --requirepass tradingagents123 \
    --logfile "$REDIS_LOG" \
    --port 6379

sleep 1

if redis-cli -a tradingagents123 ping 2>/dev/null | grep -q PONG; then
    echo "  ✓ Redis 启动成功"
else
    echo "  ✗ Redis 启动失败，查看日志: $REDIS_LOG"
    exit 1
fi

# ── 3.5 初始化 Provider 和应用管理员账号 ────────────────────────

echo ""
echo "[3.5/5] 初始化 LLM Provider 和管理员账号..."

cd "$PROJECT_DIR"

# 强制写入默认模型配置
echo "  设置默认分析模型..."
python3 -c "
import json
path = '$PROJECT_DIR/config/settings.json'
with open(path) as f: d = json.load(f)
d['quick_think_llm'] = 'claude-opus-4.6'
d['deep_think_llm'] = 'claude-opus-4.6-1m'
d['quick_analysis_model'] = 'claude-opus-4.6'
d['deep_analysis_model'] = 'claude-opus-4.6-1m'
with open(path, 'w') as f: json.dump(d, f, indent=2, ensure_ascii=False)
print('OK')
" && echo "  ✓ 默认模型已设置 (快速: claude-opus-4.6 / 深度: claude-opus-4.6-1m)"

# 初始化 llm_providers 集合（含 CodeBuddy）
PYTHONPATH="$PROJECT_DIR" venv/bin/python app/scripts/init_providers.py > /tmp/tradingagents-providers.log 2>&1
if grep -q "成功初始化" /tmp/tradingagents-providers.log 2>/dev/null; then
    echo "  ✓ LLM Provider 初始化完成"
else
    echo "  ⚠ Provider 初始化异常，查看日志: /tmp/tradingagents-providers.log"
fi

# 写入 CodeBuddy API Key
CB_KEY=$(grep 'CODEBUDDY_API_KEY=' "$PROJECT_DIR/.env" | cut -d'=' -f2 | tr -d '\r')
if [ -n "$CB_KEY" ] && [ "$CB_KEY" != "your_codebuddy_api_key" ]; then
    mongosh "mongodb://admin:tradingagents123@localhost:27017/tradingagents?authSource=admin" --quiet --eval \
        "db.llm_providers.updateOne({name:'codebuddy'},{'\$set':{api_key:'$CB_KEY',is_active:true}});" 2>/dev/null
    echo "  ✓ CodeBuddy API Key 已写入"
fi

# 同步 CodeBuddy 全部模型到 system_configs.llm_configs（幂等，已存在则跳过）
PYTHONPATH="$PROJECT_DIR" venv/bin/python "$PROJECT_DIR/scripts/sync_codebuddy_models.py" \
    > /tmp/tradingagents-codebuddy-sync.log 2>&1
if grep -qE "(已追加|已齐全)" /tmp/tradingagents-codebuddy-sync.log 2>/dev/null; then
    tail -n 1 /tmp/tradingagents-codebuddy-sync.log | sed 's/^/  /'
else
    echo "  ⚠ CodeBuddy 模型同步异常，查看日志: /tmp/tradingagents-codebuddy-sync.log"
fi

# 初始化管理员账号
PYTHONPATH="$PROJECT_DIR" venv/bin/python scripts/create_default_admin.py > /tmp/tradingagents-init.log 2>&1
if grep -q "操作完成" /tmp/tradingagents-init.log 2>/dev/null; then
    echo "  ✓ 管理员账号就绪 (admin / admin123)"
else
    echo "  ⚠ 初始化脚本未返回预期结果，查看日志: /tmp/tradingagents-init.log"
fi

# ── 3.6 恢复 CodeBuddy 模型配置 ────────────────────────────────


# ── 4. 启动后端 ──────────────────────────────────────────────

echo ""
echo "[4/5] 启动后端 (FastAPI)..."

cd "$PROJECT_DIR"
PYTHONPATH="$PROJECT_DIR" nohup \
    venv/bin/uvicorn app.main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --workers 1 \
    > "$BACKEND_LOG" 2>&1 &

BACKEND_PID=$!
echo "  后端 PID: $BACKEND_PID"

# 等待后端就绪（最多 30 秒）
echo "  等待后端就绪..."
for i in $(seq 1 30); do
    if curl -s http://localhost:8000/api/health > /dev/null 2>&1; then
        echo "  ✓ 后端启动成功 (http://localhost:8000)"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "  ✗ 后端启动超时，查看日志: $BACKEND_LOG"
        exit 1
    fi
    sleep 1
done

# 将默认模型写入数据库（前端从数据库读取）
TOKEN=$(curl -s -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' | python3 -c "import json,sys; print(json.load(sys.stdin).get('data',{}).get('access_token',''))" 2>/dev/null)
if [ -n "$TOKEN" ]; then
    curl -s -X PUT http://localhost:8000/api/config/settings \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d '{"quick_analysis_model":"claude-opus-4.6","deep_analysis_model":"claude-opus-4.6-1m","quick_think_llm":"claude-opus-4.6","deep_think_llm":"claude-opus-4.6-1m","finnhub_api_key":"d7bsf81r01quh9fbk6lgd7bsf81r01quh9fbk6m0","alpha_vantage_api_key":"JWEQCDPS4VH255SC"}' > /dev/null 2>&1
    echo "  ✓ 默认模型已同步到数据库 (claude-opus-4.6 / claude-opus-4.6-1m) + Finnhub Key"
fi

# ── 5. 启动前端 ──────────────────────────────────────────────

echo ""
echo "[5/5] 启动前端 (Vue 3)..."

cd "$PROJECT_DIR/frontend"
nohup npm run dev -- --host 0.0.0.0 --port 3000 > "$FRONTEND_LOG" 2>&1 &

FRONTEND_PID=$!
echo "  前端 PID: $FRONTEND_PID"

# 等待前端就绪（最多 20 秒）
for i in $(seq 1 20); do
    if curl -s http://localhost:3000 > /dev/null 2>&1; then
        echo "  ✓ 前端启动成功 (http://localhost:3000)"
        break
    fi
    if [ $i -eq 20 ]; then
        echo "  ✗ 前端启动超时，查看日志: $FRONTEND_LOG"
        exit 1
    fi
    sleep 1
done

# ── 完成 ────────────────────────────────────────────────────

echo ""
echo "========================================"
echo "  ✅ TradingAgents-CN 启动完成"
echo "========================================"
echo ""
echo "  前端:  http://localhost:3000"
echo "  后端:  http://localhost:8000"
echo "  API文档: http://localhost:8000/docs"
echo ""
echo "  日志文件:"
echo "    后端:    $BACKEND_LOG"
echo "    前端:    $FRONTEND_LOG"
echo "    MongoDB: $MONGO_LOG"
echo "    Redis:   $REDIS_LOG"
echo ""
