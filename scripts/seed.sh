#!/usr/bin/env bash
# seed.sh — 通过真实 HTTP 接口回放一组典型上游消息：
#
#   序列 A（暴雨，湖北 420000，external_id=rainstorm-2026-0801-hb-001）
#     1. 修订 1（橙色）        -> 201 applied
#     2. 修订 2（红色升级）    -> 201 applied
#     3. 修订 1 重复消息       -> 200 replayed（幂等，返回同一事件行）
#     4. 修订 1 篡改变体       -> 409 冲突（同号不同内容，拒绝接收）
#     5. 修订 3 解除           -> 201 applied（当前状态变为 cancelled）
#
#   序列 B（雷暴大风，湖南 430000，external_id=thunder-2026-0801-hn-002）
#     6. 修订 2 先到（乱序）   -> 201 applied
#     7. 迟到的修订 1          -> 201 late（只留痕，当前状态保持修订 2）
#
# 用法：BASE=http://localhost:8080 ./scripts/seed.sh
set -euo pipefail

BASE="${BASE:-http://localhost:8080}"

have_jq() { command -v jq >/dev/null 2>&1; }

post() { # post <label> <json>
  local label="$1" body="$2"
  local resp code payload
  resp="$(curl -sS -w '\n%{http_code}' -X POST "$BASE/v1/warnings/events" \
    -H 'Content-Type: application/json' -d "$body")"
  code="$(tail -n1 <<<"$resp")"
  payload="$(sed '$d' <<<"$resp")"
  printf '\n=== %s -> HTTP %s\n' "$label" "$code"
  if have_jq; then
    jq '{outcome, revision: .event.revision, event_id: .event.id, current: {revision: .current.revision, status: .current.status, severity: .current.severity}} // .' <<<"$payload" 2>/dev/null || echo "$payload"
  else
    echo "$payload"
  fi
}

get() { # get <label> <path>
  local label="$1" path="$2"
  printf '\n--- %s\n' "$label"
  if have_jq; then
    curl -sS "$BASE$path" | jq . 2>/dev/null || curl -sS "$BASE$path"
  else
    curl -sS "$BASE$path"
  fi
  echo
}

A_SRC=cn-met
A_EXT=rainstorm-2026-0801-hb-001
B_EXT=thunder-2026-0801-hn-002

zh() { # zh <severity>
  case "$1" in
    blue) echo 蓝色 ;; yellow) echo 黄色 ;; orange) echo 橙色 ;; red) echo 红色 ;; *) echo "$1" ;;
  esac
}

rev_a() { # rev_a <revision> <severity> <status>
  cat <<JSON
{"source":"$A_SRC","external_id":"$A_EXT","revision":$1,
 "severity":"$2","status":"$3","region_code":"420000",
 "headline":"暴雨$(zh "$2")预警（修订 $1）","description":"湖北省暴雨预警，修订 $1",
 "issued_at":"2026-08-01T08:00:00+08:00",
 "effective_at":"2026-08-01T09:00:00+08:00",
 "expires_at":"2026-08-02T09:00:00+08:00"}
JSON
}

rev_b() { # rev_b <revision> <severity>
  cat <<JSON
{"source":"$A_SRC","external_id":"$B_EXT","revision":$1,
 "severity":"$2","status":"active","region_code":"430000",
 "headline":"雷暴大风$(zh "$2")预警（修订 $1）","description":"湖南省雷暴大风预警，修订 $1",
 "issued_at":"2026-08-01T08:30:00+08:00",
 "effective_at":"2026-08-01T09:30:00+08:00",
 "expires_at":"2026-08-02T09:30:00+08:00"}
JSON
}

echo "########## 序列 A：暴雨（湖北 420000）##########"
post "A1 修订 1（橙色）"        "$(rev_a 1 orange active)"
post "A2 修订 2（红色升级）"    "$(rev_a 2 red active)"
post "A3 修订 1 重复消息"       "$(rev_a 1 orange active)"
post "A4 修订 1 篡改变体"       "$(rev_a 1 red active)"
post "A5 修订 3 解除"           "$(rev_a 3 red cancelled)"

echo
echo "########## 序列 B：雷暴大风（湖南 430000，乱序到达）##########"
post "B1 修订 2 先到"           "$(rev_b 2 yellow)"
post "B2 迟到的修订 1"          "$(rev_b 1 blue)"

echo
echo "########## 回放后的读侧核对 ##########"
get "A 当前状态（应为修订 3 cancelled）"        "/v1/warnings/$A_SRC/$A_EXT"
get "A 审计轨迹（3 条事件，按接收顺序）"        "/v1/warnings/$A_SRC/$A_EXT/events"
get "B 当前状态（应保持修订 2，迟到修订 1 未回退）" "/v1/warnings/$A_SRC/$B_EXT"
get "B 审计轨迹（修订 2 在前，迟到修订 1 在后）"    "/v1/warnings/$A_SRC/$B_EXT/events"
get "检索：湖北 420000 当前预警"                "/v1/warnings?region_code=420000"
get "检索：已解除的预警"                        "/v1/warnings?status=cancelled"
