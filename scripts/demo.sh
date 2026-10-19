#!/bin/sh
# Quản lý một lần chạy demo tách riêng Kafka, checkpoint và thư mục output.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

if [ ! -f .env ]; then
    printf 'Thiếu .env. Tạo bằng: cp .env.example .env\n' >&2
    exit 1
fi

# Các giá trị này phải đến từ .env, không lấy lại export còn sót trong terminal.
unset CSV_PATH SPEEDUP REORDER_BUFFER STARTING_OFFSETS ARCHIVE_RAW_EVENTS

STATE_FILE=.demo_last_run
COMPOSE_FILE=docker/docker-compose.yml

read_state() {
    if [ ! -f "$STATE_FILE" ]; then
        printf 'Chưa có lần chạy nào. Dùng: sh scripts/demo.sh start\n' >&2
        exit 1
    fi
    PROJECT=$(sed -n '1p' "$STATE_FILE")
    OUTPUT_DIR=$(sed -n '2p' "$STATE_FILE")
    if [ -z "$PROJECT" ] || [ -z "$OUTPUT_DIR" ]; then
        printf 'Thông tin lần chạy trong %s không hợp lệ.\n' "$STATE_FILE" >&2
        exit 1
    fi
    export OUTPUT_HOST_DIR="../$OUTPUT_DIR"
}

compose() {
    docker compose --env-file .env -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

usage() {
    printf 'Cách dùng: sh scripts/demo.sh {start|status|logs|count|batch ID [LIMIT]|append SKIP LIMIT|down}\n' >&2
    exit 2
}

case "${1:-}" in
    start)
        SO_DONG=$(awk '/^SO_DONG=/ {value=$0; sub(/^SO_DONG=/, "", value)} END {print value}' .env)
        case "$SO_DONG" in
            ''|*[!0-9]*) printf 'SO_DONG trong .env phải là số nguyên dương.\n' >&2; exit 2 ;;
        esac
        if [ "$SO_DONG" -eq 0 ]; then
            printf 'SO_DONG trong .env phải lớn hơn 0.\n' >&2
            exit 2
        fi
        RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
        PROJECT="retail-rocket-${SO_DONG}-${RUN_ID}"
        OUTPUT_DIR="output_${SO_DONG}_${RUN_ID}"
        mkdir "$OUTPUT_DIR"
        export SKIP=0 LIMIT="$SO_DONG" OUTPUT_HOST_DIR="../$OUTPUT_DIR"
        printf 'SO_DONG=%s\nProject=%s\nOutput=%s\n' "$SO_DONG" "$PROJECT" "$OUTPUT_DIR"
        compose up --build -d
        printf '%s\n%s\n' "$PROJECT" "$OUTPUT_DIR" > "${STATE_FILE}.tmp.$$"
        mv "${STATE_FILE}.tmp.$$" "$STATE_FILE"
        compose ps -a
        printf '\nXem log: sh scripts/demo.sh logs\nKiểm tra: sh scripts/demo.sh count\n'
        ;;
    status)
        read_state
        printf 'Project=%s\nOutput=%s\n' "$PROJECT" "$OUTPUT_DIR"
        compose ps -a
        ;;
    logs)
        read_state
        compose logs -f producer spark-streaming
        ;;
    count)
        read_state
        python3 - "$OUTPUT_DIR" <<'PY'
import json
import sys
from pathlib import Path

files = list((Path(sys.argv[1]) / 'intent_input_offsets').glob('batch_id=*.json'))
ids = sorted(int(path.stem.split('=')[1]) for path in files)
total = sum(json.loads(path.read_text())['num_input_rows'] for path in files)
print('Batch điểm đã ghi:', len(ids), '| batch đầu:', ids[0] if ids else 'chưa có')
print('Sự kiện Kafka đã đọc:', total)
PY
        ;;
    batch)
        read_state
        case "${2:-}" in
            ''|*[!0-9]*) usage ;;
        esac
        LIMIT_ROWS=${3:-100}
        case "$LIMIT_ROWS" in
            ''|*[!0-9]*) usage ;;
        esac
        compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-streaming \
            /app/src/inspect_batch.py --output /app/output --batch-id "$2" --limit "$LIMIT_ROWS"
        ;;
    append)
        read_state
        case "${2:-}" in
            ''|*[!0-9]*) usage ;;
        esac
        case "${3:-}" in
            ''|*[!0-9]*) usage ;;
        esac
        export SKIP="$2" LIMIT="$3"
        compose up -d --no-deps --force-recreate producer
        ;;
    down)
        read_state
        compose down
        printf 'Đã dừng %s; giữ output %s và các volume.\n' "$PROJECT" "$OUTPUT_DIR"
        ;;
    *) usage ;;
esac
