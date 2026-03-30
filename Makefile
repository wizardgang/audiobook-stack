HOSTS = 10.0.0.8 10.0.0.9
KEY = $(shell pwd)/docker
EXCLUDES = --exclude=inbox --exclude=outputs --exclude=audiobooks --exclude=.git --exclude=.env
USER ?= root
DEST ?= /root/tts-node

.PHONY: up down logs status reset worker-remote inject sync watch-sync retry books invalidate-chunks purge-book speak speak-chattts

# ── Core ──────────────────────────────────────────────────────────────────────

up:
	@mkdir -p inbox chunks outputs audiobooks abs-metadata abs-config
	docker-compose up -d --build
	@echo ""
	@echo "  Drop PDFs into ./inbox/ to start the pipeline"
	@echo "  Dashboard:       http://localhost:9000"
	@echo "  Audiobookshelf:  http://localhost:13378"

down:
	docker-compose down

logs:
	docker-compose logs -f --tail=50

# ── Status ────────────────────────────────────────────────────────────────────

status:
	@docker exec pipeline-redis redis-cli hgetall "book:$$(docker exec pipeline-redis redis-cli smembers pipeline:seen_files | head -1)" 2>/dev/null || \
	  echo "No books processed yet"

queue:
	@echo "=== TTS queue depth ==="
	@docker exec pipeline-redis redis-cli llen pipeline:tts
	@echo "=== Done queue depth ==="
	@docker exec pipeline-redis redis-cli llen pipeline:done

# ── Remote workers ────────────────────────────────────────────────────────────
# Usage: make worker-remote HOST=192.168.1.20 WORKER_ID=remote-1 [USER=deploy] [DEST=/opt/tts-node]
# On the remote node, choose a volume override:
#   Linux (pre-mounted NFS):  docker compose -f docker-compose.worker.yml up -d --build
#   Windows (NFS via Docker): docker compose -f docker-compose.worker.yml -f docker-compose.worker.nfs.yml up -d --build
#   Windows (SMB/CIFS):       docker compose -f docker-compose.worker.yml -f docker-compose.worker.smb.yml up -d --build
worker-remote:
	@test -n "$(HOST)" || (echo "Usage: make worker-remote HOST=... WORKER_ID=..."; exit 1)
	rsync -av -e "ssh -i $(KEY) -o StrictHostKeyChecking=no" $(EXCLUDES) \
	  . $(USER)@$(HOST):$(DEST)
	ssh -i $(KEY) -o StrictHostKeyChecking=no $(USER)@$(HOST) "cd $(DEST) && \
	  REDIS_URL=redis://$$(hostname -I | awk '{print $$1}'):6379 \
	  WORKER_ID=$(WORKER_ID) \
	  docker compose -f docker-compose.worker.yml up -d --build"
	@echo "Remote worker $(WORKER_ID) started on $(USER)@$(HOST):$(DEST)"

# ── Audio quality test ───────────────────────────────────────────────────────
# Synthesise text directly, bypassing the full pipeline. Output: ./tts-test.mp3
#
# Abogen/Kokoro (abogen container must be running):
#   make speak TEXT="The quick brown fox jumps over the lazy dog."
#   make speak FILE=test.txt
#
# ChatTTS (pipeline-chattts-worker container must be running):
#   make speak-chattts TEXT="The quick brown fox."
#   make speak-chattts FILE=test.txt

speak:
	@test -n "$(TEXT)$(FILE)" || (echo "Usage: make speak TEXT='...'  or  make speak FILE=path/to/file.txt"; exit 1)
	@TEXT_CONTENT=$$([ -n "$(FILE)" ] && cat "$(FILE)" || printf '%s' "$(TEXT)"); \
	PAYLOAD=$$(python3 -c "import json,sys; print(json.dumps({'text':sys.argv[1],'format':'mp3'}))" "$$TEXT_CONTENT"); \
	curl -sf -X POST http://localhost:8808/api/generate \
		-H "Content-Type: application/json" \
		-d "$$PAYLOAD" \
		-o tts-test.mp3 \
	&& echo "Saved → tts-test.mp3 ($$(du -h tts-test.mp3 | cut -f1))" \
	|| echo "ERROR: abogen API call failed — is the abogen container running?"

speak-chattts:
	@test -n "$(TEXT)$(FILE)" || (echo "Usage: make speak-chattts TEXT='...'  or  make speak-chattts FILE=path/to/file.txt"; exit 1)
	@TEXT_CONTENT=$$([ -n "$(FILE)" ] && cat "$(FILE)" || printf '%s' "$(TEXT)"); \
	docker exec pipeline-chattts-worker python /app/speak.py "$$TEXT_CONTENT" /tmp/tts-test.mp3 \
	&& docker cp pipeline-chattts-worker:/tmp/tts-test.mp3 ./tts-test.mp3 \
	&& echo "Saved → tts-test.mp3 ($$(du -h tts-test.mp3 | cut -f1))" \
	|| echo "ERROR: pipeline-chattts-worker container not running (start with COMPOSE_PROFILES=chattts-tts)"

# ── Inject test PDF ──────────────────────────────────────────────────────────
inject:
	@test -n "$(FILE)" || (echo "Usage: make inject FILE=path/to/book.pdf"; exit 1)
	cp "$(FILE)" ./inbox/

# ── Reset ─────────────────────────────────────────────────────────────────────
reset:
	docker exec pipeline-redis redis-cli flushdb
	rm -rf chunks/* outputs/*
	@echo "Queue and temp files cleared (audiobooks preserved)"


# Single sync to all hosts
sync:
	@for host in $(HOSTS); do \
		echo "--- Syncing to $$host ---"; \
		rsync -av -e "ssh -i $(KEY) -o StrictHostKeyChecking=no" $(EXCLUDES) . $(USER)@$$host:$(DEST); \
	done

# ── Queue Control ────────────────────────────────────────────────────────────

pause:
	@echo "Pausing entire pipeline cluster..."
	@docker exec pipeline-redis redis-cli set pipeline:state paused

resume:
	@echo "Resuming pipeline cluster..."
	@docker exec pipeline-redis redis-cli del pipeline:state

# ── Purge a book and restart from scratch ─────────────────────────────────
# Usage: make purge-book BOOK_ID=<id>
# Removes all TTS chunks from the queue, deletes Redis state, then
# re-queues the book for full re-processing via pipeline:orchestrate.
purge-book:
	@test -n "$(BOOK_ID)" || (echo "Usage: make purge-book BOOK_ID=<id>"; exit 1)
	@echo "Removing TTS chunks for $(BOOK_ID) from queue..."
	@docker exec pipeline-redis redis-cli eval "local items = redis.call('lrange', KEYS[1], 0, -1); local removed = 0; for _, item in ipairs(items) do if string.find(item, ARGV[1], 1, true) then redis.call('lrem', KEYS[1], 1, item); removed = removed + 1 end end; return removed" 1 pipeline:tts "$(BOOK_ID)"
	@echo "Deleting Redis state keys..."
	@docker exec pipeline-redis redis-cli eval "local keys = redis.call('keys', 'chunk:$(BOOK_ID):*'); for _, k in ipairs(keys) do redis.call('del', k) end; return #keys" 0
	@docker exec pipeline-redis redis-cli srem pipeline:seen_files "$(BOOK_ID)"
	@BOOK_FILE=$$(docker exec pipeline-redis redis-cli hget "book:$(BOOK_ID)" filename); \
	docker exec pipeline-redis redis-cli del "book:$(BOOK_ID)"; \
	echo "Re-queuing $$BOOK_FILE for full re-processing..."; \
	docker exec pipeline-redis redis-cli lpush pipeline:orchestrate "{\"id\":\"$(BOOK_ID)\",\"path\":\"/inbox/$$BOOK_FILE\"}"
	@echo "Done — book $(BOOK_ID) will be re-processed from scratch"

# ── Retry failed merge ────────────────────────────────────────────────────
# Usage: make retry BOOK_ID=<book_id>
retry:
	@test -n "$(BOOK_ID)" || (echo "Usage: make retry BOOK_ID=<id>"; exit 1)
	@TITLE=$$(docker exec pipeline-redis redis-cli hget "book:$(BOOK_ID)" title); \
	TOTAL=$$(docker exec pipeline-redis redis-cli hget "book:$(BOOK_ID)" total_chunks); \
	test -n "$$TITLE" || (echo "Error: book:$(BOOK_ID) not found in Redis"; exit 1); \
	docker exec pipeline-redis redis-cli hset "book:$(BOOK_ID)" status queued error "" merge_started_at ""; \
	docker exec pipeline-redis redis-cli lpush pipeline:done \
	  "{\"book_id\":\"$(BOOK_ID)\",\"title\":\"$$TITLE\",\"total\":$$TOTAL,\"out_dir\":\"/outputs/$(BOOK_ID)\"}"; \
	echo "Merge re-triggered for book $(BOOK_ID) ($$TITLE, $$TOTAL chunks)"

# ── Invalidate / re-queue chunks ─────────────────────────────────────────
# Usage:
#   make invalidate-chunks BOOK_ID=<id>               # re-queue all error chunks
#   make invalidate-chunks BOOK_ID=<id> CHUNKS=all    # re-queue every chunk
#   make invalidate-chunks BOOK_ID=<id> CHUNKS=0,5,23 # re-queue specific indices
invalidate-chunks:
	@test -n "$(BOOK_ID)" || (echo "Usage: make invalidate-chunks BOOK_ID=<id> [CHUNKS=all|errors|0,5,23]"; exit 1)
	docker exec pipeline-orchestrator python /app/invalidate_chunks.py "$(BOOK_ID)" "$(or $(CHUNKS),errors)"

# ── List all books ────────────────────────────────────────────────────────
books:
	@echo "=== Books in pipeline ==="
	@for id in $$(docker exec pipeline-redis redis-cli smembers pipeline:seen_files); do \
		STATUS=$$(docker exec pipeline-redis redis-cli hget "book:$$id" status); \
		TITLE=$$(docker exec pipeline-redis redis-cli hget "book:$$id" title); \
		DONE=$$(docker exec pipeline-redis redis-cli hget "book:$$id" done_chunks); \
		TOTAL=$$(docker exec pipeline-redis redis-cli hget "book:$$id" total_chunks); \
		echo "  [$$STATUS] $$TITLE ($$DONE/$$TOTAL) — ID: $$id"; \
	done

# Continuous sync every 10 seconds
watch-sync:
	@echo "Starting continuous sync to $(HOSTS)..."
	@while true; do \
		$(MAKE) -s sync; \
		echo "Sync complete. Waiting 10s..."; \
		sleep 10; \
	done

