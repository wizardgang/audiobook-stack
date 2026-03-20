HOSTS = 10.0.0.8 10.0.0.9
KEY = $(shell pwd)/docker
EXCLUDES = --exclude=inbox --exclude=outputs --exclude=audiobooks --exclude=.git --exclude=.env
USER ?= root
DEST ?= /root/tts-node

.PHONY: up down logs status reset worker-remote inject sync watch-sync

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
worker-remote:
	@test -n "$(HOST)" || (echo "Usage: make worker-remote HOST=... WORKER_ID=..."; exit 1)
	rsync -av -e "ssh -i $(KEY) -o StrictHostKeyChecking=no" $(EXCLUDES) \
	  . $(USER)@$(HOST):$(DEST)
	ssh -i $(KEY) -o StrictHostKeyChecking=no $(USER)@$(HOST) "cd $(DEST) && \
	  REDIS_URL=redis://$$(hostname -I | awk '{print $$1}'):6379 \
	  WORKER_ID=$(WORKER_ID) \
	  docker compose -f docker-compose.worker.yml up -d --build"
	@echo "Remote worker $(WORKER_ID) started on $(USER)@$(HOST):$(DEST)"

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

# Continuous sync every 10 seconds
watch-sync:
	@echo "Starting continuous sync to $(HOSTS)..."
	@while true; do \
		$(MAKE) -s sync; \
		echo "Sync complete. Waiting 10s..."; \
		sleep 10; \
	done

