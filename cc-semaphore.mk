GO_COVERAGE_HTML ?= coverage.html
COVERAGE_REPORT_URL := $(SEMAPHORE_ORGANIZATION_URL)/jobs/$(SEMAPHORE_JOB_ID)/artifacts/$(GO_COVERAGE_HTML)

ifeq ($(SEMAPHORE_2),true)
# In Semaphore 2, the cache must be manually managed.
# References:
#   https://docs.semaphoreci.com/article/68-caching-dependencies
#   https://docs.semaphoreci.com/article/54-toolbox-reference#cache

INIT_CI_TARGETS += ci-bin-sem-cache-restore
EPILOGUE_TARGETS += ci-bin-sem-cache-store store-test-results-to-semaphore
DEB_CACHE_DIR ?= $(SEMAPHORE_CACHE_DIR)/.deb-cache
PIP_CACHE_DIR ?= $(shell pip3 cache dir)
CI_BIN_CACHE_KEY = $(CI_BIN)
# How many days cache entries can stay in the semaphore cache before they are considered stale
SEM_CACHE_DURATION_DAYS ?= 7
current_time := $(shell date +"%s")

# Only write to the cache from master builds because of security reasons. 
.PHONY: ci-bin-sem-cache-store
ci-bin-sem-cache-store:
ifneq ($(SEMAPHORE_GIT_REF_TYPE),pull-request)
	@echo "Storing semaphore caches"
	$(MAKE) _ci-bin-sem-cache-store SEM_CACHE_KEY=$(CI_BIN_CACHE_KEY) SEM_CACHE_PATH=$(CI_BIN)
	$(MAKE) _ci-bin-sem-cache-store SEM_CACHE_KEY=gocache SEM_CACHE_PATH=$(GOPATH)/pkg/mod
	$(MAKE) _ci-bin-sem-cache-store SEM_CACHE_KEY=golangci_lint_cache SEM_CACHE_PATH=$(GOLANGCI_LINT_CACHE)
	$(MAKE) _ci-bin-sem-cache-store SEM_CACHE_KEY=pip3_cache SEM_CACHE_PATH=$(PIP_CACHE_DIR)
	$(MAKE) _ci-bin-sem-cache-store SEM_CACHE_KEY=install_package_cache SEM_CACHE_PATH=$(DEB_CACHE_DIR)
	$(MAKE) _ci-bin-sem-cache-store SEM_CACHE_KEY=maven_cache SEM_CACHE_PATH=$(HOME)/.m2/repository
endif

# cache restore allows fuzzy matching. When it finds multiple matches, it will select the most recent cache archive.
# Additionally, it will not overwrite an existing cache archive with the same key.
# Therefore, we store the cache with a timestamp in the key to avoid collisions.
#
# But caching can be expensive, so we'll only recache an item if the previous item was cached a while ago,
# we arbitrarily put three days ago for now, see the logic in _ci-bin-sem-cache-store
.PHONY: _ci-bin-sem-cache-store
_ci-bin-sem-cache-store:
	@stored_timestamp=$$(cache list | grep $(SEM_CACHE_KEY)_ | awk '{print $$1}' | awk -F_ '{print $$NF}' | sort -r | awk 'NR==1'); \
	if [ -z "$$stored_timestamp" ]; then \
		echo "Cache entry $(SEM_CACHE_KEY) does not exist in the cache, try to store it..."; \
		cache store $(SEM_CACHE_KEY)_$(current_time) $(SEM_CACHE_PATH); \
	else \
		threshold_timestamp=$$(date -d "$(SEM_CACHE_DURATION_DAYS) days ago" +%s); \
		if [ "$$stored_timestamp" -lt "$$threshold_timestamp" ]; then \
			echo "Existing entry $(SEM_CACHE_KEY) is too old, storing it again..."; \
			cache store $(SEM_CACHE_KEY)_$(current_time) $(SEM_CACHE_PATH); \
		else \
			echo "Cache entry $(SEM_CACHE_KEY) was updated recently, skipping..."; \
		fi \
	fi

.PHONY: ci-bin-sem-cache-restore
ci-bin-sem-cache-restore:
	@echo "Restoring semaphore caches"
	cache restore $(CI_BIN_CACHE_KEY)_
	cache restore gocache_
	cache restore golangci_lint_cache_
	cache restore pip3_cache_
	cache restore install_package_cache_
	cache restore maven_cache_

.PHONY: ci-bin-sem-cache-delete
ci-bin-sem-cache-delete:
	@echo "Deleting semaphore caches"
	cache delete $(CI_BIN_CACHE_KEY)_
endif

.PHONY: ci-generate-and-store-coverage-data
ci-generate-and-store-coverage-data: $(GO_COVERAGE_HTML) print-coverage-out
	artifact push job $(GO_COVERAGE_HTML)

.PHONY: ci-coverage
ci-coverage: ci-generate-and-store-coverage-data go-gate-coverage
	@echo "find coverate report at: $(COVERAGE_REPORT_URL)"

.PHONY: store-test-results-to-semaphore
store-test-results-to-semaphore:
ifneq ($(wildcard $(TEST_RESULT_FILE)),)
ifeq ($(TEST_RESULT_NAME),)
	test-results publish $(TEST_RESULT_FILE) --force
else
	test-results publish $(TEST_RESULT_FILE) --name "$(TEST_RESULT_NAME)"
endif
else
	@echo "test results not found at $(TEST_RESULT_FILE)"
endif
