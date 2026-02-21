.PHONY: all build test lint fmt fmt-check coverage coverage-check clean \
       tier1 tier2 tier3 tier4 ci doc

JAVA_HOME := $(shell cs java-home)
export JAVA_HOME
export PATH := $(HOME)/.local/share/coursier/bin:$(JAVA_HOME)/bin:$(PATH)

# === Build ===
build:
	sbt compile

build-test:
	sbt "Test / compile"

# === Testing ===
test:
	sbt test

test-only:
	sbt "testOnly $(CLASS)"

# === Formatting ===
fmt:
	scalafmt

fmt-check:
	scalafmt --check

# === Linting ===
lint: fmt-check build
	@echo "Lint passed"

# === Coverage ===
coverage:
	sbt clean coverage test coverageReport
	@echo "Coverage report: target/scala-2.12/scoverage-report/index.html"

coverage-check:
	sbt clean coverage test coverageReport
	@echo "Coverage check passed (95% minimum enforced by sbt)"

# === Documentation ===
doc:
	sbt doc

book:
	mdbook build book

book-serve:
	mdbook serve book --port 3000

# === Quality Tiers (Certeza Methodology) ===
# Tier 1: On-save (<30s)
tier1: fmt-check build

# Tier 2: Pre-commit (<5min)
tier2: tier1 test

# Tier 3: Pre-push (5-10min)
tier3: tier2 coverage-check

# Tier 4: CI/CD (full)
tier4: tier3 doc
	@echo "All CI checks passed"

# === CI ===
ci: tier4

# === Clean ===
clean:
	sbt clean
	rm -rf target/ project/target/
