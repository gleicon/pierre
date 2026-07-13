.PHONY: build release test clippy fmt fmt-check check run clean \
        dist-plan dist-build tag tags-push publish

VERSION := $(shell grep -m1 '^version' Cargo.toml | sed -E 's/version = "(.*)"/\1/')
CONFIG  ?= pierre.toml

## Development

build: ## Debug build
	cargo build

release: ## Release build
	cargo build --release

test: ## Run the full test suite (lib unit tests + all integration tests)
	cargo test

clippy: ## Lint, all targets (lib, tests, examples)
	cargo clippy --all-targets

fmt: ## Apply rustfmt
	cargo fmt

fmt-check: ## Check formatting without modifying files (CI-safe)
	cargo fmt -- --check

check: fmt-check clippy test ## Everything CI/a pre-release should run: format, lint, test

run: build ## Run the debug binary against $(CONFIG) (default: pierre.toml)
	./target/debug/pierre $(CONFIG)

clean: ## Remove build artifacts, including cargo-dist output
	cargo clean
	rm -rf target/distrib

## Release (cargo-dist — see dist-workspace.toml)

dist-plan: ## Preview what `dist build`/a tag push would produce, without building
	dist plan

dist-build: ## Build release artifacts locally for the host platform (target/distrib/)
	dist build

# Tags an annotated vX.Y.Z release from Cargo.toml's current [package] version.
# Refuses on a dirty working tree or a version that's already tagged — bump
# Cargo.toml's version first, commit that, then tag.
tag: ## Create an annotated git tag from Cargo.toml's version (does not push)
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "working tree is dirty — commit or stash before tagging" >&2; \
		exit 1; \
	fi
	@if git rev-parse "v$(VERSION)" >/dev/null 2>&1; then \
		echo "v$(VERSION) already exists — bump Cargo.toml's version first" >&2; \
		exit 1; \
	fi
	git tag -a "v$(VERSION)" -m "Pierre v$(VERSION)"
	@echo "tagged v$(VERSION) locally — run 'make tags-push' to push it"

tags-push: ## Push the current branch and all tags to origin
	git push origin HEAD
	git push origin --tags

# Pushing the vX.Y.Z tag is what triggers .github/workflows/release.yml
# (cargo-dist), which builds the cross-platform binaries and publishes the
# GitHub Release. This target just sequences the safety checks + the two
# pushes; nothing here builds artifacts itself.
publish: check tag tags-push ## Full release: verify, tag, push — triggers the CI release workflow
	@echo "pushed v$(VERSION) — CI release workflow is now building: https://github.com/gleicon/pierre/actions"
