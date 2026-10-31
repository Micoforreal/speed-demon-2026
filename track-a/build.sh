#!/usr/bin/env bash
# Compiles main.rs offline (no network) into ./run.
# Final build uses rustc -C opt-level=3 -C lto=fat -C codegen-units=1 -C target-cpu=x86-64-v3 -C panic=abort.
set -euo pipefail
rustc -C opt-level=3 -C lto=fat -C codegen-units=1 -C target-cpu=x86-64-v3 -C panic=abort main.rs -o run
