#!/usr/bin/env bash
# Generates the PAR2 test fixtures committed alongside this script.
#
# Produces 3 small source files (40000/35000/25000 bytes - none a multiple of
# the 4096-byte slice size, so every file's final slice is padding-trimmed on
# repair) totaling 100000 bytes, and a PAR2 recovery set covering them at 20%
# redundancy, using par2cmdline (https://github.com/Parchive/par2cmdline).
#
# Source file content is deterministic (AES-CTR keystream from fixed
# keys/IVs, not real crypto use) so the fixtures reproduce byte-for-byte on
# regeneration - re-running this script should produce a no-op git diff.
#
# Requires: par2 (par2cmdline) and openssl on PATH.
#
# Usage: ./gen.sh   (run from this directory)
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

rm -f file1.bin file2.bin file3.bin fixture.par2 fixture.vol*.par2

gen_file() {
	local name="$1" size="$2" key="$3" iv="$4"
	head -c "$size" /dev/zero | openssl enc -aes-256-ctr -K "$key" -iv "$iv" -nosalt >"$name"
}

gen_file file1.bin 40000 "$(printf '11%.0s' $(seq 1 32))" "$(printf '11%.0s' $(seq 1 16))"
gen_file file2.bin 35000 "$(printf '22%.0s' $(seq 1 32))" "$(printf '22%.0s' $(seq 1 16))"
gen_file file3.bin 25000 "$(printf '33%.0s' $(seq 1 32))" "$(printf '33%.0s' $(seq 1 16))"

par2 create -s4096 -r20 -a fixture.par2 file1.bin file2.bin file3.bin

echo "Generated fixtures:"
ls -la file1.bin file2.bin file3.bin fixture.par2 fixture.vol*.par2
