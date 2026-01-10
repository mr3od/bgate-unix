#!/bin/bash
# bgate-unix v0.5.0 Final QA (Corrected)
set -e

# --- Config ---
TARGET="/Users/nexumind/Desktop/bgate-tests"
TEST_ROOT="./qa_workspace_final"
DB="$TEST_ROOT/test.db"
VAULT="$TEST_ROOT/vault"
INCOMING="$TEST_ROOT/incoming"

# --- Setup ---
echo "🧹 Cleaning workspace..."
rm -rf "$TEST_ROOT"
mkdir -p "$VAULT" "$INCOMING"
cp -R "$TARGET/"* "$INCOMING/" 2>/dev/null || true
FILE_COUNT_START=$(find "$INCOMING" -type f | wc -l)
echo "   -> Staged $FILE_COUNT_START files."

# --- 1. Safety Defaults ---
echo "🧪 [Test 1] Safety Default (Read-Only)"
uv run bgate scan "$INCOMING" --recursive --db "$DB" > /dev/null
COUNT_AFTER=$(find "$INCOMING" -type f | wc -l)
if [ "$COUNT_AFTER" -eq "$FILE_COUNT_START" ]; then
    echo "✅ PASS: No files moved."
else
    echo "❌ FAIL: Files moved in read-only mode!"
    exit 1
fi

# --- 2. Execution ---
echo "🧪 [Test 2] Execution (Move)"
# Clean DB to simulate fresh run for move
rm -f "$DB"

# Run move
uv run bgate scan "$INCOMING" --into "$VAULT" --recursive --move --db "$DB" > /dev/null

COUNT_INCOMING=$(find "$INCOMING" -type f | wc -l)
COUNT_VAULT=$(find "$VAULT" -type f | wc -l)
TOTAL_NOW=$((COUNT_INCOMING + COUNT_VAULT))

echo "   -> Left in Incoming: $COUNT_INCOMING (Duplicates)"
echo "   -> Moved to Vault:   $COUNT_VAULT (Unique)"

if [ "$TOTAL_NOW" -eq "$FILE_COUNT_START" ] && [ "$COUNT_VAULT" -gt 0 ]; then
    echo "✅ PASS: Conservation of mass verified. Files moved."
else
    echo "❌ FAIL: File count mismatch. Start: $FILE_COUNT_START, Now: $TOTAL_NOW"
    exit 1
fi

# --- 3. Idempotency ---
echo "🧪 [Test 3] Self-Scan (Idempotency)"
# Scan the vault. Should find exactly COUNT_VAULT uniques.
OUTPUT=$(uv run bgate scan "$VAULT" --recursive --db "$DB" --json)
UNIQUE_COUNT=$(echo "$OUTPUT" | grep -o '"unique": [0-9]*' | awk '{print $2}')
DUP_COUNT=$(echo "$OUTPUT" | grep -o '"duplicate": [0-9]*' | awk '{print $2}')

if [ "$UNIQUE_COUNT" -eq "$COUNT_VAULT" ] && [ "$DUP_COUNT" -eq 0 ]; then
     echo "✅ PASS: Vault self-scan correct."
else
     echo "❌ FAIL: Expected $COUNT_VAULT unique, got $UNIQUE_COUNT. Dupes: $DUP_COUNT"
     exit 1
fi

# --- 4. Metadata ---
echo "🧪 [Test 4] Metadata Tagging"
# Use a file from the Vault to create a duplicate in Incoming
TEST_FILE=$(find "$VAULT" -type f | head -n 1)
cp "$TEST_FILE" "$INCOMING/dup_test.file"

OUTPUT=$(uv run bgate scan "$INCOMING" --tag "source:qa" --json --db "$DB")
TAG_CHECK=$(echo "$OUTPUT" | grep '"source": "qa"')

if [ -n "$TAG_CHECK" ]; then
    echo "✅ PASS: Metadata tag found."
else
    echo "❌ FAIL: Metadata tag missing."
    exit 1
fi

# --- 5. Ignores ---
echo "🧪 [Test 5] Ignore Logic"
mkdir -p "$INCOMING/node_modules"
touch "$INCOMING/node_modules/junk.js"
rm -f "$INCOMING/dup_test.file" # Cleanup dup

# Create a real test file that should NOT be ignored
echo "test content" > "$INCOMING/should_scan.txt"

# We expect to scan 1 file (should_scan.txt), ignoring node_modules and .DS_Store files
OUTPUT=$(uv run bgate scan "$INCOMING" --recursive --json --db "$DB")
TOTAL=$(echo "$OUTPUT" | grep -o '"total": [0-9]*' | awk '{print $2}')

# Total should be 1 (our test file)
if [ "$TOTAL" -eq 1 ]; then
    echo "✅ PASS: Scanned 1 file, ignored node_modules and system files."
else
    echo "❌ FAIL: Expected 1 file, got $TOTAL"
    exit 1
fi

echo "🎉 ALL TESTS PASSED."