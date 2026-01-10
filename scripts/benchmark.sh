#!/bin/bash
# bgate-unix Move Operation Benchmark with Idempotency Test
# Usage: ./benchmark.sh <source_dir> <vault_dir> [db_path]

set -e

if [ $# -lt 2 ]; then
    echo "Usage: $0 <source_dir> <vault_dir> [db_path]"
    echo "Example: $0 rakhys-pipeline/data/ /tmp/vault/"
    exit 1
fi

SOURCE_DIR="$1"
VAULT_DIR="$2"
DB_PATH="${3:-benchmark.db}"

echo "🚀 bgate-unix Move Operation Benchmark"
echo "=================================================="
echo "Source: $SOURCE_DIR"
echo "Vault:  $VAULT_DIR"
echo "DB:     $DB_PATH"
echo

# Function to run benchmark and collect metrics
run_benchmark() {
    local run_name="$1"
    local clean_start="$2"
    
    echo "🔥 Running $run_name..."
    
    if [ "$clean_start" = "true" ]; then
        echo "🧹 Cleaning up previous runs..."
        rm -rf "$VAULT_DIR"
        rm -f "$DB_PATH"
        mkdir -p "$VAULT_DIR"
    fi
    
    # Analyze source data
    echo "📊 Analyzing source data..."
    TOTAL_FILES=$(find "$SOURCE_DIR" -type f | wc -l)
    TOTAL_SIZE=$(du -sb "$SOURCE_DIR" | cut -f1)
    TOTAL_GB=$(echo "scale=2; $TOTAL_SIZE / 1024 / 1024 / 1024" | bc)
    
    echo "   Files: $(printf "%'d" $TOTAL_FILES)"
    echo "   Size:  ${TOTAL_GB} GB"
    echo
    
    echo "Command: bgate scan $SOURCE_DIR --into $VAULT_DIR --recursive --move --db $DB_PATH"
    echo
    
    START_TIME=$(date +%s.%N)
    bgate scan "$SOURCE_DIR" --into "$VAULT_DIR" --recursive --move --db "$DB_PATH"
    END_TIME=$(date +%s.%N)
    
    ELAPSED=$(echo "$END_TIME - $START_TIME" | bc)
    
    echo
    echo "📈 Analyzing results..."
    
    # Count moved files and size
    MOVED_FILES=$(find "$VAULT_DIR" -type f 2>/dev/null | wc -l)
    MOVED_SIZE=$(du -sb "$VAULT_DIR" 2>/dev/null | cut -f1 || echo "0")
    MOVED_GB=$(echo "scale=2; $MOVED_SIZE / 1024 / 1024 / 1024" | bc)
    
    # Count remaining files (duplicates)
    REMAINING_FILES=$(find "$SOURCE_DIR" -type f | wc -l)
    
    echo
    echo "=================================================="
    echo "📈 $run_name RESULTS"
    echo "=================================================="
    printf "Runtime: %.2f seconds\n" "$ELAPSED"
    echo
    echo "Files:"
    printf "  Total:      %'d\n" "$TOTAL_FILES"
    printf "  Moved:      %'d (unique)\n" "$MOVED_FILES"
    printf "  Duplicates: %'d\n" "$REMAINING_FILES"
    echo
    echo "Data:"
    echo "  Total Size: ${TOTAL_GB} GB"
    echo "  Moved Size: ${MOVED_GB} GB"
    
    if [ "$ELAPSED" != "0" ] && [ "$MOVED_SIZE" != "0" ]; then
        # Calculate bandwidth (MB/sec)
        MOVED_MB=$(echo "scale=2; $MOVED_SIZE / 1024 / 1024" | bc)
        BANDWIDTH=$(echo "scale=1; $MOVED_MB / $ELAPSED" | bc)
        echo "  Bandwidth:  ${BANDWIDTH} MB/sec"
        
        # Calculate throughput
        FILES_PER_SEC=$(echo "scale=1; $MOVED_FILES / $ELAPSED" | bc)
        TOTAL_PER_SEC=$(echo "scale=1; $TOTAL_FILES / $ELAPSED" | bc)
        echo
        echo "Throughput:"
        echo "  Files/sec:  ${FILES_PER_SEC} (moved)"
        echo "  Total/sec:  ${TOTAL_PER_SEC} (including duplicates)"
        
        # Deduplication analysis
        if [ "$TOTAL_FILES" -gt 0 ]; then
            DEDUP_RATIO=$(echo "scale=1; $REMAINING_FILES * 100 / $TOTAL_FILES" | bc)
            echo "  Deduplication: ${DEDUP_RATIO}% duplicates found"
        fi
    else
        echo "  No files moved (idempotent behavior)"
    fi
    
    echo
}

# Run initial benchmark (clean start)
run_benchmark "FIRST RUN (Clean Start)" "true"

echo "⏱️  Waiting 2 seconds before idempotency test..."
sleep 2

# Run idempotency test (no cleanup)
run_benchmark "IDEMPOTENCY TEST (No Cleanup)" "false"

echo "🎯 Idempotency Analysis:"
if [ "$MOVED_FILES" -eq 0 ]; then
    echo "  ✅ PASS: No files moved on second run (perfect idempotency)"
else
    echo "  ⚠️  WARNING: $MOVED_FILES files moved on second run"
    echo "     This may indicate new files were added or database issues"
fi

echo
echo "🎉 Benchmark complete!"