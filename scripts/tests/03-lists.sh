#!/usr/bin/env bash
# Test list/array operations

set -euo pipefail

# Load common test utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

print_section "List Operations"

# Test array/list operations
echo "Test 1: List creation and append operations..."
$VALKEY_CLI -h "$HOST" del doc6 > /dev/null
$VALKEY_CLI -h "$HOST" am.new doc6 > /dev/null
$VALKEY_CLI -h "$HOST" am.createlist doc6 users > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext doc6 users "Alice" > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext doc6 users "Bob" > /dev/null
len=$($VALKEY_CLI -h "$HOST" am.listlen doc6 users)
assert_equals "$len" "2"
echo "   ✓ List creation and append works"

# Test array index access
echo "Test 2: Array index access..."
val1=$($VALKEY_CLI -h "$HOST" --raw am.gettext doc6 'users[0]')
val2=$($VALKEY_CLI -h "$HOST" --raw am.gettext doc6 'users[1]')
assert_equals "$val1" "Alice"
assert_equals "$val2" "Bob"
echo "   ✓ Array index access works"

# Test different types in lists
echo "Test 3: Different types in lists..."
$VALKEY_CLI -h "$HOST" am.createlist doc6 ages > /dev/null
$VALKEY_CLI -h "$HOST" am.appendint doc6 ages 25 > /dev/null
$VALKEY_CLI -h "$HOST" am.appendint doc6 ages 30 > /dev/null
age1=$($VALKEY_CLI -h "$HOST" am.getint doc6 'ages[0]')
age2=$($VALKEY_CLI -h "$HOST" am.getint doc6 'ages[1]')
assert_equals "$age1" "25"
assert_equals "$age2" "30"
echo "   ✓ Different types in lists work"

# Test nested list paths
echo "Test 4: Nested list paths..."
$VALKEY_CLI -h "$HOST" del doc7 > /dev/null
$VALKEY_CLI -h "$HOST" am.new doc7 > /dev/null
$VALKEY_CLI -h "$HOST" am.createlist doc7 data.items > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext doc7 data.items "item1" > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext doc7 data.items "item2" > /dev/null
item1=$($VALKEY_CLI -h "$HOST" --raw am.gettext doc7 'data.items[0]')
item2=$($VALKEY_CLI -h "$HOST" --raw am.gettext doc7 'data.items[1]')
assert_equals "$item1" "item1"
assert_equals "$item2" "item2"
echo "   ✓ Nested list paths work"

# Test list persistence
echo "Test 5: List persistence..."
$VALKEY_CLI -h "$HOST" --raw am.save doc6 > /tmp/list-saved.bin
truncate -s -1 /tmp/list-saved.bin
$VALKEY_CLI -h "$HOST" del doc6 > /dev/null
$VALKEY_CLI -h "$HOST" --raw -x am.load doc6 < /tmp/list-saved.bin > /dev/null
len=$($VALKEY_CLI -h "$HOST" am.listlen doc6 users)
val1=$($VALKEY_CLI -h "$HOST" --raw am.gettext doc6 'users[0]')
val2=$($VALKEY_CLI -h "$HOST" --raw am.gettext doc6 'users[1]')
assert_equals "$len" "2"
assert_equals "$val1" "Alice"
assert_equals "$val2" "Bob"
echo "   ✓ List persistence works"

rm -f /tmp/list-saved.bin

echo ""
echo "✅ All list operation tests passed!"
