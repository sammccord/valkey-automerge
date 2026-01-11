#!/usr/bin/env bash
# Test JSON-based search indexing functionality
#
# NOTE: These tests require the valkey-json module to be loaded.
# If valkey-json is not available, the tests will be skipped.

set -euo pipefail

# Load common test utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

print_section "JSON-based Search Indexing"

# Check if valkey-json is available
echo "Checking for valkey-json module..."
if ! $VALKEY_CLI -h "$HOST" module list 2>/dev/null | grep -qi "json"; then
    echo "⚠️  valkey-json module not found - skipping JSON indexing tests"
    echo "   To run these tests, use the -full Docker image with valkey-json"
    echo ""
    echo "✅ JSON indexing tests skipped (valkey-json not available)"
    exit 0
fi
echo "   ✓ valkey-json module found"

# Test 1: Configure JSON indexing for a pattern
echo "Test 1: Configure JSON indexing with --format json..."
$VALKEY_CLI -h "$HOST" del jsondoc1 > /dev/null
$VALKEY_CLI -h "$HOST" am.new jsondoc1 > /dev/null
# Configure JSON indexing for product:* pattern
result=$($VALKEY_CLI -h "$HOST" am.index.configure "product:*" --format json title price description)
assert_equals "$result" "OK"
echo "   ✓ JSON index configuration created"

# Test 2: Verify JSON format is saved in configuration
echo "Test 2: Verify JSON format persisted..."
format=$($VALKEY_CLI -h "$HOST" hget "am:index:config:product:*" format)
assert_equals "$format" "json"
echo "   ✓ JSON format persisted correctly"

# Test 3: Automatic JSON document creation on PUTTEXT
echo "Test 3: Automatic JSON document creation..."
$VALKEY_CLI -h "$HOST" del "product:laptop" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "product:laptop" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "product:laptop" title "ThinkPad X1" > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "product:laptop" price 1299 > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "product:laptop" description "Professional laptop" > /dev/null
# Check that JSON document was created
type=$($VALKEY_CLI -h "$HOST" type "am:idx:product:laptop")
assert_equals "$type" "ReJSON-RL"
echo "   ✓ JSON document created"

# Test 4: Verify JSON structure and types
echo "Test 4: Verify JSON document structure and types..."
title=$($VALKEY_CLI -h "$HOST" --raw json.get "am:idx:product:laptop" $.title)
price=$($VALKEY_CLI -h "$HOST" --raw json.get "am:idx:product:laptop" $.price)
desc=$($VALKEY_CLI -h "$HOST" --raw json.get "am:idx:product:laptop" $.description)
# Verify values (JSON.GET returns JSON-formatted output)
echo "$title" | grep -q '"ThinkPad X1"'
echo "$price" | grep -q '1299'
echo "$desc" | grep -q '"Professional laptop"'
echo "   ✓ JSON document has correct structure and types"

# Test 5: Nested path indexing with JSON
echo "Test 5: Nested path JSON indexing..."
$VALKEY_CLI -h "$HOST" am.index.configure "book:*" --format json title author.name author.country price > /dev/null
$VALKEY_CLI -h "$HOST" del "book:rust101" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "book:rust101" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "book:rust101" title "Rust Programming" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "book:rust101" author.name "Steve Klabnik" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "book:rust101" author.country "USA" > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "book:rust101" price 39 > /dev/null
# Verify nested JSON structure
json_doc=$($VALKEY_CLI -h "$HOST" --raw json.get "am:idx:book:rust101" $)
echo "$json_doc" | jq -e '.[0].title == "Rust Programming"' > /dev/null
echo "$json_doc" | jq -e '.[0].author.name == "Steve Klabnik"' > /dev/null
echo "$json_doc" | jq -e '.[0].author.country == "USA"' > /dev/null
echo "$json_doc" | jq -e '.[0].price == 39' > /dev/null
echo "   ✓ Nested JSON structure preserved"

# Test 6: Array/List indexing with JSON
echo "Test 6: Array indexing with JSON format..."
$VALKEY_CLI -h "$HOST" am.index.configure "post:*" --format json title tags views > /dev/null
$VALKEY_CLI -h "$HOST" del "post:123" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "post:123" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "post:123" title "Introduction to CRDTs" > /dev/null
$VALKEY_CLI -h "$HOST" am.createlist "post:123" tags > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "post:123" tags "crdt" > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "post:123" tags "redis" > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "post:123" tags "distributed-systems" > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "post:123" views 1500 > /dev/null
# Verify array is indexed
json_doc=$($VALKEY_CLI -h "$HOST" --raw json.get "am:idx:post:123" $)
echo "$json_doc" | jq -e '.[0].title == "Introduction to CRDTs"' > /dev/null
echo "$json_doc" | jq -e '.[0].tags | length == 3' > /dev/null
echo "$json_doc" | jq -e '.[0].tags[0] == "crdt"' > /dev/null
echo "$json_doc" | jq -e '.[0].tags[1] == "redis"' > /dev/null
echo "$json_doc" | jq -e '.[0].tags[2] == "distributed-systems"' > /dev/null
echo "$json_doc" | jq -e '.[0].views == 1500' > /dev/null
echo "   ✓ Arrays indexed correctly in JSON"

# Test 7: Type preservation (bool, double, int)
echo "Test 7: Type preservation in JSON..."
$VALKEY_CLI -h "$HOST" am.index.configure "config:*" --format json enabled timeout maxRetries rate > /dev/null
$VALKEY_CLI -h "$HOST" del "config:api" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "config:api" > /dev/null
$VALKEY_CLI -h "$HOST" am.putbool "config:api" enabled true > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "config:api" timeout 30 > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "config:api" maxRetries 5 > /dev/null
$VALKEY_CLI -h "$HOST" am.putdouble "config:api" rate 0.75 > /dev/null
# Verify types are preserved
json_doc=$($VALKEY_CLI -h "$HOST" --raw json.get "am:idx:config:api" $)
echo "$json_doc" | jq -e '.[0].enabled == true' > /dev/null
echo "$json_doc" | jq -e '.[0].timeout == 30' > /dev/null
echo "$json_doc" | jq -e '.[0].maxRetries == 5' > /dev/null
echo "$json_doc" | jq -e '.[0].rate == 0.75' > /dev/null
echo "   ✓ Types preserved in JSON (bool, int, double)"

# Test 8: JSON document update
echo "Test 8: JSON document automatic update..."
$VALKEY_CLI -h "$HOST" am.puttext "product:laptop" title "ThinkPad X1 Carbon" > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "product:laptop" price 1499 > /dev/null
# Verify updated values
title=$($VALKEY_CLI -h "$HOST" --raw json.get "am:idx:product:laptop" $.title)
price=$($VALKEY_CLI -h "$HOST" --raw json.get "am:idx:product:laptop" $.price)
echo "$title" | grep -q '"ThinkPad X1 Carbon"'
echo "$price" | grep -q '1499'
echo "   ✓ JSON document updated automatically"

# Test 9: FROMJSON with JSON indexing
echo "Test 9: FROMJSON triggers JSON indexing..."
$VALKEY_CLI -h "$HOST" del "product:phone" > /dev/null
$VALKEY_CLI -h "$HOST" am.fromjson "product:phone" '{"title":"iPhone 15","price":999,"description":"Latest smartphone"}' > /dev/null
# Verify JSON index created
type=$($VALKEY_CLI -h "$HOST" type "am:idx:product:phone")
assert_equals "$type" "ReJSON-RL"
json_doc=$($VALKEY_CLI -h "$HOST" --raw json.get "am:idx:product:phone" $)
echo "$json_doc" | jq -e '.[0].title == "iPhone 15"' > /dev/null
echo "$json_doc" | jq -e '.[0].price == 999' > /dev/null
echo "$json_doc" | jq -e '.[0].description == "Latest smartphone"' > /dev/null
echo "   ✓ FROMJSON triggers JSON indexing"

# Test 10: AM.INDEX.DISABLE with JSON format
echo "Test 10: AM.INDEX.DISABLE with JSON format..."
result=$($VALKEY_CLI -h "$HOST" am.index.disable "product:*")
assert_equals "$result" "OK"
# Update document - JSON should NOT update
$VALKEY_CLI -h "$HOST" am.puttext "product:laptop" title "Should Not Update" > /dev/null
title=$($VALKEY_CLI -h "$HOST" --raw json.get "am:idx:product:laptop" $.title)
echo "$title" | grep -q '"ThinkPad X1 Carbon"'  # Should still be old value
echo "   ✓ Disabled JSON index does not update"

# Test 11: AM.INDEX.ENABLE and REINDEX with JSON
echo "Test 11: AM.INDEX.ENABLE and REINDEX with JSON format..."
result=$($VALKEY_CLI -h "$HOST" am.index.enable "product:*")
assert_equals "$result" "OK"
result=$($VALKEY_CLI -h "$HOST" am.index.reindex "product:laptop")
assert_equals "$result" "1"
# Now it should have the updated value
title=$($VALKEY_CLI -h "$HOST" --raw json.get "am:idx:product:laptop" $.title)
echo "$title" | grep -q '"Should Not Update"'
echo "   ✓ REINDEX rebuilds JSON document"

# Test 12: Mixed Hash and JSON configurations
echo "Test 12: Mixed Hash and JSON configurations..."
# Create Hash-based config
$VALKEY_CLI -h "$HOST" am.index.configure "user:*" name email > /dev/null
# Create JSON-based config
$VALKEY_CLI -h "$HOST" am.index.configure "profile:*" --format json username bio preferences.theme > /dev/null
# Test Hash indexing
$VALKEY_CLI -h "$HOST" del "user:alice" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "user:alice" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "user:alice" name "Alice" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "user:alice" email "alice@example.com" > /dev/null
# Test JSON indexing
$VALKEY_CLI -h "$HOST" del "profile:bob" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "profile:bob" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "profile:bob" username "bob123" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "profile:bob" bio "Software engineer" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "profile:bob" preferences.theme "dark" > /dev/null
# Verify Hash index
hash_type=$($VALKEY_CLI -h "$HOST" type "am:idx:user:alice")
assert_equals "$hash_type" "hash"
# Verify JSON index
json_type=$($VALKEY_CLI -h "$HOST" type "am:idx:profile:bob")
assert_equals "$json_type" "ReJSON-RL"
echo "   ✓ Hash and JSON configurations coexist"

# Test 13: Non-matching pattern with JSON format
echo "Test 13: Non-matching pattern doesn't create JSON index..."
$VALKEY_CLI -h "$HOST" del "item:xyz" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "item:xyz" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "item:xyz" title "No Index" > /dev/null
# Should not create index (no config for item:* pattern)
exists=$($VALKEY_CLI -h "$HOST" exists "am:idx:item:xyz")
assert_equals "$exists" "0"
echo "   ✓ Non-matching patterns don't create JSON indexes"

# Test 14: Empty fields not indexed in JSON
echo "Test 14: Missing configured fields in JSON..."
$VALKEY_CLI -h "$HOST" del "product:tablet" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "product:tablet" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "product:tablet" title "iPad Pro" > /dev/null
# price and description are not set
json_doc=$($VALKEY_CLI -h "$HOST" --raw json.get "am:idx:product:tablet" $)
# Should only have title field
echo "$json_doc" | jq -e '.[0].title == "iPad Pro"' > /dev/null
echo "$json_doc" | jq -e '.[0] | has("price") | not' > /dev/null
echo "$json_doc" | jq -e '.[0] | has("description") | not' > /dev/null
echo "   ✓ Missing fields not included in JSON document"

# Test 15: Complex nested structure with JSON
echo "Test 15: Complex nested JSON structure..."
$VALKEY_CLI -h "$HOST" am.index.configure "service:*" --format json name config.host config.port config.ssl metadata.version metadata.tags > /dev/null
$VALKEY_CLI -h "$HOST" del "service:api" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "service:api" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "service:api" name "API Gateway" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "service:api" config.host "api.example.com" > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "service:api" config.port 443 > /dev/null
$VALKEY_CLI -h "$HOST" am.putbool "service:api" config.ssl true > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "service:api" metadata.version "2.0.1" > /dev/null
$VALKEY_CLI -h "$HOST" am.createlist "service:api" metadata.tags > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "service:api" metadata.tags "production" > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "service:api" metadata.tags "critical" > /dev/null
# Verify complex nested structure
json_doc=$($VALKEY_CLI -h "$HOST" --raw json.get "am:idx:service:api" $)
echo "$json_doc" | jq -e '.[0].name == "API Gateway"' > /dev/null
echo "$json_doc" | jq -e '.[0].config.host == "api.example.com"' > /dev/null
echo "$json_doc" | jq -e '.[0].config.port == 443' > /dev/null
echo "$json_doc" | jq -e '.[0].config.ssl == true' > /dev/null
echo "$json_doc" | jq -e '.[0].metadata.version == "2.0.1"' > /dev/null
echo "$json_doc" | jq -e '.[0].metadata.tags | length == 2' > /dev/null
echo "$json_doc" | jq -e '.[0].metadata.tags[0] == "production"' > /dev/null
echo "$json_doc" | jq -e '.[0].metadata.tags[1] == "critical"' > /dev/null
echo "   ✓ Complex nested JSON structure works correctly"

# Test 16: FT.CREATE and FT.SEARCH with JSON - Array search
echo "Test 16: Valkey-Search JSON integration - Array/tag search..."
# Check if search module is available
if ! $VALKEY_CLI -h "$HOST" module list 2>/dev/null | grep -qi "search"; then
    echo "   ⚠️  search module not found - skipping FT.SEARCH tests"
    echo "   To run these tests, use the -full Docker image with search module"
    echo ""
    echo "✅ All JSON-based indexing tests passed (FT.SEARCH tests skipped)!"
    exit 0
fi
# Clean up any existing index
$VALKEY_CLI -h "$HOST" ft.dropindex idx:test_products 2>/dev/null || true
# Create test products with different tags and prices
$VALKEY_CLI -h "$HOST" del "product:json1" "product:json2" "product:json3" "product:json4" "product:json5" > /dev/null
$VALKEY_CLI -h "$HOST" am.index.configure "product:*" --format json title price inStock tags category > /dev/null
# Product 1: Laptop with business tags
$VALKEY_CLI -h "$HOST" am.new "product:json1" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "product:json1" title "ThinkPad X1" > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "product:json1" price 1299 > /dev/null
$VALKEY_CLI -h "$HOST" am.putbool "product:json1" inStock true > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "product:json1" category "laptop" > /dev/null
$VALKEY_CLI -h "$HOST" am.createlist "product:json1" tags > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "product:json1" tags "business" > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "product:json1" tags "portable" > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "product:json1" tags "premium" > /dev/null
# Product 2: Gaming laptop
$VALKEY_CLI -h "$HOST" am.new "product:json2" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "product:json2" title "Gaming Laptop ROG" > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "product:json2" price 1899 > /dev/null
$VALKEY_CLI -h "$HOST" am.putbool "product:json2" inStock true > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "product:json2" category "laptop" > /dev/null
$VALKEY_CLI -h "$HOST" am.createlist "product:json2" tags > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "product:json2" tags "gaming" > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "product:json2" tags "performance" > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "product:json2" tags "premium" > /dev/null
# Product 3: Budget laptop
$VALKEY_CLI -h "$HOST" am.new "product:json3" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "product:json3" title "Budget Chromebook" > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "product:json3" price 299 > /dev/null
$VALKEY_CLI -h "$HOST" am.putbool "product:json3" inStock false > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "product:json3" category "laptop" > /dev/null
$VALKEY_CLI -h "$HOST" am.createlist "product:json3" tags > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "product:json3" tags "budget" > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "product:json3" tags "portable" > /dev/null
# Product 4: Smartphone
$VALKEY_CLI -h "$HOST" am.new "product:json4" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "product:json4" title "iPhone 15 Pro" > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "product:json4" price 999 > /dev/null
$VALKEY_CLI -h "$HOST" am.putbool "product:json4" inStock true > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "product:json4" category "phone" > /dev/null
$VALKEY_CLI -h "$HOST" am.createlist "product:json4" tags > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "product:json4" tags "premium" > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "product:json4" tags "5G" > /dev/null
# Product 5: Tablet
$VALKEY_CLI -h "$HOST" am.new "product:json5" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "product:json5" title "iPad Pro" > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "product:json5" price 799 > /dev/null
$VALKEY_CLI -h "$HOST" am.putbool "product:json5" inStock true > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "product:json5" category "tablet" > /dev/null
$VALKEY_CLI -h "$HOST" am.createlist "product:json5" tags > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "product:json5" tags "portable" > /dev/null
$VALKEY_CLI -h "$HOST" am.appendtext "product:json5" tags "creative" > /dev/null
# Reindex all products to ensure putint/putbool fields are in JSON indexes
$VALKEY_CLI -h "$HOST" am.index.reindex "product:json1" > /dev/null
$VALKEY_CLI -h "$HOST" am.index.reindex "product:json2" > /dev/null
$VALKEY_CLI -h "$HOST" am.index.reindex "product:json3" > /dev/null
$VALKEY_CLI -h "$HOST" am.index.reindex "product:json4" > /dev/null
$VALKEY_CLI -h "$HOST" am.index.reindex "product:json5" > /dev/null
# Create valkey-search index on JSON (using TAG instead of TEXT)
$VALKEY_CLI -h "$HOST" ft.create idx:test_products on json prefix 1 am:idx:product:json schema \
  '$.title' as title TAG \
  '$.price' as price NUMERIC \
  '$.inStock' as inStock TAG \
  '$.tags[*]' as tags TAG \
  '$.category' as category TAG > /dev/null
# Wait for backfill to complete
sleep 1
# Search for laptops by category - should find json1, json2, json3
results=$($VALKEY_CLI -h "$HOST" ft.search idx:test_products "@category:{laptop}" nocontent)
echo "$results" | grep -q "am:idx:product:json1"
echo "$results" | grep -q "am:idx:product:json2"
echo "$results" | grep -q "am:idx:product:json3"
# Verify phone and tablet are excluded
if echo "$results" | grep -q "am:idx:product:json4"; then
    echo "   ✗ Category search incorrectly included json4 (phone)"
    exit 1
fi
if echo "$results" | grep -q "am:idx:product:json5"; then
    echo "   ✗ Category search incorrectly included json5 (tablet)"
    exit 1
fi
echo "   ✓ Category TAG search correctly filters by category"

# Test 17: FT.SEARCH with JSON - Numeric range query
echo "Test 17: Valkey-Search JSON integration - Numeric range query..."
# Search for products between $500 and $1500 - should find json1 (1299), json4 (999), json5 (799)
results=$($VALKEY_CLI -h "$HOST" ft.search idx:test_products "@price:[500 1500]" nocontent)
echo "$results" | grep -q "am:idx:product:json1"
echo "$results" | grep -q "am:idx:product:json4"
echo "$results" | grep -q "am:idx:product:json5"
# Verify budget (299) and expensive gaming (1899) are excluded
if echo "$results" | grep -q "am:idx:product:json3"; then
    echo "   ✗ Numeric range incorrectly included json3 (299)"
    exit 1
fi
if echo "$results" | grep -q "am:idx:product:json2"; then
    echo "   ✗ Numeric range incorrectly included json2 (1899)"
    exit 1
fi
echo "   ✓ Numeric range query correctly filters by price"

# Test 18: FT.SEARCH with JSON - Boolean field query
echo "Test 18: Valkey-Search JSON integration - Boolean field query..."
# Search for in-stock products - should find json1, json2, json4, json5 (not json3)
results=$($VALKEY_CLI -h "$HOST" ft.search idx:test_products "@inStock:{true}" nocontent)
echo "$results" | grep -q "am:idx:product:json1"
echo "$results" | grep -q "am:idx:product:json2"
echo "$results" | grep -q "am:idx:product:json4"
echo "$results" | grep -q "am:idx:product:json5"
# Verify out-of-stock Chromebook is excluded
if echo "$results" | grep -q "am:idx:product:json3"; then
    echo "   ✗ Boolean search incorrectly included json3 (out of stock)"
    exit 1
fi
echo "   ✓ Boolean field query correctly filters by stock status"

# Test 19: FT.SEARCH with JSON - Category filter
echo "Test 19: Valkey-Search JSON integration - Category tag filter..."
# Search for laptops only - should find json1, json2, json3
results=$($VALKEY_CLI -h "$HOST" ft.search idx:test_products "@category:{laptop}" nocontent)
echo "$results" | grep -q "am:idx:product:json1"
echo "$results" | grep -q "am:idx:product:json2"
echo "$results" | grep -q "am:idx:product:json3"
# Verify phone and tablet are excluded
if echo "$results" | grep -q "am:idx:product:json4"; then
    echo "   ✗ Category filter incorrectly included json4 (phone)"
    exit 1
fi
if echo "$results" | grep -q "am:idx:product:json5"; then
    echo "   ✗ Category filter incorrectly included json5 (tablet)"
    exit 1
fi
echo "   ✓ Category filter correctly limits to laptops"

# Test 20: FT.SEARCH with JSON - Combined complex query
echo "Test 20: Valkey-Search JSON integration - Combined query..."
# Search for: laptop + in stock + price under $1500
# Should find: json1 (1299, laptop, in stock)
# Exclude: json2 (1899, too expensive), json3 (out of stock), json4 (phone), json5 (tablet)
results=$($VALKEY_CLI -h "$HOST" ft.search idx:test_products "@category:{laptop} @inStock:{true} @price:[0 1500]" nocontent)
echo "$results" | grep -q "am:idx:product:json1"
# Verify exclusions
if echo "$results" | grep -q "am:idx:product:json2"; then
    echo "   ✗ Combined query incorrectly included json2 (price too high: 1899)"
    exit 1
fi
if echo "$results" | grep -q "am:idx:product:json3"; then
    echo "   ✗ Combined query incorrectly included json3 (out of stock)"
    exit 1
fi
if echo "$results" | grep -q "am:idx:product:json4"; then
    echo "   ✗ Combined query incorrectly included json4 (phone, not laptop)"
    exit 1
fi
if echo "$results" | grep -q "am:idx:product:json5"; then
    echo "   ✗ Combined query incorrectly included json5 (tablet, not laptop)"
    exit 1
fi
echo "   ✓ Combined query correctly applies multiple filters"

# Test 21: FT.SEARCH with JSON - Title TAG search (exact match)
echo "Test 21: Valkey-Search JSON integration - Title TAG search..."
# Search for exact title match - should find only json1
results=$($VALKEY_CLI -h "$HOST" ft.search idx:test_products "@title:{ThinkPad X1}" nocontent)
echo "$results" | grep -q "am:idx:product:json1"
# Verify others are excluded
if echo "$results" | grep -q "am:idx:product:json2"; then
    echo "   ✗ Title search incorrectly included json2"
    exit 1
fi
if echo "$results" | grep -q "am:idx:product:json3"; then
    echo "   ✗ Title search incorrectly included json3"
    exit 1
fi
if echo "$results" | grep -q "am:idx:product:json4"; then
    echo "   ✗ Title search incorrectly included json4"
    exit 1
fi
if echo "$results" | grep -q "am:idx:product:json5"; then
    echo "   ✗ Title search incorrectly included json5"
    exit 1
fi
echo "   ✓ Title TAG search correctly finds exact match"

# Test 22: FT.SEARCH with JSON - Multi-field filter search
echo "Test 22: Valkey-Search JSON integration - Multi-field filter..."
# Search for laptops under $500 - should only find json3 (Budget Chromebook at 299)
results=$($VALKEY_CLI -h "$HOST" ft.search idx:test_products "@category:{laptop} @price:[0 500]" nocontent)
echo "$results" | grep -q "am:idx:product:json3"
# Verify others are excluded
if echo "$results" | grep -q "am:idx:product:json1"; then
    echo "   ✗ Multi-field search incorrectly included json1 (price 1299)"
    exit 1
fi
if echo "$results" | grep -q "am:idx:product:json2"; then
    echo "   ✗ Multi-field search incorrectly included json2 (price 1899)"
    exit 1
fi
if echo "$results" | grep -q "am:idx:product:json4"; then
    echo "   ✗ Multi-field search incorrectly included json4 (phone)"
    exit 1
fi
if echo "$results" | grep -q "am:idx:product:json5"; then
    echo "   ✗ Multi-field search incorrectly included json5 (tablet)"
    exit 1
fi
echo "   ✓ Multi-field filter correctly applies all criteria"

# Test 23: FT.SEARCH with JSON - Nested path queries
echo "Test 23: Valkey-Search JSON integration - Nested paths..."
# Use the book example with nested author structure
$VALKEY_CLI -h "$HOST" ft.dropindex idx:test_books 2>/dev/null || true
$VALKEY_CLI -h "$HOST" del "book:json1" "book:json2" "book:json3" > /dev/null
$VALKEY_CLI -h "$HOST" am.index.configure "book:*" --format json title author.name author.country price > /dev/null
# Book 1: USA author
$VALKEY_CLI -h "$HOST" am.new "book:json1" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "book:json1" title "The Rust Programming Language" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "book:json1" author.name "Steve Klabnik" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "book:json1" author.country "USA" > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "book:json1" price 39 > /dev/null
# Book 2: UK author
$VALKEY_CLI -h "$HOST" am.new "book:json2" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "book:json2" title "Clean Code" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "book:json2" author.name "Robert Martin" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "book:json2" author.country "UK" > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "book:json2" price 45 > /dev/null
# Book 3: Another USA author
$VALKEY_CLI -h "$HOST" am.new "book:json3" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "book:json3" title "Design Patterns" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "book:json3" author.name "Gang of Four" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "book:json3" author.country "USA" > /dev/null
$VALKEY_CLI -h "$HOST" am.putint "book:json3" price 55 > /dev/null
# Reindex all books to ensure putint fields are in JSON indexes
$VALKEY_CLI -h "$HOST" am.index.reindex "book:json1" > /dev/null
$VALKEY_CLI -h "$HOST" am.index.reindex "book:json2" > /dev/null
$VALKEY_CLI -h "$HOST" am.index.reindex "book:json3" > /dev/null
# Create index with nested paths (using TAG instead of TEXT)
$VALKEY_CLI -h "$HOST" ft.create idx:test_books on json prefix 1 am:idx:book:json schema \
  '$.title' as title TAG \
  '$.author.name' as author TAG \
  '$.author.country' as country TAG \
  '$.price' as price NUMERIC > /dev/null
# Wait for backfill to complete
sleep 1
# Search for USA authors - should find json1 and json3
results=$($VALKEY_CLI -h "$HOST" ft.search idx:test_books "@country:{USA}" nocontent)
echo "$results" | grep -q "am:idx:book:json1"
echo "$results" | grep -q "am:idx:book:json3"
# Verify UK author is excluded
if echo "$results" | grep -q "am:idx:book:json2"; then
    echo "   ✗ Nested path search incorrectly included json2 (UK author)"
    exit 1
fi
echo "   ✓ Nested path queries work correctly"

# Test 24: FT.SEARCH with JSON - Combined nested path and numeric
echo "Test 24: Valkey-Search JSON integration - Nested path + numeric range..."
# Search for USA books under $50 - should only find json1 (39)
results=$($VALKEY_CLI -h "$HOST" ft.search idx:test_books "@country:{USA} @price:[0 50]" nocontent)
echo "$results" | grep -q "am:idx:book:json1"
# Verify json3 (USA but 55) and json2 (UK) are excluded
if echo "$results" | grep -q "am:idx:book:json3"; then
    echo "   ✗ Combined nested query incorrectly included json3 (price 55)"
    exit 1
fi
if echo "$results" | grep -q "am:idx:book:json2"; then
    echo "   ✗ Combined nested query incorrectly included json2 (UK)"
    exit 1
fi
echo "   ✓ Combined nested path and numeric queries work correctly"

# Cleanup
$VALKEY_CLI -h "$HOST" ft.dropindex idx:test_products > /dev/null 2>&1 || true
$VALKEY_CLI -h "$HOST" ft.dropindex idx:test_books > /dev/null 2>&1 || true

echo ""
echo "✅ All JSON-based indexing tests passed!"
