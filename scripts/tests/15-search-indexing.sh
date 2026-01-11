#!/usr/bin/env bash
# Test search indexing functionality

set -euo pipefail

# Load common test utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

print_section "Search Indexing"

# Test 1: Configure indexing for a pattern
echo "Test 1: Configure indexing for a pattern..."
$VALKEY_CLI -h "$HOST" del searchdoc1 > /dev/null
$VALKEY_CLI -h "$HOST" am.new searchdoc1 > /dev/null
# Configure indexing for article:* pattern with title and content paths
result=$($VALKEY_CLI -h "$HOST" am.index.configure "article:*" title content)
assert_equals "$result" "OK"
echo "   ✓ Index configuration created"

# Test 2: Verify configuration is saved
echo "Test 2: Verify configuration is saved..."
# Check that configuration key exists
exists=$($VALKEY_CLI -h "$HOST" exists "am:index:config:article:*")
assert_equals "$exists" "1"
# Check enabled field
enabled=$($VALKEY_CLI -h "$HOST" hget "am:index:config:article:*" enabled)
assert_equals "$enabled" "1"
# Check paths field
paths=$($VALKEY_CLI -h "$HOST" hget "am:index:config:article:*" paths)
assert_equals "$paths" "title,content"
echo "   ✓ Configuration persisted correctly"

# Test 3: Automatic index creation on PUTTEXT
echo "Test 3: Automatic index creation on PUTTEXT..."
$VALKEY_CLI -h "$HOST" del "article:123" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "article:123" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:123" title "Redis and Automerge" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:123" content "A guide to CRDTs in Redis" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:123" author "John Doe" > /dev/null
# Check that shadow Hash was created
exists=$($VALKEY_CLI -h "$HOST" exists "am:idx:article:123")
assert_equals "$exists" "1"
# Check indexed fields are present
title=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:article:123" title)
content=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:article:123" content)
assert_equals "$title" "Redis and Automerge"
assert_equals "$content" "A guide to CRDTs in Redis"
# Check that non-configured field (author) is NOT indexed
author=$($VALKEY_CLI -h "$HOST" hget "am:idx:article:123" author)
assert_equals "$author" ""
echo "   ✓ Shadow Hash created with configured fields only"

# Test 4: Automatic index update on field modification
echo "Test 4: Automatic index update on field modification..."
$VALKEY_CLI -h "$HOST" am.puttext "article:123" title "Updated Title" > /dev/null
title=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:article:123" title)
assert_equals "$title" "Updated Title"
echo "   ✓ Shadow Hash updated on field modification"

# Test 5: Nested path indexing
echo "Test 5: Nested path indexing..."
$VALKEY_CLI -h "$HOST" am.index.configure "user:*" name profile.bio profile.location > /dev/null
$VALKEY_CLI -h "$HOST" del "user:alice" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "user:alice" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "user:alice" name "Alice" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "user:alice" profile.bio "Software engineer" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "user:alice" profile.location "San Francisco" > /dev/null
# Check shadow Hash
exists=$($VALKEY_CLI -h "$HOST" exists "am:idx:user:alice")
assert_equals "$exists" "1"
name=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:user:alice" name)
bio=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:user:alice" profile_bio)
location=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:user:alice" profile_location)
assert_equals "$name" "Alice"
assert_equals "$bio" "Software engineer"
assert_equals "$location" "San Francisco"
echo "   ✓ Nested paths indexed correctly (dots replaced with underscores)"

# Test 6: AM.INDEX.DISABLE command
echo "Test 6: AM.INDEX.DISABLE command..."
result=$($VALKEY_CLI -h "$HOST" am.index.disable "user:*")
assert_equals "$result" "OK"
# Check that enabled field is now 0
enabled=$($VALKEY_CLI -h "$HOST" hget "am:index:config:user:*" enabled)
assert_equals "$enabled" "0"
# Update document - shadow Hash should NOT update
$VALKEY_CLI -h "$HOST" am.puttext "user:alice" name "Alice Updated" > /dev/null
name=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:user:alice" name)
assert_equals "$name" "Alice"  # Should still be old value
echo "   ✓ Disabled index does not update"

# Test 7: AM.INDEX.ENABLE command
echo "Test 7: AM.INDEX.ENABLE command..."
result=$($VALKEY_CLI -h "$HOST" am.index.enable "user:*")
assert_equals "$result" "OK"
# Check that enabled field is now 1
enabled=$($VALKEY_CLI -h "$HOST" hget "am:index:config:user:*" enabled)
assert_equals "$enabled" "1"
# Update document - shadow Hash should now update
$VALKEY_CLI -h "$HOST" am.puttext "user:alice" name "Alice Re-enabled" > /dev/null
name=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:user:alice" name)
assert_equals "$name" "Alice Re-enabled"
echo "   ✓ Re-enabled index updates correctly"

# Test 8: AM.INDEX.REINDEX command
echo "Test 8: AM.INDEX.REINDEX command..."
# Create document without index, then configure and reindex
$VALKEY_CLI -h "$HOST" del "article:456" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "article:456" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:456" title "Pre-index Article" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:456" content "Created before indexing" > /dev/null
# Disable indexing temporarily
$VALKEY_CLI -h "$HOST" am.index.disable "article:*" > /dev/null
# Update the document (should not create shadow Hash)
$VALKEY_CLI -h "$HOST" am.puttext "article:456" title "Updated Title" > /dev/null
# Re-enable and reindex
$VALKEY_CLI -h "$HOST" am.index.enable "article:*" > /dev/null
result=$($VALKEY_CLI -h "$HOST" am.index.reindex "article:456")
assert_equals "$result" "1"
# Check shadow Hash has current values
title=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:article:456" title)
content=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:article:456" content)
assert_equals "$title" "Updated Title"
assert_equals "$content" "Created before indexing"
echo "   ✓ REINDEX command rebuilds shadow Hash"

# Test 9: AM.INDEX.STATUS command
echo "Test 9: AM.INDEX.STATUS command..."
status=$($VALKEY_CLI -h "$HOST" am.index.status "article:*")
# Status should contain pattern, enabled, and paths
echo "$status" | grep -q "article:\*"
echo "$status" | grep -q "enabled"
echo "$status" | grep -q "title"
echo "$status" | grep -q "content"
echo "   ✓ STATUS command returns configuration"

# Test 10: Non-matching key does not create index
echo "Test 10: Non-matching key does not create index..."
$VALKEY_CLI -h "$HOST" del "post:789" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "post:789" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "post:789" title "Post Title" > /dev/null
# Should not create shadow Hash (no config for post:* pattern)
exists=$($VALKEY_CLI -h "$HOST" exists "am:idx:post:789")
assert_equals "$exists" "0"
echo "   ✓ Non-matching patterns do not create indexes"

# Test 11: FROMJSON automatic indexing
echo "Test 11: FROMJSON automatic indexing..."
$VALKEY_CLI -h "$HOST" del "article:789" > /dev/null
$VALKEY_CLI -h "$HOST" am.fromjson "article:789" '{"title":"JSON Article","content":"Created from JSON","author":"Jane"}' > /dev/null
# Check shadow Hash
exists=$($VALKEY_CLI -h "$HOST" exists "am:idx:article:789")
assert_equals "$exists" "1"
title=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:article:789" title)
content=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:article:789" content)
assert_equals "$title" "JSON Article"
assert_equals "$content" "Created from JSON"
# Author should not be indexed
author=$($VALKEY_CLI -h "$HOST" hget "am:idx:article:789" author)
assert_equals "$author" ""
echo "   ✓ FROMJSON triggers automatic indexing"

# Test 12: Wildcard pattern matching
echo "Test 12: Wildcard pattern matching..."
# Configure indexing for any key
$VALKEY_CLI -h "$HOST" am.index.configure "*" name > /dev/null
$VALKEY_CLI -h "$HOST" del "anykey" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "anykey" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "anykey" name "Wildcard Match" > /dev/null
# Check shadow Hash
exists=$($VALKEY_CLI -h "$HOST" exists "am:idx:anykey")
assert_equals "$exists" "1"
name=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:anykey" name)
assert_equals "$name" "Wildcard Match"
echo "   ✓ Wildcard pattern (*) matches all keys"

# Test 13: Empty fields not indexed
echo "Test 13: Empty fields not indexed..."
# Clean up wildcard config from test 12 (otherwise it interferes with article:*)
$VALKEY_CLI -h "$HOST" del "am:index:config:*" > /dev/null
# Reconfigure article:* pattern (needed for this and subsequent tests)
$VALKEY_CLI -h "$HOST" am.index.configure "article:*" title content author category > /dev/null
$VALKEY_CLI -h "$HOST" del "article:999" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "article:999" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:999" title "Only Title" > /dev/null
# content is not set, so shadow Hash should only have title
exists=$($VALKEY_CLI -h "$HOST" exists "am:idx:article:999")
assert_equals "$exists" "1"
title=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:article:999" title)
assert_equals "$title" "Only Title"
# content field should not exist in Hash
content=$($VALKEY_CLI -h "$HOST" hget "am:idx:article:999" content)
assert_equals "$content" ""
echo "   ✓ Empty configured fields not indexed"

# Test 14: Reindex works on documents created before config
echo "Test 14: Reindex works on documents created before config..."
# Delete any existing config for reindex:* pattern
$VALKEY_CLI -h "$HOST" del "am:index:config:reindex:*" > /dev/null
$VALKEY_CLI -h "$HOST" del "reindex:doc1" "am:idx:reindex:doc1" > /dev/null
# Create document BEFORE configuring indexing (so no auto-indexing happens)
$VALKEY_CLI -h "$HOST" am.new "reindex:doc1" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "reindex:doc1" title "Reindex Test" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "reindex:doc1" content "Test Content" > /dev/null
# Check that no index exists yet
exists=$($VALKEY_CLI -h "$HOST" exists "am:idx:reindex:doc1")
assert_equals "$exists" "0"
# Now configure indexing for this pattern
$VALKEY_CLI -h "$HOST" am.index.configure "reindex:*" title content > /dev/null
# Still no index (config doesn't retroactively index)
exists=$($VALKEY_CLI -h "$HOST" exists "am:idx:reindex:doc1")
assert_equals "$exists" "0"
# Manually reindex
$VALKEY_CLI -h "$HOST" am.index.reindex "reindex:doc1" > /dev/null
# Now check that index exists
exists=$($VALKEY_CLI -h "$HOST" exists "am:idx:reindex:doc1")
assert_equals "$exists" "1"
title=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:reindex:doc1" title)
content=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:reindex:doc1" content)
assert_equals "$title" "Reindex Test"
assert_equals "$content" "Test Content"
echo "   ✓ Manual reindexing works on pre-existing documents"

# Test 15: Multiple pattern configurations
echo "Test 15: Multiple pattern configurations..."
$VALKEY_CLI -h "$HOST" am.index.configure "blog:*" title body tags > /dev/null
$VALKEY_CLI -h "$HOST" del "blog:post1" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "blog:post1" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "blog:post1" title "Blog Post" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "blog:post1" body "Blog body text" > /dev/null
# Check shadow Hash
exists=$($VALKEY_CLI -h "$HOST" exists "am:idx:blog:post1")
assert_equals "$exists" "1"
title=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:blog:post1" title)
body=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:blog:post1" body)
assert_equals "$title" "Blog Post"
assert_equals "$body" "Blog body text"
# Original article:* pattern should still work
$VALKEY_CLI -h "$HOST" am.puttext "article:123" title "Still Works" > /dev/null
title=$($VALKEY_CLI -h "$HOST" --raw hget "am:idx:article:123" title)
assert_equals "$title" "Still Works"
echo "   ✓ Multiple patterns can coexist"

# Test 16: FT.CREATE and FT.SEARCH integration - Tag search
echo "Test 16: Valkey-Search integration - Tag field search..."
# Check if search module is available
if ! $VALKEY_CLI -h "$HOST" module list 2>/dev/null | grep -q "search"; then
    echo "   ⚠️  search module not found - skipping FT.SEARCH tests"
    echo ""
    echo "✅ All search indexing tests passed (FT.SEARCH tests skipped)!"
    exit 0
fi
# Clean up any existing index and data
$VALKEY_CLI -h "$HOST" ft.dropindex idx:test_articles 2>/dev/null || true
$VALKEY_CLI -h "$HOST" del "article:search1" "article:search2" "article:search3" "article:search4" > /dev/null
$VALKEY_CLI -h "$HOST" del "am:idx:article:search1" "am:idx:article:search2" "am:idx:article:search3" "am:idx:article:search4" > /dev/null
# Configure AM indexing FIRST, create documents, THEN create search index
# (Creating FT index before documents can cause blocking issues with module callbacks)
$VALKEY_CLI -h "$HOST" am.index.configure "article:*" title content author category > /dev/null
# Create test articles first (shadow hashes will be created)
$VALKEY_CLI -h "$HOST" am.new "article:search1" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search1" title "redis-tutorial" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search1" content "crdt-guide" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search1" author "Alice" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search1" category "tutorial" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "article:search2" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search2" title "automerge-advanced" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search2" content "crdt-deep-dive" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search2" author "Bob" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search2" category "advanced" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "article:search3" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search3" title "redis-performance" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search3" content "optimization-tips" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search3" author "Charlie" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search3" category "performance" > /dev/null
$VALKEY_CLI -h "$HOST" am.new "article:search4" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search4" title "database-sharding" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search4" content "scaling-guide" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search4" author "Alice" > /dev/null
$VALKEY_CLI -h "$HOST" am.puttext "article:search4" category "architecture" > /dev/null
# Now create the search index AFTER documents exist (avoids blocking issues)
$VALKEY_CLI -h "$HOST" ft.create idx:test_articles on hash prefix 1 am:idx:article:search schema title TAG content TAG author TAG category TAG > /dev/null
# Wait for backfill to complete
sleep 1
# Search by author:Alice - should find search1 and search4
results=$($VALKEY_CLI -h "$HOST" ft.search idx:test_articles "@author:{Alice}" nocontent)
echo "$results" | grep -q "am:idx:article:search1"
echo "$results" | grep -q "am:idx:article:search4"
# Verify Bob and Charlie articles are excluded
if echo "$results" | grep -q "am:idx:article:search2"; then
    echo "   ✗ Author search incorrectly included search2 (Bob)"
    exit 1
fi
if echo "$results" | grep -q "am:idx:article:search3"; then
    echo "   ✗ Author search incorrectly included search3 (Charlie)"
    exit 1
fi
echo "   ✓ Tag field search correctly filters by author"

# Test 17: FT.SEARCH - Category tag search
echo "Test 17: Valkey-Search integration - Category tag search..."
# Search by category:tutorial - should only find search1
results=$($VALKEY_CLI -h "$HOST" ft.search idx:test_articles "@category:{tutorial}" nocontent)
echo "$results" | grep -q "am:idx:article:search1"
# Verify other categories are excluded
if echo "$results" | grep -q "am:idx:article:search2"; then
    echo "   ✗ Tag search incorrectly included search2 (advanced)"
    exit 1
fi
if echo "$results" | grep -q "am:idx:article:search3"; then
    echo "   ✗ Tag search incorrectly included search3 (performance)"
    exit 1
fi
if echo "$results" | grep -q "am:idx:article:search4"; then
    echo "   ✗ Tag search incorrectly included search4 (architecture)"
    exit 1
fi
echo "   ✓ Category tag search correctly filters"

# Test 18: FT.SEARCH - Combined tag query
echo "Test 18: Valkey-Search integration - Combined tag query..."
# Search for Alice + tutorial category - should only find search1
results=$($VALKEY_CLI -h "$HOST" ft.search idx:test_articles "@author:{Alice} @category:{tutorial}" nocontent)
echo "$results" | grep -q "am:idx:article:search1"
# search4 by Alice but different category - should be excluded
if echo "$results" | grep -q "am:idx:article:search4"; then
    echo "   ✗ Combined query incorrectly included search4 (wrong category)"
    exit 1
fi
echo "   ✓ Combined tag query correctly filters by multiple criteria"

# Test 19: FT.SEARCH - Title tag search
echo "Test 19: Valkey-Search integration - Title tag search..."
# Search for exact title match - should only find search3
results=$($VALKEY_CLI -h "$HOST" ft.search idx:test_articles "@title:{redis-performance}" nocontent)
echo "$results" | grep -q "am:idx:article:search3"
# Verify others are excluded
if echo "$results" | grep -q "am:idx:article:search1"; then
    echo "   ✗ Title search incorrectly included search1"
    exit 1
fi
if echo "$results" | grep -q "am:idx:article:search2"; then
    echo "   ✗ Title search incorrectly included search2"
    exit 1
fi
if echo "$results" | grep -q "am:idx:article:search4"; then
    echo "   ✗ Title search incorrectly included search4"
    exit 1
fi
echo "   ✓ Title tag search works correctly"

# Test 20: FT.SEARCH - Content tag search
echo "Test 20: Valkey-Search integration - Content tag search..."
# Search for content tag - should find search1 and search2 (both have crdt in content)
results=$($VALKEY_CLI -h "$HOST" ft.search idx:test_articles "@content:{crdt-guide}" nocontent)
echo "$results" | grep -q "am:idx:article:search1"
if echo "$results" | grep -q "am:idx:article:search3"; then
    echo "   ✗ Content search incorrectly included search3"
    exit 1
fi
echo "   ✓ Content tag search works correctly"

# Cleanup
$VALKEY_CLI -h "$HOST" ft.dropindex idx:test_articles > /dev/null 2>&1 || true

echo ""
echo "✅ All search indexing tests passed!"
