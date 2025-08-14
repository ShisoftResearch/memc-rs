#pragma once
#include <cstdint>
#include <memory>
#include "unified_str.h"
#include <tbb/concurrent_hash_map.h>
#include <cstring>

namespace tbbffi {

  struct UnifiedStrHashCompare {
    static size_t hash(const UnifiedStr& k) {
      return UnifiedStrHash{}(k);
    }
    static bool equal(const UnifiedStr& a, const UnifiedStr& b) {
      return memcmp(a.data, b.data, UNIFIED_STR_CAP) == 0;
    }
  };

  struct StringMapWrapper {
    explicit StringMapWrapper(size_t capacity)
      : map(capacity) {}
    using Table = tbb::concurrent_hash_map<UnifiedStr, UnifiedStrLarge, UnifiedStrHashCompare>;
    Table map;
  };

} // namespace tbbffi

#ifdef __cplusplus
extern "C" {
#endif

tbbffi::StringMapWrapper* new_tbb_string_map(size_t capacity);
void free_tbb_string_map(tbbffi::StringMapWrapper* map);

bool tbb_string_insert(tbbffi::StringMapWrapper* m, UnifiedStr& key, UnifiedStrLarge& value);
bool tbb_string_get(tbbffi::StringMapWrapper* m, UnifiedStr& key, UnifiedStrLarge* out_value);
bool tbb_string_remove(tbbffi::StringMapWrapper* m, UnifiedStr& key);
bool tbb_string_update(tbbffi::StringMapWrapper* m, UnifiedStr& key, UnifiedStrLarge& value);

#ifdef __cplusplus
}
#endif 