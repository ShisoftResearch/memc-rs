#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include "unified_str.h"
#include "parallel_hashmap/phmap.h"

namespace parallelffi {

class ParallelStringMap {
  using Table = phmap::parallel_flat_hash_map<
    UnifiedStr, MapValue,
    UnifiedStrHash,
    UnifiedStrEqual,
    std::allocator<std::pair<const UnifiedStr, MapValue>>,
    12, std::mutex
  >;
  Table table_;

public:
  explicit ParallelStringMap(size_t capacity) {
    table_.reserve(capacity);
  }

  bool insert(const UnifiedStr& key, const MapValue& value) {
    return table_.insert({key, value}).second;
  }

  bool get(const UnifiedStr& key) {
    return table_.contains(key);
  }

  bool remove(const UnifiedStr& key) {
    return table_.erase(key) > 0;
  }

  // Returns true if the value was updated (i.e., key already existed), false if inserted new.
  bool update(const UnifiedStr& key, const MapValue& value) {
    // insert_or_assign returns a pair<iterator, bool> where bool is true if inserted, false if assigned
    // We want to return true if updated (i.e., key already existed), false if inserted new.
    auto result = table_.insert_or_assign(key, value);
    return !result.second; // true if updated (assigned), false if inserted
  }

  bool get_value(const UnifiedStr& key, MapValue& value) const {
    bool found = false;
    table_.if_contains(key, [&](const auto& kv) {
      value = kv.second;
      found = true;
    });
    return found;
  }

  int64_t size() const {
    return table_.size();
  }
};

// Factory + operations
std::shared_ptr<ParallelStringMap> new_parallel_string_map_cpp(size_t capacity);
bool parallel_string_insert_cpp(const std::shared_ptr<ParallelStringMap>& m, UnifiedStr& key, MapValue& value);
bool parallel_string_get_cpp(const std::shared_ptr<ParallelStringMap>& m, UnifiedStr& key, MapValue* out_value);
bool parallel_string_remove_cpp(const std::shared_ptr<ParallelStringMap>& m, UnifiedStr& key);
bool parallel_string_update_cpp(const std::shared_ptr<ParallelStringMap>& m, UnifiedStr& key, MapValue& value);
int64_t parallel_string_size_cpp(const std::shared_ptr<ParallelStringMap>& m);

} // namespace parallelffi

#ifdef __cplusplus
extern "C" {
#endif
// C ABI for FFI
typedef struct parallelffi_ParallelStringMapOpaque parallelffi_ParallelStringMapOpaque;

parallelffi_ParallelStringMapOpaque* new_parallel_string_map(size_t capacity);
void free_parallel_string_map(parallelffi_ParallelStringMapOpaque* map);
bool parallel_string_insert(parallelffi_ParallelStringMapOpaque* map, UnifiedStr& key, MapValue& value);
bool parallel_string_get(parallelffi_ParallelStringMapOpaque* map, UnifiedStr& key, MapValue* out_value);
bool parallel_string_remove(parallelffi_ParallelStringMapOpaque* map, UnifiedStr& key);
bool parallel_string_update(parallelffi_ParallelStringMapOpaque* map, UnifiedStr& key, MapValue& value);
int64_t parallel_string_size(parallelffi_ParallelStringMapOpaque* map);
#ifdef __cplusplus
}
#endif 