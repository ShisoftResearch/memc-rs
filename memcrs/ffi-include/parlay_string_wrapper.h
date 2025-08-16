#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include "parlay_hash/unordered_map.h"
#include "unified_str.h"

namespace parlayffi {

  // Concurrent map for UnifiedStr→UnifiedStr
  using StringMapType = parlay::parlay_unordered_map<UnifiedStr, MapValue, UnifiedStrHash, UnifiedStrEqual>;

  // A tiny C++ class owning one map instance.
  struct StringMapWrapper {
  // Pass the capacity directly to the map's constructor:
  explicit StringMapWrapper(size_t capacity)
    : map((long)capacity)            /* calls unordered_map_internal(long n) */
  {}
    StringMapType map;
  };

  // Factory: create a new StringMapWrapper on the heap.
  std::shared_ptr<StringMapWrapper> new_string_map_cpp(size_t capacity);

  // Instance methods on StringMapWrapper:
  bool insert_string_kv_cpp(const std::shared_ptr<StringMapWrapper>& m, UnifiedStr &key, MapValue& value);
  bool get_string_kv_cpp(const std::shared_ptr<StringMapWrapper>& m, UnifiedStr& key, MapValue* out_value);
  bool remove_string_kv_cpp(const std::shared_ptr<StringMapWrapper>& m, UnifiedStr& key);
  bool update_string_kv_cpp(const std::shared_ptr<StringMapWrapper>& m, UnifiedStr& key, MapValue& value);

} // namespace parlayffi

#ifdef __cplusplus
extern "C" {
#endif
// C ABI for FFI
typedef struct parlayffi_StringMapWrapperOpaque parlayffi_StringMapWrapperOpaque;

parlayffi_StringMapWrapperOpaque* new_string_map(size_t capacity);
void free_string_map(parlayffi_StringMapWrapperOpaque* map);
bool insert_string_kv(parlayffi_StringMapWrapperOpaque* map, UnifiedStr& key, MapValue& value);
bool get_string_kv(parlayffi_StringMapWrapperOpaque* map, UnifiedStr& key, MapValue* out_value);
bool remove_string_kv(parlayffi_StringMapWrapperOpaque* map, UnifiedStr& key);
bool update_string_kv(parlayffi_StringMapWrapperOpaque* map, UnifiedStr& key, MapValue& value);
#ifdef __cplusplus
}
#endif 