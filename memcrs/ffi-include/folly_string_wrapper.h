// include/folly_string_wrapper.h
#pragma once

#define FOLLY_F14_VECTOR_INTRINSICS_MODE 2

// --- Folly and STL includes
#include <folly/concurrency/ConcurrentHashMap.h>
#include <memory>
#include <optional>
#include <cstdint>
#include <cstddef>
#include "unified_str.h"

namespace follyffi {

// 1) Our templated wrapper around F14ValueMap
template<
    class Hash     = UnifiedStrHash,
    class KeyEqual = UnifiedStrEqual
>
class MapWrapper {
  using UMap = folly::ConcurrentHashMap<UnifiedStr, MapValue, Hash, KeyEqual>;

  UMap map_;

public:
  explicit MapWrapper(std::size_t capacity)
    : map_(capacity)
  {}

  bool insert(const UnifiedStr& k, const MapValue& v) {
    return map_.insert({k, v}).second;
  }

  bool get(const UnifiedStr& k) {
    auto it = map_.find(k);
    if (it != map_.end()) return true;
    return false;
  }

  bool remove(const UnifiedStr& k) {
    return map_.erase(k) == 1;
  }

  bool update(const UnifiedStr& k, const MapValue& v) {
    return map_.insert({k, v}).second;
  }

  std::optional<MapValue> get_value(const UnifiedStr& k) const {
    auto it = map_.find(k);
    if (it != map_.end()) return it->second;
    return std::nullopt;
  }
};

// 2) Explicit instantiation for string→string
using StringMap = MapWrapper<>;

// 3) C++ API (for internal use)
std::shared_ptr<StringMap> new_folly_string_map_cpp(std::size_t capacity);
bool folly_string_insert_cpp(const std::shared_ptr<StringMap>& m, UnifiedStr& key, MapValue& value);
bool folly_string_get_cpp(const std::shared_ptr<StringMap>& m, UnifiedStr& key, MapValue* out_value);
bool folly_string_remove_cpp(const std::shared_ptr<StringMap>& m, UnifiedStr& key);
bool folly_string_update_cpp(const std::shared_ptr<StringMap>& m, UnifiedStr& key, MapValue& value);

} // namespace follyffi

#ifdef __cplusplus
extern "C" {
#endif
// C ABI for FFI
typedef struct follyffi_StringMapOpaque follyffi_StringMapOpaque;

follyffi_StringMapOpaque* new_folly_string_map(std::size_t capacity);
void free_folly_string_map(follyffi_StringMapOpaque* map);
bool folly_string_insert(follyffi_StringMapOpaque* map, UnifiedStr& key, MapValue& value);
bool folly_string_get(follyffi_StringMapOpaque* map, UnifiedStr& key, MapValue* out_value);
bool folly_string_remove(follyffi_StringMapOpaque* map, UnifiedStr& key);
bool folly_string_update(follyffi_StringMapOpaque* map, UnifiedStr& key, MapValue& value);
#ifdef __cplusplus
}
#endif 