#include <folly/concurrency/ConcurrentHashMap.h>
#include <memory>
#include <optional>
#include "folly_string_wrapper.h"
#include "unified_str.h"

namespace follyffi {

  std::shared_ptr<StringMap> new_folly_string_map_cpp(std::size_t capacity) {
    return std::make_shared<StringMap>(capacity);
  }
  bool folly_string_insert_cpp(const std::shared_ptr<StringMap>& m, UnifiedStr& key, UnifiedStrLarge& value) {
    return m->insert(key, value);
  }
  bool folly_string_get_cpp(const std::shared_ptr<StringMap>& m, UnifiedStr& key, UnifiedStrLarge* out_value) {
    auto result = m->get_value(key);
    if (result.has_value()) {
      if (out_value != nullptr) {
        *out_value = result.value();
      }
      return true;
    }
    return false;
  }
  bool folly_string_remove_cpp(const std::shared_ptr<StringMap>& m, UnifiedStr& key) {
    return m->remove(key);
  }
  bool folly_string_update_cpp(const std::shared_ptr<StringMap>& m, UnifiedStr& key, UnifiedStrLarge& value) {
    return m->update(key, value);
  }

} // namespace follyffi

extern "C" {
struct follyffi_StringMapOpaque {
  std::shared_ptr<follyffi::StringMap> inner;
};

follyffi_StringMapOpaque* new_folly_string_map(std::size_t capacity) {
  auto obj = new follyffi_StringMapOpaque;
  obj->inner = follyffi::new_folly_string_map_cpp(capacity);
  return obj;
}
void free_folly_string_map(follyffi_StringMapOpaque* map) {
  delete map;
}
bool folly_string_insert(follyffi_StringMapOpaque* map, UnifiedStr& key, UnifiedStrLarge& value) {
  return follyffi::folly_string_insert_cpp(map->inner, key, value);
}
bool folly_string_get(follyffi_StringMapOpaque* map, UnifiedStr& key, UnifiedStrLarge* out_value) {
  return follyffi::folly_string_get_cpp(map->inner, key, out_value);
}
bool folly_string_remove(follyffi_StringMapOpaque* map, UnifiedStr& key) {
  return follyffi::folly_string_remove_cpp(map->inner, key);
}
bool folly_string_update(follyffi_StringMapOpaque* map, UnifiedStr& key, UnifiedStrLarge& value) {
  return follyffi::folly_string_update_cpp(map->inner, key, value);
}
} // extern "C" 