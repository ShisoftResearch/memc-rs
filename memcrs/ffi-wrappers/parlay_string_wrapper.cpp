#include "parlay_string_wrapper.h"
#include "unified_str.h"
#include <string>
#include <cstring>

namespace parlayffi {

  std::shared_ptr<StringMapWrapper> new_string_map_cpp(size_t capacity) {
    return std::make_shared<StringMapWrapper>(capacity);
  }

  bool insert_string_kv_cpp(const std::shared_ptr<StringMapWrapper>& m, UnifiedStr& key, UnifiedStrLarge& value) {
    return m->map.insert({key, value}).second;
  }

  bool get_string_kv_cpp(const std::shared_ptr<StringMapWrapper>& m, UnifiedStr& key, UnifiedStrLarge* out_value) {
    auto result = m->map.Find(key);
    if (result.has_value()) {
      if (out_value != nullptr) {
        *out_value = result.value();
      }
      return true;
    }
    return false;
  }

  bool remove_string_kv_cpp(const std::shared_ptr<StringMapWrapper>& m, UnifiedStr& key) {
    return m->map.erase(key) > 0;
  }

  bool update_string_kv_cpp(const std::shared_ptr<StringMapWrapper>& m, UnifiedStr& key, UnifiedStrLarge& value) {
    auto result = m->map.Upsert(key, [value](const std::optional<UnifiedStrLarge>& v) { return value; });
    return !result.has_value();
  }

}  // namespace parlayffi

extern "C" {
struct parlayffi_StringMapWrapperOpaque {
  std::shared_ptr<parlayffi::StringMapWrapper> inner;
};

parlayffi_StringMapWrapperOpaque* new_string_map(size_t capacity) {
  auto obj = new parlayffi_StringMapWrapperOpaque;
  obj->inner = parlayffi::new_string_map_cpp(capacity);
  return obj;
}
void free_string_map(parlayffi_StringMapWrapperOpaque* map) {
  delete map;
}
bool insert_string_kv(parlayffi_StringMapWrapperOpaque* map, UnifiedStr& key, UnifiedStrLarge& value) {
  return parlayffi::insert_string_kv_cpp(map->inner, key, value);
}
bool get_string_kv(parlayffi_StringMapWrapperOpaque* map, UnifiedStr& key, UnifiedStrLarge* out_value) {
  return parlayffi::get_string_kv_cpp(map->inner, key, out_value);
}
bool remove_string_kv(parlayffi_StringMapWrapperOpaque* map, UnifiedStr& key) {
  return parlayffi::remove_string_kv_cpp(map->inner, key);
}
bool update_string_kv(parlayffi_StringMapWrapperOpaque* map, UnifiedStr& key, UnifiedStrLarge& value) {
  return parlayffi::update_string_kv_cpp(map->inner, key, value);
}
} // extern "C" 