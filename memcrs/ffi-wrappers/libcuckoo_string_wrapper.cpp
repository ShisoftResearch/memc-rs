#include "libcuckoo_string_wrapper.h"
#include "unified_str.h"
#include <cstring>

namespace cuckooffi {

  std::shared_ptr<CuckooStringMap> new_cuckoo_string_map_cpp(size_t capacity) {
    return std::make_shared<CuckooStringMap>(capacity);
  }
  bool cuckoo_string_insert_cpp(const std::shared_ptr<CuckooStringMap>& m, UnifiedStr& key, MapValue& value) {
    return m->insert(key, value);
  }
  bool cuckoo_string_get_cpp(const std::shared_ptr<CuckooStringMap>& m, UnifiedStr& key, MapValue* out_value) {
    MapValue value;
    bool found = m->get_value(key, value);
    if (found && out_value != nullptr) {
      *out_value = value;
    }
    return found;
  }
  bool cuckoo_string_remove_cpp(const std::shared_ptr<CuckooStringMap>& m, UnifiedStr& key) {
    return m->remove(key);
  }
  int64_t cuckoo_string_size_cpp(const std::shared_ptr<CuckooStringMap>& m) {
    return m->size();
  }

} // namespace cuckooffi

extern "C" {
struct cuckooffi_CuckooStringMapOpaque {
  std::shared_ptr<cuckooffi::CuckooStringMap> inner;
};

cuckooffi_CuckooStringMapOpaque* new_cuckoo_string_map(size_t capacity) {
  auto obj = new cuckooffi_CuckooStringMapOpaque;
  obj->inner = cuckooffi::new_cuckoo_string_map_cpp(capacity);
  return obj;
}
void free_cuckoo_string_map(cuckooffi_CuckooStringMapOpaque* map) {
  delete map;
}
bool cuckoo_string_insert(cuckooffi_CuckooStringMapOpaque* map, UnifiedStr& key, MapValue& value) {
  return cuckooffi::cuckoo_string_insert_cpp(map->inner, key, value);
}
bool cuckoo_string_get(cuckooffi_CuckooStringMapOpaque* map, UnifiedStr& key, MapValue* out_value) {
  return cuckooffi::cuckoo_string_get_cpp(map->inner, key, out_value);
}
bool cuckoo_string_remove(cuckooffi_CuckooStringMapOpaque* map, UnifiedStr& key) {
  return cuckooffi::cuckoo_string_remove_cpp(map->inner, key);
}
int64_t cuckoo_string_size(cuckooffi_CuckooStringMapOpaque* map) {
  return cuckooffi::cuckoo_string_size_cpp(map->inner);
}
} // extern "C" 