#include "phmap_string_wrapper.h"
#include "unified_str.h"
#include <string>
#include <cstring>

namespace parallelffi {

  std::shared_ptr<ParallelStringMap> new_parallel_string_map_cpp(size_t capacity) {
    return std::make_shared<ParallelStringMap>(capacity);
  }
  bool parallel_string_insert_cpp(const std::shared_ptr<ParallelStringMap>& m, UnifiedStr& key, MapValue& value) {
    return m->insert(key, value);
  }
  bool parallel_string_get_cpp(const std::shared_ptr<ParallelStringMap>& m, UnifiedStr& key, MapValue* out_value) {
    MapValue value;
    bool found = m->get_value(key, value);
    if (found && out_value != nullptr) {
      *out_value = value;
    }
    return found;
  }
  bool parallel_string_remove_cpp(const std::shared_ptr<ParallelStringMap>& m, UnifiedStr& key) {
    return m->remove(key);
  }
  int64_t parallel_string_size_cpp(const std::shared_ptr<ParallelStringMap>& m) {
    return m->size();
  }

} // namespace parallelffi

extern "C" {
struct parallelffi_ParallelStringMapOpaque {
  std::shared_ptr<parallelffi::ParallelStringMap> inner;
};

parallelffi_ParallelStringMapOpaque* new_parallel_string_map(size_t capacity) {
  auto obj = new parallelffi_ParallelStringMapOpaque;
  obj->inner = parallelffi::new_parallel_string_map_cpp(capacity);
  return obj;
}
void free_parallel_string_map(parallelffi_ParallelStringMapOpaque* map) {
  delete map;
}
bool parallel_string_insert(parallelffi_ParallelStringMapOpaque* map, UnifiedStr& key, MapValue& value) {
  return parallelffi::parallel_string_insert_cpp(map->inner, key, value);
}
bool parallel_string_get(parallelffi_ParallelStringMapOpaque* map, UnifiedStr& key, MapValue* out_value) {
  return parallelffi::parallel_string_get_cpp(map->inner, key, out_value);
}
bool parallel_string_remove(parallelffi_ParallelStringMapOpaque* map, UnifiedStr& key) {
  return parallelffi::parallel_string_remove_cpp(map->inner, key);
}
int64_t parallel_string_size(parallelffi_ParallelStringMapOpaque* map) {
  return parallelffi::parallel_string_size_cpp(map->inner);
}
} // extern "C" 