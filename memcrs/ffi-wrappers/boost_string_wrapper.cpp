#include "boost_string_wrapper.h"
#include "unified_str.h"
#include <cstring>

namespace boostffi {

std::shared_ptr<BoostStringMap> new_boost_string_map_cpp(size_t capacity) {
  return std::make_shared<BoostStringMap>(capacity);
}
bool boost_string_insert_cpp(const std::shared_ptr<BoostStringMap>& m, UnifiedStr& k, UnifiedStrLarge& v) {
  return m->insert(k, v);
}
bool boost_string_get_cpp(const std::shared_ptr<BoostStringMap>& m, UnifiedStr& k, UnifiedStrLarge* out_value) {
  auto result = m->get(k);
  if (result.has_value()) {
    if (out_value != nullptr) {
      *out_value = result.value();
    }
    return true;
  }
  return false;
}
bool boost_string_remove_cpp(const std::shared_ptr<BoostStringMap>& m, UnifiedStr& k) {
  return m->remove(k);
}
bool boost_string_update_cpp(const std::shared_ptr<BoostStringMap>& m, UnifiedStr& k, UnifiedStrLarge& v) {
  return m->update(k, v);
}
size_t boost_string_size_cpp(const std::shared_ptr<BoostStringMap>& m) {
  return m->size();
}

}  // namespace boostffi

extern "C" {
struct boostffi_BoostStringMapOpaque {
  std::shared_ptr<boostffi::BoostStringMap> inner;
};

boostffi_BoostStringMapOpaque* new_boost_string_map(size_t capacity) {
  auto obj = new boostffi_BoostStringMapOpaque;
  obj->inner = boostffi::new_boost_string_map_cpp(capacity);
  return obj;
}
void free_boost_string_map(boostffi_BoostStringMapOpaque* map) {
  delete map;
}
bool boost_string_insert(boostffi_BoostStringMapOpaque* map, UnifiedStr& k, UnifiedStrLarge& v) {
  return boostffi::boost_string_insert_cpp(map->inner, k, v);
}
bool boost_string_get(boostffi_BoostStringMapOpaque* map, UnifiedStr& k, UnifiedStrLarge* out_value) {
  return boostffi::boost_string_get_cpp(map->inner, k, out_value);
}
bool boost_string_remove(boostffi_BoostStringMapOpaque* map, UnifiedStr& k) {
  return boostffi::boost_string_remove_cpp(map->inner, k);
}
bool boost_string_update(boostffi_BoostStringMapOpaque* map, UnifiedStr& k, UnifiedStrLarge& v) {
  return boostffi::boost_string_update_cpp(map->inner, k, v);
}
size_t boost_string_size(boostffi_BoostStringMapOpaque* map) {
  return boostffi::boost_string_size_cpp(map->inner);
}
} // extern "C" 