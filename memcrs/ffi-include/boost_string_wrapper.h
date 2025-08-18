#pragma once

#include <boost/unordered/concurrent_flat_map.hpp>
#include <cstdint>
#include <optional>
#include <memory>
#include "unified_str.h"
#include <cstring>

namespace boostffi {

struct BoostStringMap {
  using Table = boost::concurrent_flat_map<UnifiedStr, MapValue, UnifiedStrHash, UnifiedStrEqual>;
  Table table;

  explicit BoostStringMap(size_t capacity) {
    table.reserve(capacity);
  }

  std::optional<MapValue> get(const UnifiedStr& k) const {
    std::optional<MapValue> out;
    table.visit(k, [&](auto const& kv) { out = kv.second; });
    return out;
  }

  bool insert(const UnifiedStr& k, const MapValue& v) {
    return table.insert(std::make_pair(k, v));
  }

  bool remove(const UnifiedStr& k) {
    return table.erase(k) > 0;
  }

  bool update(const UnifiedStr& k, const MapValue& v) {
    return table.insert_or_assign(k, v);
  }

  size_t size() const {
    return table.size();
  }
};

// Factory + operations exposed to cxx
std::shared_ptr<BoostStringMap> new_boost_string_map_cpp(size_t capacity);
bool boost_string_insert_cpp(const std::shared_ptr<BoostStringMap>& m, UnifiedStr& k, MapValue& v);
bool boost_string_get_cpp(const std::shared_ptr<BoostStringMap>& m, UnifiedStr& k, MapValue* out_value);
bool boost_string_remove_cpp(const std::shared_ptr<BoostStringMap>& m, UnifiedStr& k);
bool boost_string_update_cpp(const std::shared_ptr<BoostStringMap>& m, UnifiedStr& k, MapValue& v);
size_t boost_string_size_cpp(const std::shared_ptr<BoostStringMap>& m);

}  // namespace boostffi

#ifdef __cplusplus
extern "C" {
#endif
// C ABI for FFI
typedef struct boostffi_BoostStringMapOpaque boostffi_BoostStringMapOpaque;

boostffi_BoostStringMapOpaque* new_boost_string_map(size_t capacity);
void free_boost_string_map(boostffi_BoostStringMapOpaque* map);
bool boost_string_insert(boostffi_BoostStringMapOpaque* map, UnifiedStr& k, MapValue& v);
bool boost_string_get(boostffi_BoostStringMapOpaque* map, UnifiedStr& k, MapValue* out_value);
bool boost_string_remove(boostffi_BoostStringMapOpaque* map, UnifiedStr& k);
bool boost_string_update(boostffi_BoostStringMapOpaque* map, UnifiedStr& k, MapValue& v);
size_t boost_string_size(boostffi_BoostStringMapOpaque* map);
#ifdef __cplusplus
}
#endif 