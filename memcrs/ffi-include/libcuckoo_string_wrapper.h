#pragma once
#include <cstdint>
#include <memory>
#include "unified_str.h"
#include "libcuckoo/cuckoohash_map.hh"
#include <cstring>

namespace cuckooffi {

class CuckooStringMap {
  using Table = libcuckoo::cuckoohash_map<UnifiedStr, MapValue, UnifiedStrHash, UnifiedStrEqual>;
  Table table_;
public:
  explicit CuckooStringMap(size_t capacity) : table_(capacity) {}
  bool insert(const UnifiedStr& key, const MapValue& value) { return table_.insert(key, value); }
  bool get(const UnifiedStr& key) { MapValue value; return table_.find(key, value); }
  bool remove(const UnifiedStr& key) { return table_.erase(key); }
  bool get_value(const UnifiedStr& key, MapValue& value) const {
    return table_.find(key, value);
  }
  int64_t size() const { return table_.size(); }
};
std::shared_ptr<CuckooStringMap> new_cuckoo_string_map_cpp(size_t capacity);
bool cuckoo_string_insert_cpp(const std::shared_ptr<CuckooStringMap>& m, UnifiedStr& key, MapValue& value);
bool cuckoo_string_get_cpp(const std::shared_ptr<CuckooStringMap>& m, UnifiedStr& key, MapValue* out_value);
bool cuckoo_string_remove_cpp(const std::shared_ptr<CuckooStringMap>& m, UnifiedStr& key);
int64_t cuckoo_string_size_cpp(const std::shared_ptr<CuckooStringMap>& m);

} // namespace cuckooffi

#ifdef __cplusplus
extern "C" {
#endif
// C ABI for FFI
typedef struct cuckooffi_CuckooStringMapOpaque cuckooffi_CuckooStringMapOpaque;

cuckooffi_CuckooStringMapOpaque* new_cuckoo_string_map(size_t capacity);
void free_cuckoo_string_map(cuckooffi_CuckooStringMapOpaque* map);
bool cuckoo_string_insert(cuckooffi_CuckooStringMapOpaque* map, UnifiedStr& key, MapValue& value);
bool cuckoo_string_get(cuckooffi_CuckooStringMapOpaque* map, UnifiedStr& key, MapValue* out_value);
bool cuckoo_string_remove(cuckooffi_CuckooStringMapOpaque* map, UnifiedStr& key);
int64_t cuckoo_string_size(cuckooffi_CuckooStringMapOpaque* map);
#ifdef __cplusplus
}
#endif 