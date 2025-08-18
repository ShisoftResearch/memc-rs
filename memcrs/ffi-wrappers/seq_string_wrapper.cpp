#include "seq_string_wrapper.h"
#include "unified_str.h"
#include <string>
#include <cstring>

namespace seqffi {

  std::shared_ptr<SeqStringMap> new_seq_string_map_cpp(size_t capacity) {
    return std::make_shared<SeqStringMap>(capacity);
  }
  bool seq_string_find_cpp(const std::shared_ptr<SeqStringMap>& m, UnifiedStr& key, MapValue* out_value) {
    MapValue value;
    bool found = m->get_value(key, value);
    if (found && out_value != nullptr) {
      *out_value = value;
    }
    return found;
  }
  bool seq_string_insert_cpp(const std::shared_ptr<SeqStringMap>& m, UnifiedStr& key, MapValue& value) {
    return m->insert(key, value);
  }
  bool seq_string_remove_cpp(const std::shared_ptr<SeqStringMap>& m, UnifiedStr& key) {
    return m->remove(key);
  }
  bool seq_string_update_cpp(const std::shared_ptr<SeqStringMap>& m, UnifiedStr& key, MapValue& value) {
    return m->update(key, value);
  }
  int64_t seq_string_size_cpp(const std::shared_ptr<SeqStringMap>& m) {
    return m->size();
  }

} // namespace seqffi

extern "C" {
struct seqffi_SeqStringMapOpaque {
  std::shared_ptr<seqffi::SeqStringMap> inner;
};

seqffi_SeqStringMapOpaque* new_seq_string_map(size_t capacity) {
  auto obj = new seqffi_SeqStringMapOpaque;
  obj->inner = seqffi::new_seq_string_map_cpp(capacity);
  return obj;
}
void free_seq_string_map(seqffi_SeqStringMapOpaque* map) {
  delete map;
}
bool seq_string_find(seqffi_SeqStringMapOpaque* map, UnifiedStr& key, MapValue* out_value) {
  return seqffi::seq_string_find_cpp(map->inner, key, out_value);
}
bool seq_string_insert(seqffi_SeqStringMapOpaque* map, UnifiedStr& key, MapValue& value) {
  return seqffi::seq_string_insert_cpp(map->inner, key, value);
}
bool seq_string_remove(seqffi_SeqStringMapOpaque* map, UnifiedStr& key) {
  return seqffi::seq_string_remove_cpp(map->inner, key);
}
int64_t seq_string_size(seqffi_SeqStringMapOpaque* map) {
  return seqffi::seq_string_size_cpp(map->inner);
}
} // extern "C" 