#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include "unified_str.h"
#include "seq/seq/seq/concurrent_map.hpp"

namespace seqffi {

class SeqStringMap {
  using Table = seq::concurrent_map<
    UnifiedStr, UnifiedStrLarge,
    UnifiedStrHash,
    UnifiedStrEqual,
    std::allocator<std::pair<UnifiedStr, UnifiedStrLarge>>
  >;
  Table table_;

public:
  explicit SeqStringMap(size_t capacity) : table_(capacity) {}

  bool find(const UnifiedStr& key) {
    bool found = false;
    table_.cvisit(key, [&](const auto&) { found = true; });
    return found;
  }

  bool insert(const UnifiedStr& key, const UnifiedStrLarge& value) {
    return table_.insert(std::make_pair(key, value));
  }

  bool update(const UnifiedStr& key, const UnifiedStrLarge& value) {
    return table_.insert_or_assign(key, value);
  }

  bool remove(const UnifiedStr& key) {
    return table_.erase(key) > 0;
  }

  bool get_value(const UnifiedStr& key, UnifiedStrLarge& value) const {
    bool found = false;
    table_.cvisit(key, [&](const auto& kv) {
      value = kv.second;
      found = true;
    });
    return found;
  }

  int64_t size() const {
    return table_.size();
  }
};

// Factory + operations
std::shared_ptr<SeqStringMap> new_seq_string_map_cpp(size_t capacity);
bool seq_string_find_cpp(const std::shared_ptr<SeqStringMap>& m, UnifiedStr& key, UnifiedStrLarge* out_value);
bool seq_string_insert_cpp(const std::shared_ptr<SeqStringMap>& m, UnifiedStr& key, UnifiedStrLarge& value);
bool seq_string_remove_cpp(const std::shared_ptr<SeqStringMap>& m, UnifiedStr& key);
bool seq_string_update_cpp(const std::shared_ptr<SeqStringMap>& m, UnifiedStr& key, UnifiedStrLarge& value);
int64_t seq_string_size_cpp(const std::shared_ptr<SeqStringMap>& m);

} // namespace seqffi

#ifdef __cplusplus
extern "C" {
#endif
// C ABI for FFI
typedef struct seqffi_SeqStringMapOpaque seqffi_SeqStringMapOpaque;

seqffi_SeqStringMapOpaque* new_seq_string_map(size_t capacity);
void free_seq_string_map(seqffi_SeqStringMapOpaque* map);
bool seq_string_find(seqffi_SeqStringMapOpaque* map, UnifiedStr& key, UnifiedStrLarge* out_value);
bool seq_string_insert(seqffi_SeqStringMapOpaque* map, UnifiedStr& key, UnifiedStrLarge& value);
bool seq_string_update(seqffi_SeqStringMapOpaque* map, UnifiedStr& key, UnifiedStrLarge& value);
bool seq_string_remove(seqffi_SeqStringMapOpaque* map, UnifiedStr& key);
int64_t seq_string_size(seqffi_SeqStringMapOpaque* map);
#ifdef __cplusplus
}
#endif 