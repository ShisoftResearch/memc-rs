#include "tbb_string_wrapper.h"
#include "unified_str.h"
#include <cstring>

extern "C" {

using Table = tbbffi::StringMapWrapper::Table;

bool tbb_string_insert(tbbffi::StringMapWrapper* m, UnifiedStr& key, MapValue& value) {
    return m->map.insert({key, value});
}

bool tbb_string_get(tbbffi::StringMapWrapper* m, UnifiedStr& key, MapValue* out_value) {
    Table::const_accessor acc;
    bool found = m->map.find(acc, key);
    if (found && out_value != nullptr) {
        *out_value = acc->second;
    }
    return found;
}

bool tbb_string_remove(tbbffi::StringMapWrapper* m, UnifiedStr& key) {
    return m->map.erase(key);
}

bool tbb_string_update(tbbffi::StringMapWrapper* m, UnifiedStr& key, MapValue& value) {
    Table::accessor acc;
    if (m->map.insert(acc, key)) {
        acc->second = value;
        return true;
    } else {
        acc->second = value;
        return true;
    }
}

tbbffi::StringMapWrapper* new_tbb_string_map(size_t capacity) {
    return new tbbffi::StringMapWrapper(capacity);
}

void free_tbb_string_map(tbbffi::StringMapWrapper* map) {
    delete map;
}

} // extern "C" 