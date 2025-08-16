#pragma once
#include <stddef.h>
#include <stdint.h>
#include <cstring>

#define UNIFIED_STR_CAP 16
#define MAP_VAL_BUFFER_CAP 48 // sizeof(Record) in Rust

// Reserve the last byte for length information
#define UNIFIED_STR_DATA_CAP (UNIFIED_STR_CAP - 1)
#define MAP_VAL_DATA_CAP (MAP_VAL_BUFFER_CAP - 1)

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint8_t data[UNIFIED_STR_CAP];
} UnifiedStr;

typedef struct {
    uint8_t data[MAP_VAL_BUFFER_CAP];
} MapValue;

#ifdef __cplusplus
}
// Shared hash and equality for UnifiedStr
struct UnifiedStrHash {
  size_t operator()(const UnifiedStr& s) const {
    // Optimized FNV-1a hash processing 8 bytes at a time for better performance
    // Exclude the last byte (length byte) from hashing
    size_t h = 0xcbf29ce484222325;
    
    // Process 8 bytes at a time as uint64_t operations
    const size_t full_chunks = UNIFIED_STR_DATA_CAP / 8;
    for (size_t chunk = 0; chunk < full_chunks; ++chunk) {
      // Read 8 bytes as uint64_t (assuming little-endian, which is most common)
      uint64_t chunk_data;
      std::memcpy(&chunk_data, &s.data[chunk * 8], sizeof(uint64_t));
      
      // Single XOR and multiply operation for 8 bytes
      h = (h ^ chunk_data) * 0x100000001b3;
    }
    
    // Process remaining bytes individually (0-7 bytes)
    for (size_t i = full_chunks * 8; i < UNIFIED_STR_DATA_CAP; ++i) {
      h = (h ^ s.data[i]) * 0x100000001b3;
    }
    
    return h;
  }
};

// FNV-1a hash for uint64_t keys, matching UnifiedStrHash algorithm
struct UnifiedStrHash64 {
  size_t operator()(const uint64_t& v) const {
    // Optimized: process the entire uint64_t at once instead of byte-by-byte
    size_t h = 0xcbf29ce484222325;
    h = (h ^ v) * 0x100000001b3;
    return h;
  }
};

struct UnifiedStrEqual {
  bool operator()(const UnifiedStr& a, const UnifiedStr& b) const {
    // Compare only the data bytes, excluding the length byte
    return std::memcmp(a.data, b.data, UNIFIED_STR_DATA_CAP) == 0;
  }
};

// Hash and equality for MapValue
struct MapValueHash {
  size_t operator()(const MapValue& s) const {
    // Optimized FNV-1a hash processing 8 bytes at a time for better performance
    // Exclude the last byte (length byte) from hashing
    size_t h = 0xcbf29ce484222325;
    
    // Process 8 bytes at a time as uint64_t operations
    const size_t full_chunks = MAP_VAL_DATA_CAP / 8;
    for (size_t chunk = 0; chunk < full_chunks; ++chunk) {
      // Read 8 bytes as uint64_t (assuming little-endian, which is most common)
      uint64_t chunk_data;
      std::memcpy(&chunk_data, &s.data[chunk * 8], sizeof(uint64_t));
      
      // Single XOR and multiply operation for 8 bytes
      h = (h ^ chunk_data) * 0x100000001b3;
    }
    
    // Process remaining bytes individually (0-7 bytes)
    for (size_t i = full_chunks * 8; i < MAP_VAL_DATA_CAP; ++i) {
      h = (h ^ s.data[i]) * 0x100000001b3;
    }
    
    return h;
  }
};

struct MapValueEqual {
  bool operator()(const MapValue& a, const MapValue& b) const {
    // Compare only the data bytes, excluding the length byte
    return std::memcmp(a.data, b.data, MAP_VAL_DATA_CAP) == 0;
  }
};

// Patch: Mark UnifiedStr and MapValue as trivially copyable for C++ type traits
namespace std {
  template<>
  struct is_trivially_copyable<UnifiedStr> : std::true_type {};
  
  template<>
  struct is_trivially_copyable<MapValue> : std::true_type {};
}
#endif 