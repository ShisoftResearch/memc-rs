#pragma once
#include <stddef.h>
#include <stdint.h>
#include <cstring>

#define UNIFIED_STR_CAP 32
#define MAP_VAL_BUFFER_CAP 48 // sizeof(Record) in Rust

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint8_t data[UNIFIED_STR_CAP];
} UnifiedStr;

typedef struct {
    uint8_t data[MAP_VAL_BUFFER_CAP];
} MapValue;

// Compatibility alias used by seq_string_wrapper interfaces.
typedef MapValue UnifiedStrLarge;

#ifdef __cplusplus
}

// Shared hash and equality for UnifiedStr using MurmurHash3 x64_64
struct UnifiedStrHash {
  size_t operator()(const UnifiedStr& s) const {
    return murmur3_x64_64(s.data, UNIFIED_STR_CAP, 0);
  }

public:
  // MurmurHash3 x64_64 implementation (true 64-bit variant)
  static inline uint64_t murmur3_x64_64(const uint8_t* data, size_t len, uint32_t seed) {
    const uint64_t c1 = 0x87c37b91114253d5ULL;
    const uint64_t c2 = 0x4cf5ad432745937fULL;

    const size_t nblocks = len / 8;

    uint64_t h = seed;

    // Process 8-byte blocks
    for (size_t i = 0; i < nblocks; i++) {
      uint64_t k;
      std::memcpy(&k, &data[i * 8], sizeof(uint64_t));

      k *= c1;
      k = rotl64(k, 31);
      k *= c2;
      h ^= k;

      h = rotl64(h, 27);
      h = h * 5 + 0x52dce729;
    }

    // Process remaining bytes
    const uint8_t* tail = data + nblocks * 8;
    uint64_t k = 0;

    const size_t tail_len = len % 8;
    if (tail_len >= 7) k ^= static_cast<uint64_t>(tail[6]) << 48;
    if (tail_len >= 6) k ^= static_cast<uint64_t>(tail[5]) << 40;
    if (tail_len >= 5) k ^= static_cast<uint64_t>(tail[4]) << 32;
    if (tail_len >= 4) k ^= static_cast<uint64_t>(tail[3]) << 24;
    if (tail_len >= 3) k ^= static_cast<uint64_t>(tail[2]) << 16;
    if (tail_len >= 2) k ^= static_cast<uint64_t>(tail[1]) << 8;
    if (tail_len >= 1) {
      k ^= static_cast<uint64_t>(tail[0]);
      k *= c1;
      k = rotl64(k, 31);
      k *= c2;
      h ^= k;
    }

    // Finalization
    h ^= len;
    h = fmix64(h);

    return h;
  }

  // MurmurHash3 x64_128 implementation, returning lower 64 bits
  static inline uint64_t murmur3_x64_128(const uint8_t* data, size_t len, uint32_t seed) {
    const uint64_t c1 = 0x87c37b91114253d5ULL;
    const uint64_t c2 = 0x4cf5ad432745937fULL;

    const size_t nblocks = len / 16;

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    // Process 16-byte blocks
    for (size_t i = 0; i < nblocks; i++) {
      uint64_t k1, k2;
      std::memcpy(&k1, &data[i * 16], sizeof(uint64_t));
      std::memcpy(&k2, &data[i * 16 + 8], sizeof(uint64_t));

      k1 *= c1;
      k1 = rotl64(k1, 31);
      k1 *= c2;
      h1 ^= k1;

      h1 = rotl64(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729;

      k2 *= c2;
      k2 = rotl64(k2, 33);
      k2 *= c1;
      h2 ^= k2;

      h2 = rotl64(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5;
    }

    // Process remaining bytes
    const uint8_t* tail = data + nblocks * 16;
    uint64_t k1 = 0;
    uint64_t k2 = 0;

    // Handle tail bytes (fall through pattern)
    if (len % 16 >= 15) k2 ^= static_cast<uint64_t>(tail[14]) << 48;
    if (len % 16 >= 14) k2 ^= static_cast<uint64_t>(tail[13]) << 40;
    if (len % 16 >= 13) k2 ^= static_cast<uint64_t>(tail[12]) << 32;
    if (len % 16 >= 12) k2 ^= static_cast<uint64_t>(tail[11]) << 24;
    if (len % 16 >= 11) k2 ^= static_cast<uint64_t>(tail[10]) << 16;
    if (len % 16 >= 10) k2 ^= static_cast<uint64_t>(tail[9]) << 8;
    if (len % 16 >= 9) {
      k2 ^= static_cast<uint64_t>(tail[8]);
      k2 *= c2;
      k2 = rotl64(k2, 33);
      k2 *= c1;
      h2 ^= k2;
    }

    if (len % 16 >= 8) k1 ^= static_cast<uint64_t>(tail[7]) << 56;
    if (len % 16 >= 7) k1 ^= static_cast<uint64_t>(tail[6]) << 48;
    if (len % 16 >= 6) k1 ^= static_cast<uint64_t>(tail[5]) << 40;
    if (len % 16 >= 5) k1 ^= static_cast<uint64_t>(tail[4]) << 32;
    if (len % 16 >= 4) k1 ^= static_cast<uint64_t>(tail[3]) << 24;
    if (len % 16 >= 3) k1 ^= static_cast<uint64_t>(tail[2]) << 16;
    if (len % 16 >= 2) k1 ^= static_cast<uint64_t>(tail[1]) << 8;
    if (len % 16 >= 1) {
      k1 ^= static_cast<uint64_t>(tail[0]);
      k1 *= c1;
      k1 = rotl64(k1, 31);
      k1 *= c2;
      h1 ^= k1;
    }

    // Finalization
    h1 ^= len;
    h2 ^= len;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    // h2 += h1; // We only need h1 for 64-bit output

    return h1; // Return lower 64 bits
  }

  static inline uint64_t rotl64(uint64_t x, int8_t r) {
    return (x << r) | (x >> (64 - r));
  }

  static inline uint64_t fmix64(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
  }
};

// MurmurHash3 hash for uint64_t keys, matching UnifiedStrHash algorithm
struct UnifiedStrHash64 {
  size_t operator()(const uint64_t& v) const {
    // Use the same MurmurHash3 implementation as UnifiedStrHash
    return UnifiedStrHash::murmur3_x64_64(reinterpret_cast<const uint8_t*>(&v), sizeof(uint64_t), 0);
  }
};

// Note: UnifiedStrLargeHash removed.

struct UnifiedStrEqual {
  bool operator()(const UnifiedStr& a, const UnifiedStr& b) const {
    // Compare only the data bytes, excluding the length byte
    return std::memcmp(a.data, b.data, UNIFIED_STR_CAP) == 0;
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