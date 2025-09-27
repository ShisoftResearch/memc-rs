pub const UNIFIED_STR_CAP: usize = 32;
pub const MAP_VAL_BUFFER_CAP: usize = std::mem::size_of::<Record>();

// Reserve the last byte for length information
pub const UNIFIED_STR_DATA_CAP: usize = UNIFIED_STR_CAP - 1;
pub const MAP_VAL_DATA_CAP: usize = MAP_VAL_BUFFER_CAP - 1;

#[repr(C)]
#[derive(Copy, Clone, Debug, Ord, PartialOrd)]
pub struct UnifiedStr {
    pub data: [u8; UNIFIED_STR_CAP],
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct MapValue {
    pub data: [u8; MAP_VAL_BUFFER_CAP],
}

use std::{hash::{BuildHasher, Hash, Hasher}, os::raw::c_void, ptr};
use crate::cache::cache::Record;

// Custom hasher for UnifiedStr using MurmurHash3 x64_128 (reduced to 64-bit)
pub struct UnifiedStrHasher {
    state: u64,
}

impl UnifiedStrHasher {
    pub fn new() -> Self {
        Self { state: 0 }
    }

    #[inline]
    fn rotl64(x: u64, r: i8) -> u64 {
        (x << r) | (x >> (64 - r))
    }

    // MurmurHash3 x64_64 implementation (true 64-bit variant)
    pub fn murmur3_x64_64(data: &[u8], seed: u32) -> u64 {
        const C1: u64 = 0x87c37b91114253d5;
        const C2: u64 = 0x4cf5ad432745937f;

        let len = data.len();
        let nblocks = len / 8;

        let mut h: u64 = seed as u64;

        // Process 8-byte blocks
        for i in 0..nblocks {
            let mut k: u64 = 0;
            
            unsafe {
                libc::memcpy(
                    &mut k as *mut u64 as *mut c_void,
                    data.as_ptr().add(i * 8) as *const c_void,
                    8,
                );
            }

            k = k.wrapping_mul(C1);
            k = Self::rotl64(k, 31);
            k = k.wrapping_mul(C2);
            h ^= k;

            h = Self::rotl64(h, 27);
            h = h.wrapping_mul(5).wrapping_add(0x52dce729);
        }

        // Process remaining bytes
        let tail = &data[nblocks * 8..];
        let mut k: u64 = 0;

        let tail_len = len % 8;
        if tail_len >= 7 { k ^= (tail[6] as u64) << 48; }
        if tail_len >= 6 { k ^= (tail[5] as u64) << 40; }
        if tail_len >= 5 { k ^= (tail[4] as u64) << 32; }
        if tail_len >= 4 { k ^= (tail[3] as u64) << 24; }
        if tail_len >= 3 { k ^= (tail[2] as u64) << 16; }
        if tail_len >= 2 { k ^= (tail[1] as u64) << 8; }
        if tail_len >= 1 {
            k ^= tail[0] as u64;
            k = k.wrapping_mul(C1);
            k = Self::rotl64(k, 31);
            k = k.wrapping_mul(C2);
            h ^= k;
        }

        // Finalization
        h ^= len as u64;
        h = Self::fmix64(h);

        h
    }

    // MurmurHash3 x64_128 implementation, returning lower 64 bits
    pub fn murmur3_x64_128(data: &[u8], seed: u32) -> u64 {
        const C1: u64 = 0x87c37b91114253d5;
        const C2: u64 = 0x4cf5ad432745937f;

        let len = data.len();
        let nblocks = len / 16;

        let mut h1: u64 = seed as u64;
        let mut h2: u64 = seed as u64;

        // Process 16-byte blocks
        for i in 0..nblocks {
            let mut k1: u64 = 0;
            let mut k2: u64 = 0;
            
            unsafe {
                libc::memcpy(
                    &mut k1 as *mut u64 as *mut c_void,
                    data.as_ptr().add(i * 16) as *const c_void,
                    8,
                );
                libc::memcpy(
                    &mut k2 as *mut u64 as *mut c_void,
                    data.as_ptr().add(i * 16 + 8) as *const c_void,
                    8,
                );
            }

            k1 = k1.wrapping_mul(C1);
            k1 = Self::rotl64(k1, 31);
            k1 = k1.wrapping_mul(C2);
            h1 ^= k1;

            h1 = Self::rotl64(h1, 27);
            h1 = h1.wrapping_add(h2);
            h1 = h1.wrapping_mul(5).wrapping_add(0x52dce729);

            k2 = k2.wrapping_mul(C2);
            k2 = Self::rotl64(k2, 33);
            k2 = k2.wrapping_mul(C1);
            h2 ^= k2;

            h2 = Self::rotl64(h2, 31);
            h2 = h2.wrapping_add(h1);
            h2 = h2.wrapping_mul(5).wrapping_add(0x38495ab5);
        }

        // Process remaining bytes
        let tail = &data[nblocks * 16..];
        let mut k1: u64 = 0;
        let mut k2: u64 = 0;

        // Handle tail bytes (fall through pattern) - match C++ exactly
        let tail_len = len % 16;
        if tail_len >= 15 { k2 ^= (tail[14] as u64) << 48; }
        if tail_len >= 14 { k2 ^= (tail[13] as u64) << 40; }
        if tail_len >= 13 { k2 ^= (tail[12] as u64) << 32; }
        if tail_len >= 12 { k2 ^= (tail[11] as u64) << 24; }
        if tail_len >= 11 { k2 ^= (tail[10] as u64) << 16; }
        if tail_len >= 10 { k2 ^= (tail[9] as u64) << 8; }
        if tail_len >= 9 {
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(C2);
            k2 = Self::rotl64(k2, 33);
            k2 = k2.wrapping_mul(C1);
            h2 ^= k2;
        }

        if tail_len >= 8 { k1 ^= (tail[7] as u64) << 56; }
        if tail_len >= 7 { k1 ^= (tail[6] as u64) << 48; }
        if tail_len >= 6 { k1 ^= (tail[5] as u64) << 40; }
        if tail_len >= 5 { k1 ^= (tail[4] as u64) << 32; }
        if tail_len >= 4 { k1 ^= (tail[3] as u64) << 24; }
        if tail_len >= 3 { k1 ^= (tail[2] as u64) << 16; }
        if tail_len >= 2 { k1 ^= (tail[1] as u64) << 8; }
        if tail_len >= 1 {
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(C1);
            k1 = Self::rotl64(k1, 31);
            k1 = k1.wrapping_mul(C2);
            h1 ^= k1;
        }

        // Finalization
        h1 ^= len as u64;
        h2 ^= len as u64;

        h1 = h1.wrapping_add(h2);
        h2 = h2.wrapping_add(h1);

        h1 = Self::fmix64(h1);
        h2 = Self::fmix64(h2);

        h1 = h1.wrapping_add(h2);
        // h2 = h2.wrapping_add(h1); // We only need h1 for 64-bit output

        h1 // Return lower 64 bits
    }

    #[inline]
    fn fmix64(mut k: u64) -> u64 {
        k ^= k >> 33;
        k = k.wrapping_mul(0xff51afd7ed558ccd);
        k ^= k >> 33;
        k = k.wrapping_mul(0xc4ceb9fe1a85ec53);
        k ^= k >> 33;
        k
    }
}

impl Hasher for UnifiedStrHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.state = Self::murmur3_x64_64(bytes, 0);
    }

    fn write_u8(&mut self, i: u8) {
        self.state = Self::murmur3_x64_64(&[i], 0);
    }

    fn write_u64(&mut self, i: u64) {
        self.state = Self::murmur3_x64_64(&i.to_le_bytes(), 0);
    }

    fn finish(&self) -> u64 {
        self.state
    }
}

impl Default for UnifiedStrHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for UnifiedStrHasher {
    fn clone(&self) -> Self {
        Self::new()
    }
}

// BuildHasher implementation for UnifiedStrHasher
impl BuildHasher for UnifiedStrHasher {
    type Hasher = UnifiedStrHasher;

    fn build_hasher(&self) -> Self::Hasher {
        UnifiedStrHasher::new()
    }
}

impl UnifiedStr {
    #[inline]
    pub fn from_bytes(src: &[u8]) -> Self {
        let mut data = [0u8; UNIFIED_STR_CAP];
        let len = core::cmp::min(src.len(), UNIFIED_STR_DATA_CAP);
        data[1..len + 1].copy_from_slice(&src[..len]);
        // Store the original length in the last byte
        data[0] = len as u8;
        Self { data }
    }
    #[inline]
    pub fn as_bytes_trimmed(&self) -> &[u8] {
        let stored_len = self.len();
        let len = core::cmp::min(stored_len, UNIFIED_STR_DATA_CAP);
        &self.data[1..len+1]
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.data[0] as usize
    }
}

impl MapValue {
    #[inline]
    pub fn from_record(record: Record) -> Self {
        let mut buffer = Self {
            data: [0u8; MAP_VAL_BUFFER_CAP],
        };
        unsafe {
            ptr::write(&mut buffer.data as *mut [u8; MAP_VAL_BUFFER_CAP] as *mut Record, record);
        }
        return buffer;
    }
    
    #[inline]
    pub fn to_record_ref(&self) -> &'static Record {
        unsafe {
            return &*(&self.data as *const [u8; MAP_VAL_BUFFER_CAP] as *const Record);
        }
    }

    #[inline]
    pub fn to_record(&self) -> Record {
        unsafe {
            return ptr::read(&self.data as *const [u8; MAP_VAL_BUFFER_CAP] as *const Record);
        }
    }
}

impl PartialEq for UnifiedStr {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            libc::memcmp(
                self.data.as_ptr() as *const c_void,
                other.data.as_ptr() as *const c_void,
                UNIFIED_STR_CAP,
            ) == 0
        }
    }
}

impl Eq for UnifiedStr {}

impl Hash for UnifiedStr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use the trimmed bytes for hashing to avoid including padding
        let trimmed_bytes = self.as_bytes_trimmed();
        trimmed_bytes.hash(state);
    }
}