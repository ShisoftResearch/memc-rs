pub const UNIFIED_STR_CAP: usize = 32;
pub const UNIFIED_STR_LARGE_CAP: usize = 32;

#[repr(C)]
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct UnifiedStr {
    pub data: [u8; UNIFIED_STR_CAP],
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct UnifiedStrLarge {
    pub data: [u8; UNIFIED_STR_LARGE_CAP],
}

use std::hash::{BuildHasher, Hash, Hasher};

pub struct UnifiedStrHasher {
    state: u64,
}

impl UnifiedStrHasher {
    pub fn new() -> Self {
        Self {
            state: 0xcbf29ce484222325,
        }
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

impl Hasher for UnifiedStrHasher {
    fn write(&mut self, bytes: &[u8]) {
        // Fast-path 8 bytes
        if bytes.len() == 8 {
            let chunk = u64::from_le_bytes(bytes.try_into().unwrap());
            self.state ^= chunk;
            self.state = self.state.wrapping_mul(0x100000001b3);
            return;
        }
        // Process in 8-byte chunks up to fixed capacity limits
        const CHUNK: usize = 8;
        let full_chunks = bytes.len() / CHUNK;
        for i in 0..full_chunks {
            let start = i * CHUNK;
            let chunk = u64::from_le_bytes(bytes[start..start + CHUNK].try_into().unwrap());
            self.state ^= chunk;
            self.state = self.state.wrapping_mul(0x100000001b3);
        }
        for b in &bytes[full_chunks * CHUNK..] {
            self.state ^= *b as u64;
            self.state = self.state.wrapping_mul(0x100000001b3);
        }
    }
    fn finish(&self) -> u64 {
        self.state
    }
}

impl BuildHasher for UnifiedStrHasher {
    type Hasher = UnifiedStrHasher;
    fn build_hasher(&self) -> Self::Hasher {
        UnifiedStrHasher::new()
    }
}

impl Hash for UnifiedStr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.data[..]);
    }
}

impl Hash for UnifiedStrLarge {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.data[..]);
    }
}

impl UnifiedStr {
    #[inline]
    pub fn from_bytes(src: &[u8]) -> Self {
        let mut data = [0u8; UNIFIED_STR_CAP];
        let len = core::cmp::min(src.len(), UNIFIED_STR_CAP);
        data[..len].copy_from_slice(&src[..len]);
        Self { data }
    }
    #[inline]
    pub fn from_str(s: &str) -> Self {
        Self::from_bytes(s.as_bytes())
    }
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
    #[inline]
    pub fn as_bytes_trimmed(&self) -> &[u8] {
        let len = self
            .data
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(UNIFIED_STR_CAP);
        &self.data[..len]
    }
}

impl UnifiedStrLarge {
    #[inline]
    pub fn from_bytes(src: &[u8]) -> Self {
        let mut data = [0u8; UNIFIED_STR_LARGE_CAP];
        let len = core::cmp::min(src.len(), UNIFIED_STR_LARGE_CAP);
        data[..len].copy_from_slice(&src[..len]);
        Self { data }
    }
    #[inline]
    pub fn from_str(s: &str) -> Self {
        Self::from_bytes(s.as_bytes())
    }
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
    #[inline]
    pub fn as_bytes_trimmed(&self) -> &[u8] {
        let len = self
            .data
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(UNIFIED_STR_LARGE_CAP);
        &self.data[..len]
    }
    #[inline]
    pub fn len_trimmed(&self) -> usize {
        self.as_bytes_trimmed().len()
    }
}
