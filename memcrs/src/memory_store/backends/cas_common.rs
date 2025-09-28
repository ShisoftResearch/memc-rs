use crate::{
    cache::cache::{CacheMetaData, SetStatus},
    cache::error::CacheError,
    memcache::store::Record,
    memory_store::store::Peripherals,
};

/// Common CAS (Check and Set) operations that can be shared across all backends
pub struct CasOperations;

impl CasOperations {
    /// Check if two CAS values match for an update operation
    pub fn check_cas_match(existing_cas: u32, provided_cas: u32) -> bool {
        existing_cas == provided_cas
    }

    /// Handle CAS logic for set operations and return the new CAS value
    pub fn handle_set_cas(record: &mut Record, peripherals: &Peripherals) -> (u32, bool) {
        if record.header.cas > 0 {
            // For existing CAS, increment it
            record.header.cas += 1;
            record.header.timestamp = peripherals.timestamp();
            (record.header.cas, true) // true indicates this was an existing CAS
        } else {
            // For new records, assign CAS = 1
            record.header.cas = 1;
            record.header.timestamp = peripherals.timestamp();
            (record.header.cas, false) // false indicates this was a new record
        }
    }

    /// Determine if a delete operation should proceed based on CAS
    pub fn should_delete(header: &CacheMetaData, existing_cas: Option<u32>) -> Result<bool, CacheError> {
        if header.cas == 0 {
            // CAS = 0 means delete without checking
            Ok(true)
        } else if let Some(existing) = existing_cas {
            // Check if CAS matches
            if existing == header.cas {
                Ok(true)
            } else {
                Err(CacheError::KeyExists)
            }
        } else {
            // Record doesn't exist but CAS was provided
            Err(CacheError::NotFound)
        }
    }

    /// Return appropriate CAS mismatch error
    pub fn cas_mismatch_error() -> CacheError {
        CacheError::KeyExists
    }

    /// Return appropriate not found error
    pub fn not_found_error() -> CacheError {
        CacheError::NotFound
    }

    /// Execute a set operation with proper CAS logic
    pub fn execute_set_operation<F>(
        record: &mut Record,
        peripherals: &Peripherals,
        check_existing: F,
    ) -> Result<SetStatus, CacheError>
    where
        F: FnOnce() -> Option<Record>,
    {
        if record.header.cas > 0 {
            // Check if record exists
            if let Some(existing_record) = check_existing() {
                if Self::check_cas_match(existing_record.header.cas, record.header.cas) {
                    // CAS matches, update the record
                    let (cas, _) = Self::handle_set_cas(record, peripherals);
                    Ok(SetStatus { cas })
                } else {
                    // CAS doesn't match
                    Err(Self::cas_mismatch_error())
                }
            } else {
                // Record doesn't exist, but we have a CAS value
                // This means it's an initial set with a specific CAS
                let (cas, _) = Self::handle_set_cas(record, peripherals);
                Ok(SetStatus { cas })
            }
        } else {
            // CAS is 0, always set (insert or update) and assign cas = 1
            let (cas, _) = Self::handle_set_cas(record, peripherals);
            Ok(SetStatus { cas })
        }
    }

    /// Execute a delete operation with proper CAS logic
    pub fn execute_delete_operation<F1, F2>(
        header: &CacheMetaData,
        check_existing: F1,
        perform_delete: F2,
    ) -> Result<Record, CacheError>
    where
        F1: FnOnce() -> Option<Record>,
        F2: FnOnce() -> Option<Record>,
    {
        if header.cas == 0 {
            // If CAS is 0, delete without CAS checking
            check_existing()
                .ok_or_else(Self::not_found_error)?;
            perform_delete().ok_or_else(Self::not_found_error)
        } else {
            // Check if the record exists and CAS matches
            if let Some(existing_record) = check_existing() {
                if Self::check_cas_match(existing_record.header.cas, header.cas) {
                    // CAS matches, remove the record
                    perform_delete().ok_or_else(Self::not_found_error)
                } else {
                    // CAS doesn't match
                    Err(Self::cas_mismatch_error())
                }
            } else {
                Err(Self::not_found_error())
            }
        }
    }
}
