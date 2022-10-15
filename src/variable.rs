use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;

struct Inner<T> {
    value: RwLock<T>,
}

/// Write-side of a variable with read capability to use as single source of truth
///
/// Usage of [`read`] and [`write`] should be non-blocking so that readers can always
/// quickly access the latest value.
#[derive(Clone)]
pub struct Writer<T>(Arc<Inner<T>>);

impl<T> std::fmt::Debug for Writer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Writer").finish()
    }
}

impl<T> Writer<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(Inner {
            value: RwLock::new(value),
        }))
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.0.value.write()
    }

    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.0.value.read()
    }

    pub fn reader(&self) -> Reader<T> {
        Reader(self.0.clone())
    }
}

/// Read-side of a variable, intentionally limited to avoid blocking the writer
#[derive(Clone)]
pub struct Reader<T>(Arc<Inner<T>>);

impl<T> Reader<T> {
    pub fn project<U>(&self, f: impl Fn(&T) -> U) -> U {
        let value = self.0.value.read();
        f(&*value)
    }
}

impl<T: Copy> Reader<T> {
    pub fn get(&self) -> T {
        *self.0.value.read()
    }
}

impl<T: Clone> Reader<T> {
    pub fn cloned(&self) -> T {
        self.0.value.read().clone()
    }
}
