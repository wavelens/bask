/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! The dedup part of the aggregation plane: a test-and-set membership set so a
//! worker can admit each distinct key once (e.g. enqueue each URL a single time).

use std::any::{Any, TypeId};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

/// A registered dedup set, identified by a marker type carrying its `Key`.
pub trait Dedup: Send + Sync + 'static {
    type Key: Eq + Hash + Send + 'static;
}

/// Sharded by key hash so membership is consistent (a key always maps to one shard)
/// while distinct keys contend on different locks.
pub(crate) struct DedupSet<D: Dedup> {
    shards: Vec<Mutex<HashSet<D::Key>>>,
}

impl<D: Dedup> DedupSet<D> {
    fn new(shards: usize) -> Self {
        DedupSet {
            shards: (0..shards.max(1))
                .map(|_| Mutex::new(HashSet::new()))
                .collect(),
        }
    }
    fn first_seen(&self, key: D::Key) -> bool {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % self.shards.len();
        self.shards[idx].lock().unwrap().insert(key)
    }
    fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().unwrap().len()).sum()
    }
}

pub(crate) trait AnyDedup: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn len(&self) -> usize;
}

impl<D: Dedup> AnyDedup for DedupSet<D> {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn len(&self) -> usize {
        DedupSet::len(self)
    }
}

#[derive(Default)]
pub(crate) struct Dedups {
    map: HashMap<TypeId, Arc<dyn AnyDedup>>,
}

impl Dedups {
    pub fn insert<D: Dedup>(&mut self, shards: usize) {
        self.map
            .insert(TypeId::of::<D>(), Arc::new(DedupSet::<D>::new(shards)));
    }

    pub fn first_seen<D: Dedup>(&self, key: D::Key) -> bool {
        match self.map.get(&TypeId::of::<D>()) {
            Some(set) => set
                .as_any()
                .downcast_ref::<DedupSet<D>>()
                .expect("dedup type")
                .first_seen(key),
            None => panic!("dedup {} not registered", std::any::type_name::<D>()),
        }
    }

    pub fn sizes(&self) -> HashMap<TypeId, usize> {
        self.map.iter().map(|(k, v)| (*k, v.len())).collect()
    }
}
