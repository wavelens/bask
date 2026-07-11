/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Durable restore points. A checkpoint is the [`Dedup`](crate::Dedup) primitive made
//! durable and data-carrying: on arrival its payload is materialized and the source
//! rows it covers are recorded, so a re-run skips finished work, prunes covered seeds,
//! and reseeds materialized-but-unconsumed items. Absent any checkpoint type the whole
//! subsystem is inert and the run is byte-for-byte the in-memory engine.

use std::any::Any;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::task::RouteKey;

/// The source rows a task covers: an ordinal set that starts empty (no allocation) and
/// grows only when provenance is active. Sources stamp ordinals, workers inherit them,
/// and routers union them; a checkpoint records the set its item covers. The set is boxed
/// so an inert `Coverage` is one pointer wide on every envelope in the no-checkpoint path.
#[derive(Clone, Default, Debug)]
#[allow(clippy::box_collection)]
pub struct Coverage(Option<Box<BTreeSet<u64>>>);

impl Coverage {
    pub fn empty() -> Self {
        Coverage(None)
    }

    pub fn single(key: u64) -> Self {
        let mut set = BTreeSet::new();
        set.insert(key);
        Coverage(Some(Box::new(set)))
    }

    pub fn is_empty(&self) -> bool {
        self.0.as_ref().is_none_or(|s| s.is_empty())
    }

    pub fn insert(&mut self, key: u64) {
        self.0.get_or_insert_with(Box::default).insert(key);
    }

    pub fn union_with(&mut self, other: &Coverage) {
        let Some(rhs) = other.0.as_ref() else { return };
        let set = self.0.get_or_insert_with(Box::default);
        set.extend(rhs.iter().copied());
    }

    pub fn contains(&self, key: u64) -> bool {
        self.0.as_ref().is_some_and(|s| s.contains(&key))
    }

    /// Whether every row here is also covered by `other` (an empty set is a subset).
    pub fn is_subset_of(&self, other: &Coverage) -> bool {
        match &self.0 {
            None => true,
            Some(set) => set.iter().all(|k| other.contains(*k)),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.0.iter().flat_map(|s| s.iter().copied())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for key in self.iter() {
            out.extend_from_slice(&key.to_le_bytes());
        }
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut cov = Coverage::empty();
        for chunk in bytes.chunks_exact(8) {
            cov.insert(u64::from_le_bytes(chunk.try_into().unwrap()));
        }
        cov
    }
}

/// A materialized checkpoint's lifecycle: `Stored` on arrival, `Consumed` once a worker
/// registered for its type has processed it (the "process later" transition).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Status {
    Stored,
    Consumed,
}

/// A checkpoint item being written to the store: its identity, payload bytes (absent for
/// `key_only`), and the source rows it covers.
pub struct Committed {
    pub name: String,
    pub key: String,
    pub payload: Option<Vec<u8>>,
    pub coverage: Coverage,
}

/// A stored item replayed on the next run when a worker for its type is now registered.
pub struct StoredItem {
    pub key: String,
    pub payload: Vec<u8>,
    pub coverage: Coverage,
}

/// The durable backing for checkpoints: an index of what is stored, the source rows
/// covered so far, and each source's recorded extent. Reads happen once at startup and
/// writes are per checkpoint commit, so a plain single-connection sqlite file suffices;
/// an in-memory implementation ([`MemStore`]) degrades checkpoints to in-run dedup.
pub trait Store: Send + Sync {
    fn statuses(&self) -> anyhow::Result<Vec<(String, String, Status)>>;
    fn covered(&self) -> anyhow::Result<Coverage>;
    fn extents(&self) -> anyhow::Result<HashMap<String, Coverage>>;
    fn stored_items(&self, name: &str) -> anyhow::Result<Vec<StoredItem>>;
    fn commit(&self, rec: &Committed) -> anyhow::Result<()>;
    fn consume(&self, name: &str, key: &str) -> anyhow::Result<()>;
    fn record_extent(&self, source: &str, extent: &Coverage) -> anyhow::Result<()>;
}

impl<S: Store + ?Sized> Store for Arc<S> {
    fn statuses(&self) -> anyhow::Result<Vec<(String, String, Status)>> {
        (**self).statuses()
    }
    fn covered(&self) -> anyhow::Result<Coverage> {
        (**self).covered()
    }
    fn extents(&self) -> anyhow::Result<HashMap<String, Coverage>> {
        (**self).extents()
    }
    fn stored_items(&self, name: &str) -> anyhow::Result<Vec<StoredItem>> {
        (**self).stored_items(name)
    }
    fn commit(&self, rec: &Committed) -> anyhow::Result<()> {
        (**self).commit(rec)
    }
    fn consume(&self, name: &str, key: &str) -> anyhow::Result<()> {
        (**self).consume(name, key)
    }
    fn record_extent(&self, source: &str, extent: &Coverage) -> anyhow::Result<()> {
        (**self).record_extent(source, extent)
    }
}

/// A stored item in memory: its status, optional payload, and coverage.
type MemItem = (Status, Option<Vec<u8>>, Coverage);

#[derive(Default)]
struct MemInner {
    items: HashMap<(String, String), MemItem>,
    extents: HashMap<String, Coverage>,
}

/// A non-persistent [`Store`]: checkpoints still skip and reseed within one process, but
/// nothing survives it. The opt-out from the default `bask.sqlite`.
#[derive(Default)]
pub struct MemStore {
    inner: Mutex<MemInner>,
}

impl Store for MemStore {
    fn statuses(&self) -> anyhow::Result<Vec<(String, String, Status)>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .items
            .iter()
            .map(|((n, k), (s, _, _))| (n.clone(), k.clone(), *s))
            .collect())
    }

    fn covered(&self) -> anyhow::Result<Coverage> {
        let inner = self.inner.lock().unwrap();
        let mut cov = Coverage::empty();
        for (_, _, c) in inner.items.values() {
            cov.union_with(c);
        }
        Ok(cov)
    }

    fn extents(&self) -> anyhow::Result<HashMap<String, Coverage>> {
        Ok(self.inner.lock().unwrap().extents.clone())
    }

    fn stored_items(&self, name: &str) -> anyhow::Result<Vec<StoredItem>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .items
            .iter()
            .filter(|((n, _), (s, p, _))| n == name && *s == Status::Stored && p.is_some())
            .map(|((_, k), (_, p, c))| StoredItem {
                key: k.clone(),
                payload: p.clone().unwrap(),
                coverage: c.clone(),
            })
            .collect())
    }

    fn commit(&self, rec: &Committed) -> anyhow::Result<()> {
        self.inner.lock().unwrap().items.insert(
            (rec.name.clone(), rec.key.clone()),
            (Status::Stored, rec.payload.clone(), rec.coverage.clone()),
        );
        Ok(())
    }

    fn consume(&self, name: &str, key: &str) -> anyhow::Result<()> {
        if let Some(item) = self
            .inner
            .lock()
            .unwrap()
            .items
            .get_mut(&(name.to_string(), key.to_string()))
        {
            item.0 = Status::Consumed;
        }
        Ok(())
    }

    fn record_extent(&self, source: &str, extent: &Coverage) -> anyhow::Result<()> {
        self.inner
            .lock()
            .unwrap()
            .extents
            .insert(source.to_string(), extent.clone());
        Ok(())
    }
}

/// Type-erased checkpoint behavior for one task type: its store identity, whether it
/// carries a payload, and how to key/encode/decode it. Backed by the derived
/// [`CheckpointInfo`] for Rust types and by a callback for dynamic front-ends.
pub trait CheckpointOps: Send + Sync {
    fn name(&self) -> &str;
    fn key_only(&self) -> bool;
    fn key(&self, payload: &(dyn Any + Send + Sync)) -> String;
    fn encode(&self, payload: &(dyn Any + Send + Sync)) -> anyhow::Result<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> anyhow::Result<Box<dyn Any + Send + Sync>>;
}

/// The registered checkpoint types, resolved by routing key. Populated from the derive's
/// inventory (Rust) and from [`checkpoint_dyn`](crate::EngineBuilder::checkpoint_dyn)
/// registrations (front-ends).
#[derive(Default)]
pub(crate) struct Checkpoints {
    by_key: HashMap<RouteKey, Arc<dyn CheckpointOps>>,
}

impl Checkpoints {
    pub fn insert(&mut self, key: RouteKey, ops: Arc<dyn CheckpointOps>) {
        self.by_key.insert(key, ops);
    }

    pub fn get(&self, key: &RouteKey) -> Option<&Arc<dyn CheckpointOps>> {
        self.by_key.get(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&RouteKey, &Arc<dyn CheckpointOps>)> {
        self.by_key.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }
}

/// What to do with a checkpoint task once its store state is known.
pub(crate) enum Admit {
    /// Nothing to run; the item was skipped (`counted` false) or terminally materialized.
    Finished { skipped: bool },
    /// Run the registered worker; on success mark the item consumed.
    RunWorker,
}

/// The runtime coordinator: an in-memory write-through index over the [`Store`] for O(1)
/// skip checks, plus per-source extent accumulation for the current pass.
pub(crate) struct Durability {
    pub checkpoints: Checkpoints,
    store: Arc<dyn Store>,
    index: Mutex<HashMap<(String, String), Status>>,
    running: Mutex<HashSet<(String, String)>>,
    extents: Mutex<HashMap<Arc<str>, Coverage>>,
}

impl Durability {
    pub fn new(checkpoints: Checkpoints, store: Arc<dyn Store>) -> anyhow::Result<Self> {
        let index = store
            .statuses()?
            .into_iter()
            .map(|(n, k, s)| ((n, k), s))
            .collect();
        Ok(Durability {
            checkpoints,
            store,
            index: Mutex::new(index),
            running: Mutex::new(HashSet::new()),
            extents: Mutex::new(HashMap::new()),
        })
    }

    pub fn covered(&self) -> anyhow::Result<Coverage> {
        self.store.covered()
    }

    pub fn extents(&self) -> anyhow::Result<HashMap<String, Coverage>> {
        self.store.extents()
    }

    pub fn stored_items(&self, name: &str) -> anyhow::Result<Vec<StoredItem>> {
        self.store.stored_items(name)
    }

    /// Note a source row minted under `source`, folding it into that source's extent.
    pub fn observe(&self, source: &Arc<str>, key: u64) {
        self.extents
            .lock()
            .unwrap()
            .entry(source.clone())
            .or_default()
            .insert(key);
    }

    /// Persist the extent of every source that completed a full pass this run.
    pub fn record_extents(&self) -> anyhow::Result<()> {
        for (source, extent) in self.extents.lock().unwrap().iter() {
            self.store.record_extent(source, extent)?;
        }
        Ok(())
    }

    /// Decide the fate of an arriving checkpoint, materializing it on first sight. The
    /// key is reserved under the index lock so two racing tasks materialize it once.
    pub fn admit(
        &self,
        ops: &Arc<dyn CheckpointOps>,
        key: &str,
        payload: &(dyn Any + Send + Sync),
        coverage: &Coverage,
        has_worker: bool,
    ) -> anyhow::Result<Admit> {
        let name = ops.name().to_string();
        let prev = {
            let mut index = self.index.lock().unwrap();
            let slot = (name.clone(), key.to_string());
            match index.get(&slot).copied() {
                Some(status) => Some(status),
                None => {
                    index.insert(slot, Status::Stored);
                    None
                }
            }
        };
        match prev {
            Some(Status::Consumed) => Ok(Admit::Finished { skipped: true }),
            // A stored item with a worker runs the "process later" step, but only if we
            // win the claim; a concurrent reseed/re-emit of the same key is skipped.
            Some(Status::Stored) if has_worker && self.claim(&name, key) => Ok(Admit::RunWorker),
            Some(Status::Stored) => Ok(Admit::Finished { skipped: true }),
            None => {
                let bytes = if ops.key_only() {
                    None
                } else {
                    Some(ops.encode(payload)?)
                };
                self.store.commit(&Committed {
                    name: name.clone(),
                    key: key.to_string(),
                    payload: bytes,
                    coverage: coverage.clone(),
                })?;
                if has_worker {
                    self.claim(&name, key);
                    Ok(Admit::RunWorker)
                } else {
                    Ok(Admit::Finished { skipped: false })
                }
            }
        }
    }

    fn claim(&self, name: &str, key: &str) -> bool {
        self.running
            .lock()
            .unwrap()
            .insert((name.to_string(), key.to_string()))
    }

    /// A claimed run finished; mark the item consumed so it never runs again.
    pub fn consume(&self, name: &str, key: &str) -> anyhow::Result<()> {
        self.store.consume(name, key)?;
        self.index
            .lock()
            .unwrap()
            .insert((name.to_string(), key.to_string()), Status::Consumed);
        self.running
            .lock()
            .unwrap()
            .remove(&(name.to_string(), key.to_string()));
        Ok(())
    }

    /// A claimed run was abandoned (failed, retried, or cancelled); release it so a retry
    /// or a later run can claim and re-run it.
    pub fn release(&self, name: &str, key: &str) {
        self.running
            .lock()
            .unwrap()
            .remove(&(name.to_string(), key.to_string()));
    }
}

#[cfg(feature = "checkpoint")]
mod derive_support {
    use std::any::{Any, TypeId};

    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use super::CheckpointOps;
    use crate::task::Task;

    /// A durable restore point defined on the task type. Derive it with
    /// `#[derive(Checkpoint)]` and a `#[key]` field rather than implementing by hand.
    pub trait Checkpoint: Task + Serialize + DeserializeOwned {
        const NAME: &'static str;
        const KEY_ONLY: bool = false;
        fn key(&self) -> String;
    }

    /// The derive's registration record: monomorphized key/encode/decode entry points
    /// gathered by `inventory` so the engine discovers checkpoints with no builder call.
    pub struct CheckpointInfo {
        pub(crate) type_id: fn() -> TypeId,
        name: &'static str,
        key_only: bool,
        key: fn(&(dyn Any + Send + Sync)) -> String,
        encode: fn(&(dyn Any + Send + Sync)) -> anyhow::Result<Vec<u8>>,
        decode: fn(&[u8]) -> anyhow::Result<Box<dyn Any + Send + Sync>>,
    }

    impl CheckpointInfo {
        pub const fn of<C: Checkpoint>() -> Self {
            CheckpointInfo {
                type_id: TypeId::of::<C>,
                name: C::NAME,
                key_only: C::KEY_ONLY,
                key: key_of::<C>,
                encode: encode_of::<C>,
                decode: decode_of::<C>,
            }
        }
    }

    fn key_of<C: Checkpoint>(payload: &(dyn Any + Send + Sync)) -> String {
        payload
            .downcast_ref::<C>()
            .expect("checkpoint payload")
            .key()
    }

    fn encode_of<C: Checkpoint>(payload: &(dyn Any + Send + Sync)) -> anyhow::Result<Vec<u8>> {
        let value = payload.downcast_ref::<C>().expect("checkpoint payload");
        Ok(serde_json::to_vec(value)?)
    }

    fn decode_of<C: Checkpoint>(bytes: &[u8]) -> anyhow::Result<Box<dyn Any + Send + Sync>> {
        Ok(Box::new(serde_json::from_slice::<C>(bytes)?))
    }

    inventory::collect!(CheckpointInfo);

    /// Adapts an inventory record to the object-safe [`CheckpointOps`] the engine uses.
    pub(crate) struct RustCheckpoint(pub &'static CheckpointInfo);

    impl CheckpointOps for RustCheckpoint {
        fn name(&self) -> &str {
            self.0.name
        }
        fn key_only(&self) -> bool {
            self.0.key_only
        }
        fn key(&self, payload: &(dyn Any + Send + Sync)) -> String {
            (self.0.key)(payload)
        }
        fn encode(&self, payload: &(dyn Any + Send + Sync)) -> anyhow::Result<Vec<u8>> {
            (self.0.encode)(payload)
        }
        fn decode(&self, bytes: &[u8]) -> anyhow::Result<Box<dyn Any + Send + Sync>> {
            (self.0.decode)(bytes)
        }
    }

    /// Every checkpoint type the linker gathered, as `(TypeId, ops)` for registration.
    pub(crate) fn registered() -> impl Iterator<Item = (TypeId, std::sync::Arc<dyn CheckpointOps>)>
    {
        inventory::iter::<CheckpointInfo>().map(|info| {
            (
                (info.type_id)(),
                std::sync::Arc::new(RustCheckpoint(info)) as _,
            )
        })
    }
}

#[cfg(feature = "checkpoint")]
pub(crate) use derive_support::registered;
#[cfg(feature = "checkpoint")]
pub use derive_support::{Checkpoint, CheckpointInfo};
