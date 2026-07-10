/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::any::{Any, TypeId};

/// Any `Send + Sync + 'static` value is a task; there is no central enum to edit.
/// `Sync` lets a worker borrow the payload across `.await`; plain data satisfies it.
pub trait Task: Send + Sync + 'static {}
impl<T: Send + Sync + 'static> Task for T {}

/// Routing key: a Rust type for statically-typed pipelines, or a runtime id for
/// dynamic front-ends (e.g. the Python bindings routing by Python class).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum RouteKey {
    Static(TypeId),
    Dyn(u64),
}

/// A type-erased task plus the engine metadata that rides alongside it.
pub(crate) struct Envelope {
    pub key: RouteKey,
    pub type_name: &'static str,
    pub payload: Box<dyn Any + Send + Sync>,
    pub attempt: u32,
    pub tried: TriedMask,
}

impl Envelope {
    pub fn new<T: Task>(task: T) -> Self {
        Envelope {
            key: RouteKey::Static(TypeId::of::<T>()),
            type_name: std::any::type_name::<T>(),
            payload: Box::new(task),
            attempt: 0,
            tried: TriedMask::empty(),
        }
    }

    pub fn new_dyn(key: u64, type_name: &'static str, payload: Box<dyn Any + Send + Sync>) -> Self {
        Envelope {
            key: RouteKey::Dyn(key),
            type_name,
            payload,
            attempt: 0,
            tried: TriedMask::empty(),
        }
    }
}

/// Bitset of worker-instance ids already attempted (supports up to 64 per group).
#[derive(Clone, Copy, Default)]
pub(crate) struct TriedMask(u64);

impl TriedMask {
    pub fn empty() -> Self {
        TriedMask(0)
    }
    pub fn contains(self, id: u16) -> bool {
        id < 64 && self.0 & (1u64 << id) != 0
    }
    pub fn with(self, id: u16) -> Self {
        if id < 64 {
            TriedMask(self.0 | (1u64 << id))
        } else {
            self
        }
    }
}
