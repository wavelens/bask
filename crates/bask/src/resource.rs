/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::task::TriedMask;

/// Key/value tags an instance self-describes with (gpu type, node, region), read by
/// attribute-aware selection to steer a retry onto (or away from) matching resources.
#[derive(Clone, Default)]
pub struct Attrs(BTreeMap<Box<str>, Box<str>>);

impl Attrs {
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|v| &**v)
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub(crate) fn set(&mut self, key: &str, value: &str) {
        self.0.insert(key.into(), value.into());
    }
}

/// Which instance a (re)dispatch may land on, evaluated against the instance that just
/// failed. `AvoidTried` is the zero-config default; the attribute variants express
/// resource affinity ("retry on the same GPU", "a different GPU type", "any GPU").
#[derive(Clone)]
pub enum Select {
    /// Any instance, including the one that just failed.
    Any,
    /// Any instance except those already tried for this task.
    AvoidTried,
    /// Pin to the instance that just ran (keep a warm resource).
    SameInstance,
    /// An instance whose attribute `key` equals the failed instance's.
    SameAttr(String),
    /// An instance whose attribute `key` differs from the failed instance's.
    DifferentAttr(String),
    /// A user predicate over instance attributes.
    Where(Arc<dyn Fn(&Attrs) -> bool + Send + Sync>),
}

impl Select {
    /// Whether an empty candidate set should reset the tried-mask and fall back to any
    /// instance. True only for the exhaustible defaults; a targeted constraint that
    /// matches nobody is a terminal failure rather than a silent reroute.
    pub(crate) fn resets(&self) -> bool {
        matches!(self, Select::Any | Select::AvoidTried)
    }

    pub(crate) fn eligible(
        &self,
        cand_id: u16,
        cand: &Attrs,
        tried: TriedMask,
        last: Option<u16>,
        last_attrs: Option<&Attrs>,
    ) -> bool {
        match self {
            Select::Any => true,
            Select::AvoidTried => !tried.contains(cand_id),
            Select::SameInstance => Some(cand_id) == last,
            Select::SameAttr(k) => match last_attrs {
                Some(prev) => prev.get(k) == cand.get(k),
                None => !tried.contains(cand_id),
            },
            Select::DifferentAttr(k) => match last_attrs {
                Some(prev) => prev.get(k) != cand.get(k),
                None => !tried.contains(cand_id),
            },
            Select::Where(pred) => pred(cand),
        }
    }
}
