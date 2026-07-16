/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Optional per-task emission constraint. A task type implementing [`EmitPolicy`] and
//! registered on the engine may only emit the task types it declares; internally the set
//! of policies forms a directed graph.

use std::any::TypeId;
use std::collections::{HashMap, HashSet};

use crate::task::{RouteKey, Task};

/// Declares which task types a task is allowed to emit. Register with
/// [`EngineBuilder::emit_policy`](crate::EngineBuilder::emit_policy) or the
/// `#[derive(EmitPolicy)]` macro; an unregistered task emits freely.
pub trait EmitPolicy: Task {
    fn declare(allow: &mut Allow);
}

/// Collects the task types an [`EmitPolicy`] permits.
#[derive(Default)]
pub struct Allow {
    pub(crate) targets: Vec<(TypeId, &'static str)>,
}

impl Allow {
    /// Permit this task to emit `T`.
    pub fn allow<T: Task>(&mut self) -> &mut Self {
        self.targets
            .push((TypeId::of::<T>(), std::any::type_name::<T>()));
        self
    }

    /// The task types this policy permits, for tools that reflect a declaration.
    pub fn targets(&self) -> &[(TypeId, &'static str)] {
        &self.targets
    }
}

/// The allowed out-edges of one constrained source.
pub(crate) struct Node {
    from: &'static str,
    targets: HashSet<RouteKey>,
    names: Vec<&'static str>,
}

impl Node {
    pub(crate) fn new(from: &'static str, targets: Vec<(RouteKey, &'static str)>) -> Self {
        Node {
            from,
            targets: targets.iter().map(|(k, _)| *k).collect(),
            names: targets.iter().map(|(_, n)| *n).collect(),
        }
    }
}

/// The engine-wide emission graph, keyed uniformly over static and dynamic routing keys.
#[derive(Default)]
pub(crate) struct EmitPolicies {
    pub(crate) map: HashMap<RouteKey, Node>,
}

impl EmitPolicies {
    pub fn insert_static<T: EmitPolicy>(&mut self) {
        let mut allow = Allow::default();
        T::declare(&mut allow);
        let targets = allow
            .targets
            .into_iter()
            .map(|(id, n)| (RouteKey::Static(id), n))
            .collect();
        self.map.insert(
            RouteKey::Static(TypeId::of::<T>()),
            Node::new(std::any::type_name::<T>(), targets),
        );
    }

    pub fn insert_dyn(&mut self, key: u64, from: &'static str, allowed: Vec<(u64, &'static str)>) {
        let targets = allowed
            .into_iter()
            .map(|(k, n)| (RouteKey::Dyn(k), n))
            .collect();
        self.map
            .insert(RouteKey::Dyn(key), Node::new(from, targets));
    }

    /// `Ok` if `from` is unconstrained or `to` is a declared target; otherwise the
    /// terminal [`Error::EmitNotAllowed`](crate::Error::EmitNotAllowed).
    pub fn check(&self, from: RouteKey, to: RouteKey, to_name: &'static str) -> crate::Result<()> {
        if let Some(node) = self.map.get(&from)
            && !node.targets.contains(&to)
        {
            return Err(crate::Error::EmitNotAllowed {
                from: node.from,
                to: to_name,
                allowed: node.names.clone(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Src;
    struct Ok1;
    struct Bad;

    impl EmitPolicy for Src {
        fn declare(allow: &mut Allow) {
            allow.allow::<Ok1>();
        }
    }

    fn key<T: 'static>() -> RouteKey {
        RouteKey::Static(TypeId::of::<T>())
    }

    #[test]
    fn allows_declared_target_and_rejects_others() {
        let mut policies = EmitPolicies::default();
        policies.insert_static::<Src>();

        assert!(policies.check(key::<Src>(), key::<Ok1>(), "Ok1").is_ok());

        let err = policies
            .check(key::<Src>(), key::<Bad>(), "Bad")
            .unwrap_err();
        assert!(matches!(err, crate::Error::EmitNotAllowed { .. }));
    }

    #[test]
    fn unconstrained_source_emits_freely() {
        let policies = EmitPolicies::default();
        assert!(policies.check(key::<Src>(), key::<Bad>(), "Bad").is_ok());
    }

    #[test]
    fn dynamic_keys_are_enforced() {
        let mut policies = EmitPolicies::default();
        policies.insert_dyn(1, "Src", vec![(2, "Ok")]);
        assert!(
            policies
                .check(RouteKey::Dyn(1), RouteKey::Dyn(2), "Ok")
                .is_ok()
        );
        assert!(
            policies
                .check(RouteKey::Dyn(1), RouteKey::Dyn(3), "No")
                .is_err()
        );
    }

    #[test]
    fn declared_targets_are_readable() {
        let mut allow = Allow::default();
        Src::declare(&mut allow);
        assert_eq!(allow.targets().len(), 1);
        assert!(allow.targets().iter().any(|(_, name)| name.ends_with("Ok1")));
    }
}

#[cfg(feature = "macros")]
mod derive_support {
    use std::any::TypeId;

    use super::{Allow, EmitPolicies, EmitPolicy, Node};
    use crate::task::RouteKey;

    /// The derive's registration record, gathered by `inventory` so the engine discovers a
    /// policy with no builder call.
    pub struct EmitPolicyInfo {
        type_id: fn() -> TypeId,
        type_name: fn() -> &'static str,
        declare: fn(&mut Allow),
    }

    impl EmitPolicyInfo {
        pub const fn of<T: EmitPolicy>() -> Self {
            EmitPolicyInfo {
                type_id: TypeId::of::<T>,
                type_name: std::any::type_name::<T>,
                declare: <T as EmitPolicy>::declare,
            }
        }
    }

    inventory::collect!(EmitPolicyInfo);

    impl EmitPolicies {
        /// Fold every `inventory`-submitted policy into the registry.
        pub(crate) fn insert_registered(&mut self) {
            for info in inventory::iter::<EmitPolicyInfo>() {
                let mut allow = Allow::default();
                (info.declare)(&mut allow);
                let targets = allow
                    .targets
                    .into_iter()
                    .map(|(id, n)| (RouteKey::Static(id), n))
                    .collect();
                self.map.insert(
                    RouteKey::Static((info.type_id)()),
                    Node::new((info.type_name)(), targets),
                );
            }
        }
    }
}

#[cfg(feature = "macros")]
pub use derive_support::EmitPolicyInfo;
