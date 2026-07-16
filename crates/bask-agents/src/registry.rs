/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::OnceLock;

use bask_core::Context;
use futures::future::BoxFuture;

/// A task a model may emit: describable as a JSON schema and reconstructable from tool-call
/// arguments. Implemented via `#[derive(AgentTask)]`.
pub trait AgentTask: bask_core::Task + serde::de::DeserializeOwned + schemars::JsonSchema {
    const NAME: &'static str;
    fn description() -> Option<&'static str> {
        None
    }
}

pub(crate) type EmitFn =
    for<'a> fn(&'a Context, serde_json::Value) -> BoxFuture<'a, anyhow::Result<()>>;

pub(crate) struct ToolEntry {
    pub name: &'static str,
    pub description: Option<&'static str>,
    pub schema: serde_json::Value,
    pub emit: EmitFn,
}

/// The record a target submits via `inventory` so the engine discovers it with no builder call.
pub struct AgentTaskInfo {
    type_id: fn() -> TypeId,
    name: fn() -> &'static str,
    description: fn() -> Option<&'static str>,
    schema: fn() -> serde_json::Value,
    emit: EmitFn,
}

impl AgentTaskInfo {
    pub const fn of<T: AgentTask>() -> Self {
        AgentTaskInfo {
            type_id: TypeId::of::<T>,
            name: name_of::<T>,
            description: <T as AgentTask>::description,
            schema: schema_of::<T>,
            emit: emit_of::<T>,
        }
    }
}

fn name_of<T: AgentTask>() -> &'static str {
    <T as AgentTask>::NAME
}

fn schema_of<T: schemars::JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(T)).unwrap_or(serde_json::Value::Null)
}

fn emit_of<T: AgentTask>(ctx: &Context, args: serde_json::Value) -> BoxFuture<'_, anyhow::Result<()>> {
    Box::pin(async move {
        let task: T = serde_json::from_value(args)?;
        ctx.emit(task).await?;
        Ok(())
    })
}

inventory::collect!(AgentTaskInfo);

/// Built once from every `inventory`-submitted target, keyed by `TypeId`.
pub(crate) fn registry() -> &'static HashMap<TypeId, ToolEntry> {
    static REGISTRY: OnceLock<HashMap<TypeId, ToolEntry>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        inventory::iter::<AgentTaskInfo>()
            .map(|info| {
                (
                    (info.type_id)(),
                    ToolEntry {
                        name: (info.name)(),
                        description: (info.description)(),
                        schema: (info.schema)(),
                        emit: info.emit,
                    },
                )
            })
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize, schemars::JsonSchema)]
    struct Ping {
        #[allow(dead_code)]
        message: String,
    }
    impl AgentTask for Ping {
        const NAME: &'static str = "Ping";
    }
    inventory::submit! { AgentTaskInfo::of::<Ping>() }

    #[test]
    fn registry_resolves_registered_task() {
        let entry = registry().get(&TypeId::of::<Ping>()).expect("Ping registered");
        assert_eq!(entry.name, "Ping");
        assert_eq!(entry.description, None);
        let props = entry.schema.get("properties").expect("schema has properties");
        assert!(props.get("message").is_some());
    }
}
