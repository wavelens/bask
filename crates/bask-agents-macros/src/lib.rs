/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! `#[derive(AgentTask)]`: implements `bask_agents::AgentTask` and registers the target with
//! the engine via `inventory`, so no builder call is needed.

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use quote::{format_ident, quote};
use syn::{DeriveInput, LitStr, parse_macro_input};

/// Resolve the path that re-exports `AgentTask`/`AgentTaskInfo`/`inventory`: `bask-agents`
/// directly, else the `bask` umbrella's `agents` module.
fn agents_root() -> proc_macro2::TokenStream {
    if let Ok(found) = crate_name("bask-agents") {
        return match found {
            FoundCrate::Itself => quote!(::bask_agents),
            FoundCrate::Name(name) => {
                let ident = format_ident!("{}", name);
                quote!(::#ident)
            }
        };
    }
    if let Ok(FoundCrate::Name(name)) = crate_name("bask") {
        let ident = format_ident!("{}", name);
        return quote!(::#ident::agents);
    }
    quote!(::bask_agents)
}

/// Derive `AgentTask`. Container attributes: `#[agent(name = "...")]` overrides the tool name
/// (default the type name); `#[agent(description = "...")]` sets the tool description.
#[proc_macro_derive(AgentTask, attributes(agent))]
pub fn derive_agent_task(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ty = &input.ident;

    let mut name = ty.to_string();
    let mut description: Option<String> = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("agent") {
            continue;
        }
        let parsed = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                name = meta.value()?.parse::<LitStr>()?.value();
            } else if meta.path.is_ident("description") {
                description = Some(meta.value()?.parse::<LitStr>()?.value());
            } else {
                return Err(meta.error("unknown agent attribute"));
            }
            Ok(())
        });
        if let Err(err) = parsed {
            return err.into_compile_error().into();
        }
    }

    let (impl_g, ty_g, where_g) = input.generics.split_for_impl();
    let root = agents_root();
    let description = match description {
        Some(text) => quote!(::core::option::Option::Some(#text)),
        None => quote!(::core::option::Option::None),
    };

    quote! {
        impl #impl_g #root::AgentTask for #ty #ty_g #where_g {
            const NAME: &'static str = #name;
            fn description() -> ::core::option::Option<&'static str> {
                #description
            }
        }
        #root::inventory::submit! {
            #root::AgentTaskInfo::of::<#ty #ty_g>()
        }
    }
    .into()
}
