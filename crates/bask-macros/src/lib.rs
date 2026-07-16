/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! `#[derive(Checkpoint)]`: marks a task type as a durable restore point, keyed by a
//! `#[key]` field, and registers it with the engine so no builder call is needed.

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use quote::{format_ident, quote};
use syn::{
    Data, DeriveInput, Fields, LitStr, Token, Type, parse_macro_input, punctuated::Punctuated,
};

/// Resolve the crate root that re-exports `Checkpoint`/`CheckpointInfo`/`inventory`,
/// preferring the `bask` umbrella and falling back to `bask-core`, so the derive works
/// whichever the user depends on.
fn bask_root() -> proc_macro2::TokenStream {
    for name in ["bask", "bask-core"] {
        if let Ok(found) = crate_name(name) {
            return match found {
                // Only bask-core's own tests/examples resolve as "itself"; the lib never
                // derives, so the extern-crate path is what those separate crates need.
                FoundCrate::Itself => quote!(::bask_core),
                FoundCrate::Name(name) => {
                    let ident = format_ident!("{}", name);
                    quote!(::#ident)
                }
            };
        }
    }
    quote!(::bask_core)
}

/// Derive `bask::Checkpoint` for a struct with a `#[key]`-annotated field. Container
/// attributes: `#[checkpoint(name = "...")]` overrides the store identity (default the
/// type name); `#[checkpoint(key_only)]` stores the key without a payload.
#[proc_macro_derive(Checkpoint, attributes(key, checkpoint))]
pub fn derive_checkpoint(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ty = &input.ident;

    let mut name = ty.to_string();
    let mut key_only = false;
    for attr in &input.attrs {
        if !attr.path().is_ident("checkpoint") {
            continue;
        }
        let parsed = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("key_only") {
                key_only = true;
            } else if meta.path.is_ident("name") {
                name = meta.value()?.parse::<LitStr>()?.value();
            } else {
                return Err(meta.error("unknown checkpoint attribute"));
            }
            Ok(())
        });
        if let Err(err) = parsed {
            return err.into_compile_error().into();
        }
    }

    let key_field = match key_field(&input.data) {
        Ok(field) => field,
        Err(err) => return err.into_compile_error().into(),
    };
    let (impl_g, ty_g, where_g) = input.generics.split_for_impl();
    let root = bask_root();

    quote! {
        impl #impl_g #root::Checkpoint for #ty #ty_g #where_g {
            const NAME: &'static str = #name;
            const KEY_ONLY: bool = #key_only;
            fn key(&self) -> ::std::string::String {
                ::std::string::ToString::to_string(&self.#key_field)
            }
        }
        #root::inventory::submit! {
            #root::CheckpointInfo::of::<#ty #ty_g>()
        }
    }
    .into()
}

fn key_field(data: &Data) -> syn::Result<proc_macro2::Ident> {
    let fields = match data {
        Data::Struct(s) => &s.fields,
        _ => {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "Checkpoint can only be derived for structs",
            ));
        }
    };
    let named = match fields {
        Fields::Named(named) => &named.named,
        _ => {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "Checkpoint requires named fields with one `#[key]`",
            ));
        }
    };
    let mut keyed = named
        .iter()
        .filter(|f| f.attrs.iter().any(|a| a.path().is_ident("key")));
    match (keyed.next(), keyed.next()) {
        (Some(field), None) => Ok(field.ident.clone().unwrap()),
        (None, _) => Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "Checkpoint requires exactly one field marked `#[key]`",
        )),
        (Some(_), Some(dup)) => Err(syn::Error::new_spanned(
            dup,
            "Checkpoint allows only one `#[key]` field",
        )),
    }
}

/// Derive `bask::EmitPolicy` from a `#[emits(TypeA, TypeB, ...)]` list. Generates the
/// trait impl and registers the type with the engine so no builder call is needed.
#[proc_macro_derive(EmitPolicy, attributes(emits))]
pub fn derive_emit_policy(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ty = &input.ident;

    let mut targets: Vec<Type> = Vec::new();
    for attr in &input.attrs {
        if !attr.path().is_ident("emits") {
            continue;
        }
        match attr.parse_args_with(Punctuated::<Type, Token![,]>::parse_terminated) {
            Ok(list) => targets.extend(list),
            Err(err) => return err.into_compile_error().into(),
        }
    }

    let (impl_g, ty_g, where_g) = input.generics.split_for_impl();
    let root = bask_root();

    quote! {
        impl #impl_g #root::EmitPolicy for #ty #ty_g #where_g {
            fn declare(allow: &mut #root::Allow) {
                #( allow.allow::<#targets>(); )*
            }
        }
        #root::inventory::submit! {
            #root::EmitPolicyInfo::of::<#ty #ty_g>()
        }
    }
    .into()
}
