//! Server traits for `alloy-rpc-server`.
//!
//! `#[rpc(server, namespace = "..")]` turns an RPC trait into a `<Trait>Server` trait that
//! implementations write as usual, plus an `into_rpc` method that registers each method on an
//! `RpcModule`, decoding its positional params into the method's arguments.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use std::collections::HashSet;
use syn::{
    Attribute, FnArg, Ident, ItemTrait, LitStr, ReturnType, Signature, TraitItem, TraitItemFn,
    Type, parse_macro_input, parse_quote, punctuated::Punctuated, spanned::Spanned, visit::Visit,
};

/// Generates `<Trait>Server` and its `into_rpc` from an RPC trait.
///
/// # Examples
///
/// ```
/// use alloy_rpc_macros::rpc;
/// use alloy_rpc_server::RpcResult;
///
/// #[rpc(server, namespace = "eth")]
/// pub trait EthApi {
///     #[method(name = "chainId")]
///     async fn chain_id(&self) -> RpcResult<u64>;
///
///     #[method(name = "getBalance")]
///     fn balance(&self, address: String, block: Option<String>) -> RpcResult<u64>;
/// }
///
/// struct Node;
///
/// impl EthApiServer for Node {
///     async fn chain_id(&self) -> RpcResult<u64> {
///         Ok(1)
///     }
///
///     fn balance(&self, _address: String, block: Option<String>) -> RpcResult<u64> {
///         Ok(if block.is_some() { 0 } else { 100 })
///     }
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let module = Node.into_rpc();
/// let request = r#"{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xab"]}"#;
/// assert_eq!(module.raw_json_request(request).await, r#"{"jsonrpc":"2.0","id":1,"result":100}"#);
/// # }
/// ```
#[proc_macro_attribute]
pub fn rpc(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as ItemTrait);
    match RpcTrait::parse(attr.into(), item).and_then(|rpc| rpc.render()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

// A parsed `#[rpc]` trait.
struct RpcTrait {
    // The trait, without its methods.
    item: ItemTrait,
    // The prefix of every method name.
    namespace: String,
    // The methods, in declaration order.
    methods: Vec<RpcMethod>,
}

impl RpcTrait {
    // Reads the `#[rpc(server, namespace = "..")]` arguments and the trait's methods.
    fn parse(attr: TokenStream2, mut item: ItemTrait) -> syn::Result<Self> {
        let (mut server, mut namespace) = (false, None);
        let parser = syn::meta::parser(|meta| {
            if meta.path.is_ident("server") {
                server = true;
            } else if meta.path.is_ident("namespace") {
                namespace = Some(meta.value()?.parse::<LitStr>()?.value());
            } else {
                return Err(meta.error("expected `server` or `namespace = \"..\"`"));
            }
            Ok(())
        });
        syn::parse::Parser::parse2(parser, attr)?;
        if !server {
            return Err(syn::Error::new(
                Span::call_site(),
                "only `#[rpc(server, ..)]` is supported",
            ));
        }
        let namespace = namespace
            .ok_or_else(|| syn::Error::new(Span::call_site(), "missing `namespace = \"..\"`"))?;
        let methods = std::mem::take(&mut item.items)
            .into_iter()
            .map(|trait_item| match trait_item {
                TraitItem::Fn(method) => RpcMethod::parse(method),
                other => Err(syn::Error::new(other.span(), "only methods are supported")),
            })
            .collect::<syn::Result<_>>()?;
        Ok(Self { item, namespace, methods })
    }

    // The server trait.
    fn render(&self) -> syn::Result<TokenStream2> {
        let ItemTrait { attrs, vis, ident, generics, supertraits, .. } = &self.item;
        let server = format_ident!("{ident}Server");
        let (impl_generics, _, where_clause) = generics.split_for_impl();
        let supertraits = supertraits.iter();
        let methods = self.methods.iter().map(RpcMethod::render_signature);
        let into_rpc = self.render_into_rpc()?;
        let params = self.methods.iter().filter_map(|method| method.render_params(&self.item));
        Ok(quote! {
            #(#attrs)*
            #vis trait #server #impl_generics: #(#supertraits +)* Sized + Send + Sync + 'static
            #where_clause
            {
                #(#methods)*
                #into_rpc
            }

            #(#params)*
        })
    }

    // `into_rpc`, which registers every method and alias. Names are checked here, so registering
    // can't fail at runtime.
    fn render_into_rpc(&self) -> syn::Result<TokenStream2> {
        let mut names = HashSet::new();
        let mut registrations = Vec::new();
        for method in &self.methods {
            let rpc_name = format!("{}_{}", self.namespace, method.name);
            for name in std::iter::once(&rpc_name).chain(&method.aliases) {
                if !names.insert(name.clone()) {
                    let message = format!("`{name}` is registered twice");
                    return Err(syn::Error::new(method.sig.ident.span(), message));
                }
            }
            registrations.push(method.render_registration(&self.item, &rpc_name));
            registrations.extend(method.aliases.iter().map(|alias| {
                quote! {
                    rpc.register_alias(#alias, #rpc_name)
                        .expect("aliases target a method registered just before");
                }
            }));
        }
        let bounds = self.generic_bounds();
        Ok(quote! {
            /// Collects all the methods of the trait into one `RpcModule`.
            fn into_rpc(self) -> ::alloy_rpc_server::RpcModule<Self> where #(#bounds,)* {
                let mut rpc = ::alloy_rpc_server::RpcModule::new(self);
                #(#registrations)*
                rpc
            }
        })
    }

    // `Send + Sync + 'static` for every type parameter, plus `DeserializeOwned` when it appears in
    // an argument and `Serialize` when it appears in a return type.
    fn generic_bounds(&self) -> Vec<TokenStream2> {
        let params = type_param_idents(&self.item);
        let mut used = UsedParams { params: &params, found: HashSet::new() };
        for ty in self.methods.iter().flat_map(|method| &method.arg_tys) {
            used.visit_type(ty);
        }
        let in_args = std::mem::take(&mut used.found);
        for method in &self.methods {
            used.visit_return_type(&method.sig.output);
        }
        let in_returns = used.found;
        self.item
            .generics
            .type_params()
            .map(|param| {
                let ident = &param.ident;
                let de = in_args
                    .contains(ident)
                    .then(|| quote!(+ ::alloy_rpc_server::__private::serde::de::DeserializeOwned));
                let ser = in_returns
                    .contains(ident)
                    .then(|| quote!(+ ::alloy_rpc_server::__private::serde::Serialize));
                quote!(#ident: Send + Sync + 'static #de #ser)
            })
            .collect()
    }
}

// A parsed `#[method]`.
struct RpcMethod {
    // The name without the namespace.
    name: String,
    // Extra names, registered as written.
    aliases: Vec<String>,
    // Whether the method runs on the blocking pool.
    blocking: bool,
    // The signature as declared.
    sig: Signature,
    // The attributes other than `#[method]`, such as docs.
    attrs: Vec<Attribute>,
    // The argument types after `&self`, in order.
    arg_tys: Vec<Type>,
}

impl RpcMethod {
    // Reads the `#[method]` attribute and checks the signature.
    fn parse(mut method: TraitItemFn) -> syn::Result<Self> {
        if let Some(attr) = method.attrs.iter().find(|attr| attr.path().is_ident("subscription")) {
            return Err(syn::Error::new(attr.meta.span(), "`#[subscription]` isn't supported yet"));
        }
        if let Some(default) = &method.default {
            return Err(syn::Error::new(default.span(), "RPC methods can't have a default body"));
        }
        let position =
            method.attrs.iter().position(|attr| attr.path().is_ident("method")).ok_or_else(
                || syn::Error::new(method.sig.span(), "missing `#[method(name = \"..\")]`"),
            )?;
        let attr = method.attrs.remove(position);

        let (mut name, mut aliases, mut blocking) = (None, Vec::new(), false);
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                name = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("aliases") {
                let value = meta.value()?;
                let content;
                syn::bracketed!(content in value);
                let list = Punctuated::<LitStr, syn::Token![,]>::parse_terminated(&content)?;
                aliases = list.iter().map(LitStr::value).collect();
            } else if meta.path.is_ident("blocking") {
                blocking = true;
            } else {
                return Err(meta.error("expected `name`, `aliases` or `blocking`"));
            }
            Ok(())
        })?;
        let name =
            name.ok_or_else(|| syn::Error::new(attr.meta.span(), "missing `name = \"..\"`"))?;
        if blocking && method.sig.asyncness.is_some() {
            return Err(syn::Error::new(method.sig.span(), "`blocking` methods can't be `async`"));
        }

        let mut inputs = method.sig.inputs.iter();
        let takes_ref = matches!(
            inputs.next(),
            Some(FnArg::Receiver(receiver))
                if receiver.reference.is_some() && receiver.mutability.is_none()
        );
        if !takes_ref {
            return Err(syn::Error::new(method.sig.span(), "RPC methods take `&self`"));
        }
        let arg_tys = inputs
            .map(|input| match input {
                FnArg::Typed(arg) => Ok((*arg.ty).clone()),
                FnArg::Receiver(receiver) => {
                    Err(syn::Error::new(receiver.span(), "only the first argument is `&self`"))
                }
            })
            .collect::<syn::Result<_>>()?;
        Ok(Self { name, aliases, blocking, sig: method.sig, attrs: method.attrs, arg_tys })
    }

    // The method in the server trait, with `async fn` written as `fn .. -> impl Future + Send`
    // so the future can be spawned.
    fn render_signature(&self) -> TokenStream2 {
        let mut sig = self.sig.clone();
        if sig.asyncness.take().is_some() {
            let output = match &sig.output {
                ReturnType::Default => quote!(()),
                ReturnType::Type(_, ty) => quote!(#ty),
            };
            sig.output = parse_quote! {
                -> impl ::core::future::Future<Output = #output> + ::core::marker::Send
            };
        }
        let attrs = &self.attrs;
        quote! { #(#attrs)* #sig; }
    }

    // The struct the method's params decode into, as a tuple struct of its argument types. Each
    // trailing `Option` argument gets `#[serde(default)]`, so it may be left out. A method without
    // arguments takes `()` instead.
    fn render_params(&self, item: &ItemTrait) -> Option<TokenStream2> {
        if self.arg_tys.is_empty() {
            return None;
        }
        let ident = self.params_ident(item);
        let used = self.used_params(item);
        let params = item.generics.type_params().filter(|param| used.contains(&param.ident));
        // Predicates on other parameters can't be stated on this struct.
        let predicates =
            item.generics.where_clause.iter().flat_map(|clause| &clause.predicates).filter(
                |predicate| {
                    let mut mentioned =
                        UsedParams { params: &type_param_idents(item), found: HashSet::new() };
                    mentioned.visit_where_predicate(predicate);
                    !mentioned.found.is_empty() && mentioned.found.is_subset(&used)
                },
            );
        let optional = self.arg_tys.iter().rev().take_while(|ty| is_option(ty)).count();
        let required = self.arg_tys.len() - optional;
        let fields = self.arg_tys.iter().enumerate().map(|(i, ty)| {
            let default = (i >= required).then(|| quote!(#[serde(default)]));
            quote!(#default #ty)
        });
        Some(quote! {
            #[doc(hidden)]
            #[derive(::alloy_rpc_server::__private::serde::Deserialize)]
            #[serde(crate = "::alloy_rpc_server::__private::serde")]
            struct #ident<#(#params),*>(#(#fields),*) where #(#predicates,)*;
        })
    }

    // `__<Trait><Method>Params`, the name of the method's params struct.
    fn params_ident(&self, item: &ItemTrait) -> Ident {
        let method = self.sig.ident.to_string();
        let camel = method
            .split('_')
            .map(|word| {
                let mut chars = word.chars();
                chars
                    .next()
                    .map_or_else(String::new, |first| first.to_uppercase().chain(chars).collect())
            })
            .collect::<String>();
        format_ident!("__{}{camel}Params", item.ident)
    }

    // The trait's type parameters the method's arguments mention.
    fn used_params(&self, item: &ItemTrait) -> HashSet<Ident> {
        let params = type_param_idents(item);
        let mut used = UsedParams { params: &params, found: HashSet::new() };
        for ty in &self.arg_tys {
            used.visit_type(ty);
        }
        used.found
    }

    // The registration of the method in `into_rpc`.
    fn render_registration(&self, item: &ItemTrait, rpc_name: &str) -> TokenStream2 {
        let ident = &self.sig.ident;
        let args = (0..self.arg_tys.len()).map(|i| format_ident!("__param{i}")).collect::<Vec<_>>();
        let (pattern, ty) = if args.is_empty() {
            (quote!(()), quote!(()))
        } else {
            let params = self.params_ident(item);
            let used = self.used_params(item);
            let generics = item
                .generics
                .type_params()
                .map(|param| &param.ident)
                .filter(|param| used.contains(*param));
            (quote!(#params(#(#args),*)), quote!(#params<#(#generics),*>))
        };
        let register = if self.sig.asyncness.is_some() {
            quote! {
                rpc.register_async_method(#rpc_name, |#pattern: #ty, context| async move {
                    context.as_ref().#ident(#(#args),*).await
                })
            }
        } else if self.blocking {
            quote! {
                rpc.register_blocking_method(#rpc_name, |#pattern: #ty, context| {
                    context.#ident(#(#args),*)
                })
            }
        } else {
            quote! {
                rpc.register_method(#rpc_name, |#pattern: #ty, context| context.#ident(#(#args),*))
            }
        };
        quote! { #register.expect("method names are checked for duplicates at compile time"); }
    }
}

// The idents of the trait's type parameters.
fn type_param_idents(item: &ItemTrait) -> HashSet<Ident> {
    item.generics.type_params().map(|param| param.ident.clone()).collect()
}

// Whether `ty` is written as `Option<..>`, which marks an argument that may be left out.
fn is_option(ty: &Type) -> bool {
    matches!(ty, Type::Path(path) if path.qself.is_none()
        && path.path.segments.last().is_some_and(|segment| segment.ident == "Option"))
}

// Collects which of `params` a type mentions, including as `Param::Assoc`.
struct UsedParams<'a> {
    // The trait's type parameters.
    params: &'a HashSet<Ident>,
    // The ones found so far.
    found: HashSet<Ident>,
}

impl<'ast> Visit<'ast> for UsedParams<'_> {
    fn visit_path_segment(&mut self, segment: &'ast syn::PathSegment) {
        if self.params.contains(&segment.ident) {
            self.found.insert(segment.ident.clone());
        }
        syn::visit::visit_path_segment(self, segment);
    }
}
