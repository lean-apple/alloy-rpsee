#![doc = include_str!("../README.md")]

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use std::collections::HashSet;
use syn::{
    Attribute, FnArg, Ident, ItemTrait, LitStr, ReturnType, Signature, TraitItem, TraitItemFn,
    Type, meta::ParseNestedMeta, parse_macro_input, parse_quote, punctuated::Punctuated,
    spanned::Spanned, visit::Visit,
};

/// Generates `<Trait>Server` and its `into_rpc` from an RPC trait. See the [crate docs](crate).
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

    // `into_rpc`, which registers every method, alias and subscription. Names are checked here, so
    // registering can't fail at runtime.
    fn render_into_rpc(&self) -> syn::Result<TokenStream2> {
        let mut names = HashSet::new();
        let mut registrations = Vec::new();
        for method in &self.methods {
            let rpc_name = self.rpc_name(&method.name);
            let taken = match &method.kind {
                ItemKind::Method { aliases, .. } => {
                    std::iter::once(rpc_name.clone()).chain(aliases.iter().cloned()).collect()
                }
                ItemKind::Subscription { unsubscribe, .. } => {
                    vec![rpc_name.clone(), self.rpc_name(unsubscribe)]
                }
            };
            for name in taken {
                if !names.insert(name.clone()) {
                    let message = format!("`{name}` is registered twice");
                    return Err(syn::Error::new(method.sig.ident.span(), message));
                }
            }
            registrations.push(method.render_registration(self, &rpc_name));
        }
        let bounds = self.type_param_bounds();
        Ok(quote! {
            /// Collects all the methods of the trait into one `RpcModule`.
            fn into_rpc(self) -> ::alloy_rpc_server::RpcModule<Self> where #(#bounds,)* {
                let mut rpc = ::alloy_rpc_server::RpcModule::new(self);
                #(#registrations)*
                rpc
            }
        })
    }

    // `<namespace>_<name>`, the name a method is registered as.
    fn rpc_name(&self, name: &str) -> String {
        format!("{}_{name}", self.namespace)
    }

    // `Send + Sync + 'static` for every type parameter, plus `DeserializeOwned` when it appears in
    // an argument and `Serialize` when it appears in a return type or a subscription item.
    fn type_param_bounds(&self) -> Vec<TokenStream2> {
        let params = type_param_idents(&self.item);
        let mut used = UsedParams { params: &params, found: HashSet::new() };
        for ty in self.methods.iter().flat_map(|method| &method.arg_types) {
            used.visit_type(ty);
        }
        let deserialized = std::mem::take(&mut used.found);
        for method in &self.methods {
            match &method.kind {
                ItemKind::Method { .. } => used.visit_return_type(&method.sig.output),
                ItemKind::Subscription { item, .. } => used.visit_type(item),
            }
        }
        let serialized = used.found;
        self.item
            .generics
            .type_params()
            .map(|param| {
                let ident = &param.ident;
                let de = deserialized
                    .contains(ident)
                    .then(|| quote!(+ ::alloy_rpc_server::__private::serde::de::DeserializeOwned));
                let ser = serialized
                    .contains(ident)
                    .then(|| quote!(+ ::alloy_rpc_server::__private::serde::Serialize));
                quote!(#ident: Send + Sync + 'static #de #ser)
            })
            .collect()
    }
}

// A parsed `#[method]` or `#[subscription]`.
struct RpcMethod {
    // The name without the namespace.
    name: String,
    // What the item registers as.
    kind: ItemKind,
    // The signature as declared.
    sig: Signature,
    // The attributes other than `#[method]` or `#[subscription]`, such as docs.
    attrs: Vec<Attribute>,
    // The argument types after `&self`, in order.
    arg_types: Vec<Type>,
}

// What an RPC item registers as.
enum ItemKind {
    // A method answering calls.
    Method {
        // Extra names, registered as written.
        aliases: Vec<String>,
        // Whether the method runs on the blocking pool.
        blocking: bool,
    },
    // A subscription sending notifications.
    Subscription {
        // The notification method name without the namespace, the subscription's own if `None`.
        notification: Option<String>,
        // The unsubscribe method name without the namespace.
        unsubscribe: String,
        // The type each notification carries.
        item: Box<Type>,
    },
}

impl RpcMethod {
    // Reads the `#[method]` or `#[subscription]` attribute and checks the signature.
    fn parse(mut method: TraitItemFn) -> syn::Result<Self> {
        if let Some(default) = &method.default {
            return Err(syn::Error::new(default.span(), "RPC methods can't have a default body"));
        }
        let position = method
            .attrs
            .iter()
            .position(|attr| attr.path().is_ident("method") || attr.path().is_ident("subscription"))
            .ok_or_else(|| {
                syn::Error::new(method.sig.span(), "missing `#[method(name = \"..\")]`")
            })?;
        let attr = method.attrs.remove(position);
        let (name, kind) = if attr.path().is_ident("method") {
            let MethodArgs { name, aliases, blocking } = MethodArgs::parse(&attr)?;
            if blocking && method.sig.asyncness.is_some() {
                let message = "`blocking` methods can't be `async`";
                return Err(syn::Error::new(method.sig.span(), message));
            }
            (name, ItemKind::Method { aliases, blocking })
        } else {
            let SubscriptionArgs { name, notification, unsubscribe, item } =
                SubscriptionArgs::parse(&attr)?;
            if method.sig.asyncness.is_none() {
                return Err(syn::Error::new(method.sig.span(), "subscriptions must be `async`"));
            }
            (name, ItemKind::Subscription { notification, unsubscribe, item })
        };

        let mut inputs = method.sig.inputs.iter();
        let takes_ref = matches!(
            inputs.next(),
            Some(FnArg::Receiver(receiver))
                if receiver.reference.is_some() && receiver.mutability.is_none()
        );
        if !takes_ref {
            return Err(syn::Error::new(method.sig.span(), "RPC methods take `&self`"));
        }
        let arg_types = inputs
            .map(|input| match input {
                FnArg::Typed(arg) => Ok((*arg.ty).clone()),
                FnArg::Receiver(receiver) => {
                    Err(syn::Error::new(receiver.span(), "only the first argument is `&self`"))
                }
            })
            .collect::<syn::Result<_>>()?;
        Ok(Self { name, kind, sig: method.sig, attrs: method.attrs, arg_types })
    }

    // The method in the server trait, with `async fn` written as `fn .. -> impl Future + Send`
    // so the future can be spawned. A subscription also takes the call's pending sink.
    fn render_signature(&self) -> TokenStream2 {
        let mut sig = self.sig.clone();
        if let ItemKind::Subscription { .. } = self.kind {
            sig.inputs
                .insert(1, parse_quote!(pending: ::alloy_rpc_server::PendingSubscriptionSink));
        }
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
        if self.arg_types.is_empty() {
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
        let optional = self.arg_types.iter().rev().take_while(|ty| is_option(ty)).count();
        let required = self.arg_types.len() - optional;
        let fields = self.arg_types.iter().enumerate().map(|(i, ty)| {
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
        for ty in &self.arg_types {
            used.visit_type(ty);
        }
        used.found
    }

    // The registration of the method in `into_rpc`.
    fn render_registration(&self, rpc_trait: &RpcTrait, rpc_name: &str) -> TokenStream2 {
        let item = &rpc_trait.item;
        let ident = &self.sig.ident;
        let args =
            (0..self.arg_types.len()).map(|i| format_ident!("__param{i}")).collect::<Vec<_>>();
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
        let (aliases, blocking) = match &self.kind {
            ItemKind::Method { aliases, blocking } => (aliases, *blocking),
            ItemKind::Subscription { notification, unsubscribe, .. } => {
                let notification =
                    rpc_trait.rpc_name(notification.as_deref().unwrap_or(&self.name));
                let unsubscribe = rpc_trait.rpc_name(unsubscribe);
                return quote! {
                    rpc.register_subscription(
                        #rpc_name,
                        #notification,
                        #unsubscribe,
                        |#pattern: #ty, pending, context| async move {
                            context.as_ref().#ident(pending, #(#args),*).await
                        },
                    )
                    .expect("method names are checked for duplicates at compile time");
                };
            }
        };
        let register = if self.sig.asyncness.is_some() {
            quote! {
                rpc.register_async_method(#rpc_name, |#pattern: #ty, context| async move {
                    context.as_ref().#ident(#(#args),*).await
                })
            }
        } else if blocking {
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
        let aliases = aliases.iter().map(|alias| {
            quote! {
                rpc.register_alias(#alias, #rpc_name)
                    .expect("aliases target a method registered just before");
            }
        });
        quote! {
            #register.expect("method names are checked for duplicates at compile time");
            #(#aliases)*
        }
    }
}

// The arguments of `#[method(name = "..", aliases = [".."], blocking)]`.
struct MethodArgs {
    // The name without the namespace.
    name: String,
    // Extra names, registered as written.
    aliases: Vec<String>,
    // Whether the method runs on the blocking pool.
    blocking: bool,
}

impl MethodArgs {
    // Reads the attribute's arguments.
    fn parse(attr: &Attribute) -> syn::Result<Self> {
        let (mut name, mut aliases, mut blocking) = (None, Vec::new(), false);
        attr.parse_nested_meta(|meta| {
            match meta.path.get_ident().map(Ident::to_string).as_deref() {
                Some("name") => name = Some(parse_string(&meta)?),
                Some("aliases") => aliases = parse_strings(&meta)?,
                Some("blocking") => blocking = true,
                _ => return Err(meta.error("expected `name`, `aliases` or `blocking`")),
            }
            Ok(())
        })?;
        Ok(Self { name: require(attr, name, "name = \"..\"")?, aliases, blocking })
    }
}

// The arguments of `#[subscription(name = ".." => "..", unsubscribe = "..", item = T)]`, where
// `=> ".."` names the notifications.
struct SubscriptionArgs {
    // The name without the namespace.
    name: String,
    // The notification method name without the namespace, if it differs.
    notification: Option<String>,
    // The unsubscribe method name without the namespace.
    unsubscribe: String,
    // The type each notification carries.
    item: Box<Type>,
}

impl SubscriptionArgs {
    // Reads the attribute's arguments.
    fn parse(attr: &Attribute) -> syn::Result<Self> {
        let (mut name, mut notification, mut unsubscribe, mut item) = (None, None, None, None);
        attr.parse_nested_meta(|meta| {
            match meta.path.get_ident().map(Ident::to_string).as_deref() {
                Some("name") => {
                    name = Some(parse_string(&meta)?);
                    if meta.input.parse::<Option<syn::Token![=>]>>()?.is_some() {
                        notification = Some(meta.input.parse::<LitStr>()?.value());
                    }
                }
                Some("unsubscribe") => unsubscribe = Some(parse_string(&meta)?),
                Some("item") => item = Some(Box::new(meta.value()?.parse::<Type>()?)),
                _ => return Err(meta.error("expected `name`, `unsubscribe` or `item`")),
            }
            Ok(())
        })?;
        Ok(Self {
            name: require(attr, name, "name = \"..\"")?,
            notification,
            unsubscribe: require(attr, unsubscribe, "unsubscribe = \"..\"")?,
            item: require(attr, item, "item = ..")?,
        })
    }
}

// The string of `key = ".."`.
fn parse_string(meta: &ParseNestedMeta<'_>) -> syn::Result<String> {
    Ok(meta.value()?.parse::<LitStr>()?.value())
}

// The strings of `key = ["..", ".."]`.
fn parse_strings(meta: &ParseNestedMeta<'_>) -> syn::Result<Vec<String>> {
    let value = meta.value()?;
    let content;
    syn::bracketed!(content in value);
    let list = Punctuated::<LitStr, syn::Token![,]>::parse_terminated(&content)?;
    Ok(list.iter().map(LitStr::value).collect())
}

// The value of a required `key`, or an error pointing at the attribute.
fn require<T>(attr: &Attribute, value: Option<T>, key: &str) -> syn::Result<T> {
    value.ok_or_else(|| syn::Error::new(attr.meta.span(), format!("missing `{key}`")))
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
