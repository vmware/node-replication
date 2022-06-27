use darling::{FromDeriveInput, FromMeta};
use proc_macro::{self, TokenStream};
use quote::quote;
use syn::{parse_macro_input, token::Token, token::Type, DeriveInput, Ident};

#[derive(Default, FromMeta)]
#[darling(default)]
struct DispatchDeclaration {
    method: Option<syn::Ident>,
}

#[derive(FromDeriveInput)]
#[darling(attributes(nr_cfg), forward_attrs(allow, doc, cfg))]
struct MyTraitOpts {
    ident: Ident,
    attrs: Vec<syn::Attribute>,
    dispatch: DispatchDeclaration,
}

#[proc_macro_derive(NodeReplicated, attributes(nr_cfg))]
pub fn derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input);
    let opts = MyTraitOpts::from_derive_input(&input).expect("Wrong options");
    let DeriveInput { ident, .. } = input;

    //println!("{:?}", opts.dispatch.method);

    /*let dispatch = opts.lorem.clone();
    let method = dispatch.method;
    let emit = quote! {
        fn #method() -> String {
            String::from("blabla")
        }
    };*/

    let output = quote! {
        //impl node_replication::Dispatch for #ident {

        //s}
    };

    output.into()
}

#[proc_macro_attribute]
pub fn methods(args: TokenStream, original: TokenStream) -> TokenStream {
    let original2 = original.clone();
    let original2 = syn::parse_macro_input!(original2 as syn::ItemImpl);
    println!("original {:#?}", original2);
    let args = syn::parse_macro_input!(args as syn::AttributeArgs);
    println!("args are {:?}", args);

    let args = match Args::from_list(&args) {
        Ok(v) => v,
        Err(e) => {
            return e.write_errors().into();
        }
    };

    original
}

#[derive(Default, FromMeta)]
#[darling(default)]
struct Args {
    path: Option<syn::Path>,
    self_type: SelfType,
}

#[derive(FromMeta, PartialEq, Eq, Copy, Clone)]
#[darling(rename_all = "PascalCase")]
enum SelfType {
    Rc,
    #[darling(rename = "owned")]
    Owned,
    Arc,
    Box,
}

impl Default for SelfType {
    fn default() -> Self {
        SelfType::Owned
    }
}
