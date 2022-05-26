extern crate proc_macro;

use proc_macro::TokenStream;
use syn::parse_macro_input;
use syn::DeriveInput;

use self::models::PaginatedStruct;

#[proc_macro_derive(PaginatedQuery, attributes(limit, offset))]
pub fn paginated_query_macro_derive(input: TokenStream) -> TokenStream {
    let syntax_tree = parse_macro_input!(input as DeriveInput);

    let model = PaginatedStruct::try_from(&syntax_tree).unwrap();

    model.gen().into()
}

mod models {
    use proc_macro2::Span;
    use proc_macro2::TokenStream;
    use quote::quote;
    use syn::*;

    #[derive(Clone, Debug)]
    pub(super) struct PaginatedStruct {
        name: Ident,
        limit: PaginatedStructField,
        offset: PaginatedStructField,
    }

    impl PaginatedStruct {
        pub(super) fn gen(&self) -> TokenStream {
            let name = &self.name;
            let limit_fn = &self.limit.gen("limit");
            let offset_fn = &self.offset.gen("offset");

            quote! {
                impl PaginatedQuery for #name {
                    #limit_fn

                    #offset_fn
                }
            }
            .into()
        }
    }

    #[derive(Clone, Debug)]
    struct PaginatedStructField {
        ident_opt: Option<Ident>,
        default_value: LitInt,
    }

    impl PaginatedStructField {
        fn gen(&self, fn_name: &'static str) -> TokenStream {
            let default_value_lit = &self.default_value;

            let impl_quote = match self.ident_opt.as_ref() {
                Some(ident) => quote! { self.#ident.unwrap_or(#default_value_lit) },
                None => quote! { #default_value_lit },
            };

            let fn_name = Ident::new(fn_name, Span::call_site());

            quote! {
                fn #fn_name(&self) -> i32 {
                    #impl_quote
                }
            }
        }

        fn limit_field<T>(
            fields: &punctuated::Punctuated<syn::Field, T>,
        ) -> core::result::Result<Self, &'static str> {
            let matched_fields = fields
                .iter()
                .filter(|f| matches!(Attr::try_from(*f), Ok(Attr::Limit(_))))
                .filter_map(|f| PaginatedStructField::try_from(f).ok())
                .collect::<Vec<_>>();

            if matched_fields.len() > 1 {
                return Err("too many attributes");
            }

            Ok(matched_fields
                .first()
                .ok_or_else(|| "field not found")?
                .clone())
        }

        fn offset_field<T>(
            fields: &punctuated::Punctuated<syn::Field, T>,
        ) -> core::result::Result<Self, &'static str> {
            let matched_fields = fields
                .iter()
                .filter(|f| matches!(Attr::try_from(*f), Ok(Attr::Offset(_))))
                .filter_map(|f| PaginatedStructField::try_from(f).ok())
                .collect::<Vec<_>>();

            if matched_fields.len() > 1 {
                return Err("too many attributes");
            }

            Ok(matched_fields
                .first()
                .ok_or_else(|| "field not found")?
                .clone())
        }
    }

    #[derive(Clone, Debug)]
    enum Attr {
        Limit(LitInt),
        Offset(LitInt),
    }

    impl Attr {
        fn default_value(&self) -> &LitInt {
            match self {
                Attr::Limit(default) => default,
                Attr::Offset(default) => default,
            }
        }
    }

    pub(super) mod extractors {
        use super::*;

        impl TryFrom<&DeriveInput> for PaginatedStruct {
            type Error = &'static str;

            fn try_from(input: &DeriveInput) -> core::result::Result<Self, Self::Error> {
                match input.data {
                    syn::Data::Struct(syn::DataStruct {
                        fields: syn::Fields::Named(FieldsNamed { ref named, .. }),
                        ..
                    }) => Ok(PaginatedStruct {
                        name: input.ident.clone(),
                        limit: PaginatedStructField::limit_field(&named)?,
                        offset: PaginatedStructField::offset_field(&named)?,
                    }),
                    _ => Err("help!"),
                }
            }
        }

        impl TryFrom<&Field> for PaginatedStructField {
            type Error = &'static str;

            fn try_from(field: &Field) -> core::result::Result<Self, Self::Error> {
                let ident_opt = field.ident.clone();
                let default_value = Attr::try_from(field.attrs.as_slice())?
                    .default_value()
                    .clone();

                match is_option_i32(&field.ty) {
                    true => Ok(PaginatedStructField {
                        ident_opt,
                        default_value,
                    }),
                    false => Err("not option i32"),
                }
            }
        }

        impl TryFrom<&Field> for Attr {
            type Error = &'static str;

            fn try_from(field: &Field) -> core::result::Result<Self, Self::Error> {
                field.attrs.as_slice().try_into()
            }
        }

        impl TryFrom<&[Attribute]> for Attr {
            type Error = &'static str;

            fn try_from(attrs: &[Attribute]) -> std::result::Result<Self, Self::Error> {
                if attrs.len() != 1 {
                    return Err("unexpected attributes");
                }

                (&attrs[0]).try_into()
            }
        }

        impl TryFrom<&Attribute> for Attr {
            type Error = &'static str;

            fn try_from(attr: &Attribute) -> core::result::Result<Self, Self::Error> {
                let lit = match attr.parse_meta() {
                    Ok(Meta::List(MetaList { nested, .. })) if nested.len() == 1 => {
                        match &nested[0] {
                            NestedMeta::Meta(Meta::NameValue(MetaNameValue {
                                lit: Lit::Int(lit),
                                ..
                            })) => lit.clone(),
                            _ => return Err("unexpected attributes"),
                        }
                    }
                    _ => return Err("unexpected attributes"),
                };

                match attr.path.get_ident() {
                    Some(ident) if ident == "limit" => Ok(Attr::Limit(lit)),
                    Some(ident) if ident == "offset" => Ok(Attr::Offset(lit)),
                    _ => Err("unexpected attributes"),
                }
            }
        }

        fn is_option_i32(ty: &Type) -> bool {
            match ty {
                Type::Path(TypePath {
                    path: Path { segments, .. },
                    ..
                }) if segments.len() == 1 => match &segments[0] {
                    PathSegment {
                        ident,
                        arguments:
                            PathArguments::AngleBracketed(AngleBracketedGenericArguments {
                                args: generic_args,
                                ..
                            }),
                    } if &ident.to_string() == "Option" && generic_args.len() == 1 => {
                        match &generic_args[0] {
                            GenericArgument::Type(Type::Path(TypePath { path, .. }))
                                if path.is_ident("i32") =>
                            {
                                true
                            }
                            _ => false,
                        }
                    }
                    _ => false,
                },
                _ => false,
            }
        }
    }
}
