/// Implement `new`, `Clone`, `Copy`, `Debug`, `Default`, and `Codec` for a
/// stateless codec declared as `struct Name<T>(PhantomData<fn() -> T>)`.
///
/// Derives would require `T` itself to implement each trait; the payload type
/// only appears in a `PhantomData`, so these impls place no bounds on it.
macro_rules! typed_codec {
    ($name:ident) => {
        impl<T> $name<T> {
            /// Create the codec.
            pub const fn new() -> Self {
                Self(::std::marker::PhantomData)
            }
        }

        impl<T> Clone for $name<T> {
            fn clone(&self) -> Self {
                *self
            }
        }

        impl<T> Copy for $name<T> {}

        impl<T> Default for $name<T> {
            fn default() -> Self {
                Self::new()
            }
        }

        impl<T> ::std::fmt::Debug for $name<T> {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                write!(f, "{}<{}>", stringify!($name), ::std::any::type_name::<T>())
            }
        }

        impl<T: 'static> $crate::codec::Codec for $name<T> {
            type Value = T;
        }
    };
}

pub(super) use typed_codec;
