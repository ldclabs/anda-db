//! # Declaring a closed wire vocabulary once
//!
//! KIP fixes a handful of small closed vocabularies — `stance`, `mode`, the
//! Assertion lifecycle, the belief statuses, the Core element kinds, the error
//! taxonomy, the conformance profiles. Each of them was written out three
//! times in this crate: as an enum for Rust callers, as a `&[&str]` registry
//! for the parser's checks, and a third time inside the test that asserted the
//! first two still agreed.
//!
//! Three copies with a test holding them together is a rule that can only be
//! *discovered* broken. [`wire_enum!`] declares the vocabulary once and derives
//! all of it — the enum, its serde impls, `Display`, `FromStr`, and the `ALL`
//! / `NAMES` slices — so the copies cannot drift, because there is only one.

/// Declares a closed wire vocabulary: an enum and every spelling of it.
///
/// ```text
/// wire_enum! {
///     /// docs for the type
///     pub enum Stance {
///         /// docs for the variant
///         Support = "support",
///     }
/// }
/// ```
///
/// Generates `ALL` (the variants) and `NAMES` (their wire spellings, in the
/// same order), `as_str`, `from_wire`, `Display`, `FromStr`, and serde impls
/// that read and write the declared spelling and nothing else.
///
/// The derives are the ones every wire vocabulary wants: `Clone`, `Copy`,
/// `Debug`, `PartialEq`, `Eq`, `Hash`. Ordering is *not* among them — most of
/// these vocabularies have no rank, and one that does says so with a
/// `#[derive(PartialOrd, Ord)]` of its own. Extra attributes pass through, so
/// a `#[derive(Default)]` with a `#[default]` variant works too.
macro_rules! wire_enum {
    (
        $(#[$enum_meta:meta])*
        $vis:vis enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident = $wire:literal ),+ $(,)?
        }
    ) => {
        $(#[$enum_meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        $vis enum $name {
            $( $(#[$variant_meta])* $variant, )+
        }

        impl $name {
            /// Every variant, in specification order.
            pub const ALL: &'static [$name] = &[ $( $name::$variant, )+ ];

            /// Every wire spelling, in the same order as [`Self::ALL`].
            pub const NAMES: &'static [&'static str] = &[ $( $wire, )+ ];

            /// The wire spelling of this value.
            pub const fn as_str(&self) -> &'static str {
                match self {
                    $( $name::$variant => $wire, )+
                }
            }

            /// The value a wire spelling names, if it names one.
            ///
            /// Allocation-free, which is what lets `FromStr` build its
            /// message only on the failing path.
            pub fn from_wire(wire: &str) -> Option<Self> {
                match wire {
                    $( $wire => Some($name::$variant), )+
                    _ => None,
                }
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = String;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                $name::from_wire(s).ok_or_else(|| {
                    format!(
                        "{s:?} is not one of the {} values {}",
                        stringify!($name),
                        $name::NAMES.join(" | "),
                    )
                })
            }
        }

        // Serde by hand rather than by derive, because a derived fieldless
        // enum also accepts serde's externally-tagged map form — `{"support":
        // null}` — and a KIP vocabulary is a string on the wire and nothing
        // else. Deserializing through `from_wire` keeps the accepted set, the
        // `NAMES` slice and `as_str` the same list by construction.
        impl ::serde::Serialize for $name {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D: ::serde::Deserializer<'de>>(
                deserializer: D,
            ) -> Result<Self, D::Error> {
                struct Visitor;

                impl ::serde::de::Visitor<'_> for Visitor {
                    type Value = $name;

                    fn expecting(
                        &self,
                        f: &mut ::std::fmt::Formatter<'_>,
                    ) -> ::std::fmt::Result {
                        write!(f, "one of {}", $name::NAMES.join(" | "))
                    }

                    fn visit_str<E: ::serde::de::Error>(self, s: &str) -> Result<$name, E> {
                        $name::from_wire(s)
                            .ok_or_else(|| E::unknown_variant(s, $name::NAMES))
                    }
                }

                deserializer.deserialize_str(Visitor)
            }
        }
    };
}
