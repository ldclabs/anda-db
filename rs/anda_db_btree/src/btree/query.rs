use super::*;

// Boolean predicates over one ordered field are exactly representable as
// disjoint ranges. Compile the *query* only, never the index's matching keys.
// Bounds borrow the input, so even a broad first-page query clones no FV.
type Span<'a, FV> = (Bound<&'a FV>, Bound<&'a FV>);
use std::cmp::Ordering as Cmp;

fn lower_cmp<FV: Ord>(a: Bound<&FV>, b: Bound<&FV>) -> Cmp {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Cmp::Equal,
        (Bound::Unbounded, _) => Cmp::Less,
        (_, Bound::Unbounded) => Cmp::Greater,
        (Bound::Included(a), Bound::Excluded(b)) => a.cmp(b).then(Cmp::Less),
        (Bound::Excluded(a), Bound::Included(b)) => a.cmp(b).then(Cmp::Greater),
        (Bound::Included(a), Bound::Included(b)) | (Bound::Excluded(a), Bound::Excluded(b)) => {
            a.cmp(b)
        }
    }
}
fn upper_cmp<FV: Ord>(a: Bound<&FV>, b: Bound<&FV>) -> Cmp {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Cmp::Equal,
        (Bound::Unbounded, _) => Cmp::Greater,
        (_, Bound::Unbounded) => Cmp::Less,
        (Bound::Included(a), Bound::Excluded(b)) => a.cmp(b).then(Cmp::Greater),
        (Bound::Excluded(a), Bound::Included(b)) => a.cmp(b).then(Cmp::Less),
        (Bound::Included(a), Bound::Included(b)) | (Bound::Excluded(a), Bound::Excluded(b)) => {
            a.cmp(b)
        }
    }
}
fn nonempty<FV: Ord>((lo, hi): Span<'_, FV>) -> bool {
    match (lo, hi) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
        (Bound::Included(a), Bound::Included(b)) => a <= b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => a < b,
    }
}
fn flip<FV>(bound: Bound<&FV>) -> Bound<&FV> {
    match bound {
        Bound::Included(v) => Bound::Excluded(v),
        Bound::Excluded(v) => Bound::Included(v),
        Bound::Unbounded => Bound::Unbounded,
    }
}
fn union<FV: Ord>(mut spans: Vec<Span<'_, FV>>) -> Vec<Span<'_, FV>> {
    spans.sort_unstable_by(|a, b| lower_cmp(a.0, b.0));
    let mut result: Vec<Span<'_, FV>> = Vec::with_capacity(spans.len());
    for span in spans {
        if let Some(last) = result.last_mut() {
            let touches = match (last.1, span.0) {
                (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
                (Bound::Excluded(a), Bound::Excluded(b)) => b < a,
                (
                    Bound::Included(a) | Bound::Excluded(a),
                    Bound::Included(b) | Bound::Excluded(b),
                ) => b <= a,
            };
            if touches {
                if upper_cmp(span.1, last.1).is_gt() {
                    last.1 = span.1;
                }
                continue;
            }
        }
        result.push(span);
    }
    result
}
fn intersection<'a, FV: Ord>(
    left: Vec<Span<'a, FV>>,
    right: Vec<Span<'a, FV>>,
) -> Vec<Span<'a, FV>> {
    let (mut i, mut j) = (0, 0);
    let mut result = Vec::new();
    while i < left.len() && j < right.len() {
        let a = left[i];
        let b = right[j];
        let lo = if lower_cmp(a.0, b.0).is_lt() {
            b.0
        } else {
            a.0
        };
        let hi = if upper_cmp(a.1, b.1).is_gt() {
            b.1
        } else {
            a.1
        };
        if nonempty((lo, hi)) {
            result.push((lo, hi));
        }
        match upper_cmp(a.1, b.1) {
            Cmp::Less => i += 1,
            Cmp::Greater => j += 1,
            Cmp::Equal => {
                i += 1;
                j += 1;
            }
        }
    }
    result
}
fn complement<FV: Ord>(spans: Vec<Span<'_, FV>>) -> Vec<Span<'_, FV>> {
    let mut result = Vec::with_capacity(spans.len() + 1);
    let mut start = Bound::Unbounded;
    for (lo, hi) in spans {
        if !matches!(lo, Bound::Unbounded) && nonempty((start, flip(lo))) {
            result.push((start, flip(lo)));
        }
        if matches!(hi, Bound::Unbounded) {
            return result;
        }
        start = flip(hi);
    }
    result.push((start, Bound::Unbounded));
    result
}
pub(super) fn compile<FV: Ord>(query: &RangeQuery<FV>) -> Vec<Span<'_, FV>> {
    use Bound::{Excluded as Ex, Included as In, Unbounded as All};
    match query {
        RangeQuery::Eq(v) => vec![(In(v), In(v))],
        RangeQuery::Gt(v) => vec![(Ex(v), All)],
        RangeQuery::Ge(v) => vec![(In(v), All)],
        RangeQuery::Lt(v) => vec![(All, Ex(v))],
        RangeQuery::Le(v) => vec![(All, In(v))],
        RangeQuery::Between(a, b) => {
            if a <= b {
                vec![(In(a), In(b))]
            } else {
                vec![]
            }
        }
        RangeQuery::Include(values) => union(values.iter().map(|v| (In(v), In(v))).collect()),
        RangeQuery::Or(children) => union(children.iter().flat_map(|q| compile(q)).collect()),
        RangeQuery::And(children) => {
            let mut children = children.iter();
            let Some(first) = children.next() else {
                return vec![];
            };
            let mut result = compile(first);
            for child in children {
                if result.is_empty() {
                    break;
                }
                result = intersection(result, compile(child));
            }
            result
        }
        RangeQuery::Not(child) => complement(compile(child)),
    }
}

// Enforce resource bounds *while* deserializing, before a deep tree exists.
// The external enum representation is identical to serde's derived format.
mod decode {
    use super::RangeQuery;
    use serde::{
        Deserialize, Deserializer,
        de::{self, DeserializeSeed, EnumAccess, SeqAccess, VariantAccess, Visitor},
    };
    use std::{fmt, marker::PhantomData};

    #[derive(Default)]
    struct Budget {
        nodes: usize,
        keys: usize,
    }

    struct Seed<'a, FV> {
        depth: usize,
        budget: &'a mut Budget,
        marker: PhantomData<FV>,
    }

    impl<'de, FV: Deserialize<'de>> Deserialize<'de> for RangeQuery<FV> {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            Seed {
                depth: 1,
                budget: &mut Budget::default(),
                marker: PhantomData,
            }
            .deserialize(deserializer)
        }
    }

    #[derive(Deserialize)]
    enum Tag {
        Eq,
        Gt,
        Ge,
        Lt,
        Le,
        Between,
        Include,
        Or,
        And,
        Not,
    }

    impl<'de, FV: Deserialize<'de>> DeserializeSeed<'de> for Seed<'_, FV> {
        type Value = RangeQuery<FV>;
        fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            self.budget.nodes += 1;
            if self.depth > RangeQuery::<FV>::MAX_DEPTH
                || self.budget.nodes > RangeQuery::<FV>::MAX_NODES
            {
                return Err(de::Error::custom(
                    "range query depth or node budget exceeded",
                ));
            }
            d.deserialize_enum(
                "RangeQuery",
                &[
                    "Eq", "Gt", "Ge", "Lt", "Le", "Between", "Include", "Or", "And", "Not",
                ],
                self,
            )
        }
    }
    impl<'de, FV: Deserialize<'de>> Visitor<'de> for Seed<'_, FV> {
        type Value = RangeQuery<FV>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a bounded range query")
        }
        fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<Self::Value, A::Error> {
            let (tag, variant) = data.variant::<Tag>()?;
            Ok(match tag {
                Tag::Eq => RangeQuery::Eq(variant.newtype_variant()?),
                Tag::Gt => RangeQuery::Gt(variant.newtype_variant()?),
                Tag::Ge => RangeQuery::Ge(variant.newtype_variant()?),
                Tag::Lt => RangeQuery::Lt(variant.newtype_variant()?),
                Tag::Le => RangeQuery::Le(variant.newtype_variant()?),
                Tag::Between => {
                    let (a, b) = variant.tuple_variant(2, Pair(PhantomData))?;
                    RangeQuery::Between(a, b)
                }
                Tag::Include => RangeQuery::Include(variant.newtype_variant_seed(Include {
                    budget: self.budget,
                    marker: PhantomData,
                })?),
                Tag::Or | Tag::And => {
                    let children = variant.newtype_variant_seed(Children {
                        depth: self.depth + 1,
                        budget: self.budget,
                        marker: PhantomData,
                    })?;
                    if matches!(tag, Tag::Or) {
                        RangeQuery::Or(children)
                    } else {
                        RangeQuery::And(children)
                    }
                }
                Tag::Not => RangeQuery::Not(Box::new(variant.newtype_variant_seed(Seed {
                    depth: self.depth + 1,
                    budget: self.budget,
                    marker: PhantomData,
                })?)),
            })
        }
    }
    struct Pair<FV>(PhantomData<FV>);
    impl<'de, FV: Deserialize<'de>> Visitor<'de> for Pair<FV> {
        type Value = (FV, FV);
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("two range endpoints")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let a = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(0, &self))?;
            let b = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(1, &self))?;
            if seq.next_element::<de::IgnoredAny>()?.is_some() {
                return Err(de::Error::invalid_length(3, &self));
            }
            Ok((a, b))
        }
    }
    struct Children<'a, FV> {
        depth: usize,
        budget: &'a mut Budget,
        marker: PhantomData<FV>,
    }
    impl<'de, FV: Deserialize<'de>> DeserializeSeed<'de> for Children<'_, FV> {
        type Value = Vec<Box<RangeQuery<FV>>>;
        fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            d.deserialize_seq(self)
        }
    }
    impl<'de, FV: Deserialize<'de>> Visitor<'de> for Children<'_, FV> {
        type Value = Vec<Box<RangeQuery<FV>>>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("bounded child queries")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut result = Vec::new();
            while let Some(child) = seq.next_element_seed(Seed {
                depth: self.depth,
                budget: self.budget,
                marker: PhantomData,
            })? {
                result.push(Box::new(child));
            }
            Ok(result)
        }
    }
    struct Include<'a, FV> {
        budget: &'a mut Budget,
        marker: PhantomData<FV>,
    }
    impl<'de, FV: Deserialize<'de>> DeserializeSeed<'de> for Include<'_, FV> {
        type Value = Vec<FV>;
        fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            d.deserialize_seq(self)
        }
    }
    impl<'de, FV: Deserialize<'de>> Visitor<'de> for Include<'_, FV> {
        type Value = Vec<FV>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("bounded Include keys")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut result = Vec::new();
            // Read at most one value beyond the bound. Avoid trusting size_hint.
            while let Some(value) = seq.next_element()? {
                self.budget.keys += 1;
                if self.budget.keys > RangeQuery::<FV>::MAX_INCLUDE_KEYS {
                    return Err(de::Error::custom("range query Include budget exceeded"));
                }
                result.push(value);
            }
            Ok(result)
        }
    }
}

/// Range query specification for flexible querying.
///
/// Queries compose: logical combinators (`And`, `Or`, `Not`) may contain any
/// other variants, enabling arbitrary boolean predicates over field values.
///
/// Ordering semantics:
///
/// Every variant emits its results in ascending key order. The query shape
/// never decides which end of a range a bounded scan keeps: the *method*
/// does. [`BTreeIndex::range_query_with`] walks matching keys upwards from
/// the smallest, so early termination (the callback's `continue` flag)
/// keeps the smallest matches; [`BTreeIndex::range_query_rev_with`] walks
/// downwards from the largest and keeps the largest matches. Both return
/// their keys ascending, with each key's own output preserved.
#[derive(Debug, Clone, Serialize)]
pub enum RangeQuery<FV> {
    /// Equal to a specific key
    Eq(FV),

    /// Greater than a specific key
    Gt(FV),

    /// Greater than or equal to a specific key
    Ge(FV),

    /// Less than a specific key
    Lt(FV),

    /// Less than or equal to a specific key
    Le(FV),

    /// Between two keys (inclusive on both ends).
    ///
    /// Empty result when `start > end`.
    Between(FV, FV),

    /// Include specific keys (duplicates are deduplicated, results sorted).
    Include(Vec<FV>),

    /// A logical OR query that returns the union of all subquery results
    /// (deduplicated, in ascending key order).
    Or(Vec<Box<RangeQuery<FV>>>),

    /// A logical AND query that returns the intersection of all subquery
    /// results.
    And(Vec<Box<RangeQuery<FV>>>),

    /// A logical NOT query that returns every indexed key not matched by the
    /// inner subquery.
    Not(Box<RangeQuery<FV>>),
}

impl<FV> RangeQuery<FV> {
    /// Maximum supported nesting depth for composed queries.
    ///
    /// Query evaluation is recursive, so an unbounded depth would let a
    /// deeply nested query built from untrusted input (e.g. a parsed filter
    /// expression) overflow the stack. Queries nested deeper than this are
    /// rejected: [`Self::try_convert_from`] returns an error and
    /// [`BTreeIndex::range_query_with`] returns an empty result.
    pub const MAX_DEPTH: usize = 64;

    /// Maximum number of predicate nodes accepted during parsing/evaluation.
    pub const MAX_NODES: usize = 4096;
    /// Maximum total number of Include entries in one query.
    pub const MAX_INCLUDE_KEYS: usize = 65_536;

    /// Checks depth and total work without recursively walking the query.
    pub fn validate(&self) -> Result<(), BoxError> {
        // Primitive predicates need no traversal stack or allocation.
        match self {
            Self::Eq(_)
            | Self::Gt(_)
            | Self::Ge(_)
            | Self::Lt(_)
            | Self::Le(_)
            | Self::Between(_, _) => return Ok(()),
            Self::Include(values) if values.len() <= Self::MAX_INCLUDE_KEYS => return Ok(()),
            _ => {}
        }
        let mut pending = vec![(self, 1)];
        let mut nodes = 0usize;
        let mut keys = 0usize;
        while let Some((query, depth)) = pending.pop() {
            nodes += 1;
            if depth > Self::MAX_DEPTH {
                return Err(format!(
                    "range query nesting depth {depth} exceeds the maximum of {}",
                    Self::MAX_DEPTH
                )
                .into());
            }
            if nodes > Self::MAX_NODES {
                return Err(format!(
                    "range query exceeds the maximum of {} nodes",
                    Self::MAX_NODES
                )
                .into());
            }
            match query {
                Self::And(children) | Self::Or(children) => {
                    // Avoid allocating a work stack proportional to an invalid
                    // wide input. Every queued child consumes at least one node.
                    if children.len() > Self::MAX_NODES.saturating_sub(nodes + pending.len()) {
                        return Err(format!(
                            "range query exceeds the maximum of {} nodes",
                            Self::MAX_NODES
                        )
                        .into());
                    }
                    pending.extend(children.iter().map(|child| (child.as_ref(), depth + 1)));
                }
                Self::Not(child) => pending.push((child, depth + 1)),
                Self::Include(values) => {
                    keys = keys.saturating_add(values.len());
                    if keys > Self::MAX_INCLUDE_KEYS {
                        return Err(format!(
                            "range query exceeds the maximum of {} Include keys",
                            Self::MAX_INCLUDE_KEYS
                        )
                        .into());
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Iteratively releases a query, including inputs too deep to drop safely.
    /// Useful for programmatically constructed input rejected by a caller.
    pub fn discard(self) {
        let mut pending = vec![self];
        while let Some(query) = pending.pop() {
            match query {
                Self::And(children) | Self::Or(children) => {
                    pending.extend(children.into_iter().map(|child| *child))
                }
                Self::Not(child) => pending.push(*child),
                _ => {}
            }
        }
    }

    /// Returns the nesting depth of this query (a leaf query has depth 1).
    ///
    /// Computed iteratively so that arbitrarily deep queries can be measured
    /// without recursing.
    pub fn depth(&self) -> usize {
        let mut max_depth = 0;
        let mut stack: Vec<(&RangeQuery<FV>, usize)> = vec![(self, 1)];
        while let Some((query, depth)) = stack.pop() {
            max_depth = max_depth.max(depth);
            match query {
                RangeQuery::And(queries) | RangeQuery::Or(queries) => {
                    stack.extend(queries.iter().map(|q| (q.as_ref(), depth + 1)));
                }
                RangeQuery::Not(query) => stack.push((query.as_ref(), depth + 1)),
                _ => {}
            }
        }
        max_depth
    }

    /// Translates a `RangeQuery<FV1>` into a `RangeQuery<FV>` by applying a
    /// `TryFrom<FV1>` conversion to every key.
    ///
    /// Useful for adapting user-facing typed queries (e.g. JSON values) to the
    /// storage-level field value type without rewriting query shape.
    ///
    /// # Errors
    ///
    /// Returns an error when any key conversion fails, or when the query is
    /// nested deeper than [`Self::MAX_DEPTH`].
    pub fn try_convert_from<FV1>(value: RangeQuery<FV1>) -> Result<Self, BoxError>
    where
        FV: Ord,
        FV: TryFrom<FV1, Error = BoxError>,
    {
        // Depth is checked once at the outermost call; the recursion below
        // then stays within a bounded stack budget. `depth()` is iterative,
        // and recursive calls go through `try_convert_from_inner`.
        if let Err(err) = value.validate() {
            value.discard();
            return Err(err);
        }
        Self::try_convert_from_inner(value)
    }

    fn try_convert_from_inner<FV1>(value: RangeQuery<FV1>) -> Result<Self, BoxError>
    where
        FV: Ord,
        FV: TryFrom<FV1, Error = BoxError>,
    {
        match value {
            RangeQuery::Eq(key) => Ok(RangeQuery::Eq(key.try_into()?)),
            RangeQuery::Gt(key) => Ok(RangeQuery::Gt(key.try_into()?)),
            RangeQuery::Ge(key) => Ok(RangeQuery::Ge(key.try_into()?)),
            RangeQuery::Lt(key) => Ok(RangeQuery::Lt(key.try_into()?)),
            RangeQuery::Le(key) => Ok(RangeQuery::Le(key.try_into()?)),
            RangeQuery::Between(start_key, end_key) => Ok(RangeQuery::Between(
                start_key.try_into()?,
                end_key.try_into()?,
            )),
            RangeQuery::Include(keys) => {
                let converted_keys = keys
                    .into_iter()
                    .map(|key| key.try_into())
                    .collect::<Result<Vec<FV>, _>>()?;
                Ok(RangeQuery::Include(converted_keys))
            }
            RangeQuery::And(queries) => {
                let converted_queries = queries
                    .into_iter()
                    .map(|query| RangeQuery::try_convert_from_inner(*query).map(Box::new))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RangeQuery::And(converted_queries))
            }
            RangeQuery::Or(queries) => {
                let converted_queries = queries
                    .into_iter()
                    .map(|query| RangeQuery::try_convert_from_inner(*query).map(Box::new))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RangeQuery::Or(converted_queries))
            }
            RangeQuery::Not(query) => {
                let converted_query = RangeQuery::try_convert_from_inner(*query)?;
                Ok(RangeQuery::Not(Box::new(converted_query)))
            }
        }
    }
}

impl<PK: BTreeKey, FV: BTreeKey> BTreeIndex<PK, FV> {
    /// Queries the index for an exact key match
    ///
    /// # Arguments
    ///
    /// * `field_value` - Key to query for
    /// * `f` - Function to apply to the posting value
    ///
    /// # Returns
    ///
    /// * `Option<R>` - Result of the function applied to the posting value
    ///
    /// # Re-entrancy
    ///
    /// `f` runs while internal locks are held. It must not call back into the
    /// same index (e.g. `insert` / `remove`), or it may deadlock.
    pub fn query_with<F, R>(&self, field_value: &FV, f: F) -> Option<R>
    where
        F: FnOnce(&Vec<PK>) -> Option<R>,
    {
        self.postings
            .get(field_value)
            .and_then(|posting| f(&posting.docs))
    }

    /// Queries the index using a range query
    ///
    /// # Arguments
    ///
    /// * `query` - Range query specification
    /// * `f` - Function to apply to the posting value. The function should return a tuple
    ///   containing a continuation flag and the results for this key.
    ///
    /// # Returns
    ///
    /// * `Vec<R>` - Vector of results from the function applied to the posting values
    ///
    /// # Re-entrancy
    ///
    /// `f` runs while internal locks are held (including the btree read lock
    /// during range scans). It must not call back into the same index, or it
    /// may deadlock.
    ///
    /// # Depth limit
    ///
    /// Queries nested deeper than [`RangeQuery::MAX_DEPTH`] are rejected
    /// (query evaluation is recursive; the cap prevents a stack overflow on
    /// maliciously deep queries). **This non-`try` method cannot report the
    /// rejection through its return type**: it returns an empty result — which
    /// is indistinguishable from "no matches" — and emits a `log::warn!` with
    /// the index name and the offending depth. If you need a hard error
    /// instead, validate the depth up-front: [`RangeQuery::try_convert_from`]
    /// returns an `Err` for over-deep queries, and [`RangeQuery::depth`] lets
    /// you check the cap explicitly before calling this method.
    pub fn range_query_with<F, R>(&self, query: RangeQuery<FV>, f: F) -> Vec<R>
    where
        F: FnMut(&FV, &Vec<PK>) -> (bool, Vec<R>),
    {
        self.range_query_or_empty(query, false, f)
    }

    /// Like [`range_query_with`](Self::range_query_with), but walks the key
    /// space from the **largest** matching key downwards.
    ///
    /// Both directions stop as soon as `f` returns `false`, so a caller that
    /// wants the *last* page of a range pays for that page only. The results
    /// are returned in ascending key order either way — the direction decides
    /// **which** keys a bounded scan collects, never how they are ordered.
    ///
    /// Scan direction is the caller's choice precisely because it cannot be
    /// derived from the query shape: `Lt(x)` bounded by a limit means "the
    /// smallest matches below x" to one caller and "the largest" to another,
    /// and silently picking one made the same predicate return opposite ends
    /// depending on where it appeared in a composite filter.
    pub fn range_query_rev_with<F, R>(&self, query: RangeQuery<FV>, f: F) -> Vec<R>
    where
        F: FnMut(&FV, &Vec<PK>) -> (bool, Vec<R>),
    {
        self.range_query_or_empty(query, true, f)
    }

    /// Queries a range, returning invalid-query errors instead of hiding them
    /// as empty results. The callback has the same lock/reentrancy contract as
    /// `range_query_with`.
    pub fn try_range_query_with<F, R>(
        &self,
        query: RangeQuery<FV>,
        f: F,
    ) -> Result<Vec<R>, BTreeError>
    where
        F: FnMut(&FV, &Vec<PK>) -> (bool, Vec<R>),
    {
        self.range_query_inner(query, false, f)
    }

    /// Fallible descending counterpart of `range_query_rev_with`.
    pub fn try_range_query_rev_with<F, R>(
        &self,
        query: RangeQuery<FV>,
        f: F,
    ) -> Result<Vec<R>, BTreeError>
    where
        F: FnMut(&FV, &Vec<PK>) -> (bool, Vec<R>),
    {
        self.range_query_inner(query, true, f)
    }

    pub(super) fn range_query_or_empty<F, R>(
        &self,
        query: RangeQuery<FV>,
        descending: bool,
        f: F,
    ) -> Vec<R>
    where
        F: FnMut(&FV, &Vec<PK>) -> (bool, Vec<R>),
    {
        self.range_query_inner(query, descending, f)
            .unwrap_or_else(|err| {
                log::warn!(
                    "BTreeIndex '{}': {err}; returning an empty result",
                    self.name
                );
                Vec::new()
            })
    }

    pub(super) fn range_query_inner<F, R>(
        &self,
        query: RangeQuery<FV>,
        descending: bool,
        mut f: F,
    ) -> Result<Vec<R>, BTreeError>
    where
        F: FnMut(&FV, &Vec<PK>) -> (bool, Vec<R>),
    {
        if let Err(source) = query.validate() {
            query.discard();
            return Err(self.generic_error(source));
        }
        Ok(match query {
            RangeQuery::Eq(key) => match self.postings.get(&key) {
                Some(posting) => f(&key, &posting.docs).1,
                None => Vec::new(),
            },
            RangeQuery::Gt(start_key) => {
                let btree = self.btree.read();
                self.walk_keys(
                    btree.range((Bound::Excluded(start_key), Bound::Unbounded)),
                    descending,
                    &mut f,
                )
            }
            RangeQuery::Ge(start_key) => {
                let btree = self.btree.read();
                self.walk_keys(btree.range(start_key..), descending, &mut f)
            }
            RangeQuery::Lt(end_key) => {
                let btree = self.btree.read();
                self.walk_keys(btree.range(..end_key), descending, &mut f)
            }
            RangeQuery::Le(end_key) => {
                let btree = self.btree.read();
                self.walk_keys(btree.range(..=end_key), descending, &mut f)
            }
            RangeQuery::Between(start_key, end_key) => {
                if start_key > end_key {
                    return Ok(Vec::new()); // empty result for invalid range
                }
                let btree = self.btree.read();
                self.walk_keys(btree.range(start_key..=end_key), descending, &mut f)
            }
            RangeQuery::Include(mut keys) => {
                keys.sort_unstable();
                keys.dedup();
                self.walk_keys(keys.iter(), descending, &mut f)
            }
            query @ (RangeQuery::And(_) | RangeQuery::Or(_) | RangeQuery::Not(_)) => {
                let spans = query::compile(&query);
                let btree = self.btree.read();
                let keys = spans
                    .into_iter()
                    .flat_map(|bounds| btree.range::<FV, _>(bounds));
                self.walk_keys(keys, descending, &mut f)
            }
        })
    }

    /// Visits `keys` in the requested direction, feeding each key's posting
    /// to `f` until it asks to stop.
    ///
    /// `descending` decides which keys a bounded scan collects, never how
    /// they are ordered on the way out: the output always ascends by key
    /// (a descending walk is reversed group by group), while each key's own
    /// output stays as the callback produced it. Both directions stop as
    /// soon as `f` says so, so either end of a range is equally cheap to
    /// page.
    pub(super) fn walk_keys<'a, I, F, R>(&self, keys: I, descending: bool, f: &mut F) -> Vec<R>
    where
        FV: 'a,
        I: DoubleEndedIterator<Item = &'a FV>,
        F: FnMut(&FV, &Vec<PK>) -> (bool, Vec<R>),
    {
        if descending {
            let mut groups: Vec<Vec<R>> = Vec::new();
            for k in keys.rev() {
                if let Some(posting) = self.postings.get(k) {
                    let (conti, rt) = f(k, &posting.docs);
                    if !rt.is_empty() {
                        groups.push(rt);
                    }
                    if !conti {
                        break;
                    }
                }
            }
            return groups.into_iter().rev().flatten().collect();
        }

        let mut results = Vec::new();
        for k in keys {
            if let Some(posting) = self.postings.get(k) {
                let (conti, rt) = f(k, &posting.docs);
                if results.is_empty() {
                    results = rt;
                } else {
                    results.extend(rt);
                }
                if !conti {
                    break;
                }
            }
        }
        results
    }

    /// Returns a vector of keys in the index
    /// This method is useful for iterating over all keys in the index.
    /// It supports pagination with `cursor` and `limit` parameters.
    /// # Arguments
    ///
    /// * `cursor` - The cursor to start pagination from (exclusive)
    /// * `limit` - Maximum number of keys to return
    ///
    /// # Returns
    ///
    /// * `Vec<FV>` - Vector of field values (keys) in the index
    ///
    pub fn keys(&self, cursor: Option<FV>, limit: Option<usize>) -> Vec<FV> {
        let start = cursor.map_or(Bound::Unbounded, Bound::Excluded);
        self.btree
            .read()
            .range((start, Bound::Unbounded))
            .take(limit.unwrap_or(usize::MAX))
            .cloned()
            .collect()
    }

    #[cfg(test)]
    pub(super) fn range_keys(&self, query: RangeQuery<FV>) -> Vec<FV> {
        self.range_query_with(query, |key, _| (true, vec![key.clone()]))
    }
}
