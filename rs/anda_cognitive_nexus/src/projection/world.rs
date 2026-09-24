//! # World time (Spec §25.2–§25.5)
//!
//! What an Assertion's `valid_time` means at a projection instant, after
//! temporal succession. Pure computation over the eligible Assertions of one
//! slot, recomputed at every basis and never written back (§14.3): an
//! Assertion ended by its successor stays `active` and keeps answering for
//! its own time.
//!
//! Every endpoint is a closed range of possible instants `[lo, hi]`. An exact
//! instant is `[x, x]`, a time bound is `[earliest, latest]`, and a missing
//! side is infinite — spelled with the engine's range sentinels, `""` for -∞
//! and `"~"` for +∞, which sort around every canonical Timestamp.

use crate::id::ElementId;
use crate::store::rows::AssertionRow;
use crate::time::{Point, TIME_MAX, TIME_MIN};
use anda_kip::Json;

/// A closed range of possible instants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Span {
    pub lo: String,
    pub hi: String,
}

impl Span {
    fn of(point: &Point) -> Self {
        let range = point.range();
        Span {
            lo: range.lo.unwrap_or_else(|| TIME_MIN.to_string()),
            hi: range.hi.unwrap_or_else(|| TIME_MAX.to_string()),
        }
    }
}

/// Where an effective interval lies relative to one instant (§25.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Placement {
    Inside,
    Outside,
    Indeterminate,
}

/// One eligible Assertion, as world time sees it.
#[derive(Clone, Debug)]
pub(crate) struct Timed {
    /// The Proposition it is about: the candidate value.
    pub proposition: ElementId,
    /// The `functional_by` partition of that value; empty otherwise.
    pub partition: String,
    /// The actor's equality key; empty when none was recorded.
    pub actor: String,
    /// The canonical context set, joined.
    pub context: String,
    pub stance: String,
    /// Stated or observed, or a written `from` (§25.4 "who takes part").
    pub takes_part: bool,
    pub from_exact: bool,
    pub until_written: bool,
    /// The latest instant by which it claims to have begun (§25.4).
    pub start_key: String,
    /// The effective start and end, narrowed by succession.
    pub start: Span,
    pub end: Span,
}

/// The written interval as spans: a missing `from` is the bound
/// {latest: asserted_at} (§25.2), a missing `until` is open.
fn written(from: Option<&Point>, until: Option<&Point>, asserted_at: &str) -> (Span, Span) {
    let start = match from {
        Some(point) => Span::of(point),
        None => Span {
            lo: TIME_MIN.to_string(),
            hi: if asserted_at.is_empty() {
                TIME_MAX.to_string()
            } else {
                asserted_at.to_string()
            },
        },
    };
    let end = until.map_or_else(
        || Span {
            lo: TIME_MAX.to_string(),
            hi: TIME_MAX.to_string(),
        },
        Span::of,
    );
    (start, end)
}

/// Where an interval lies at `at` (§25.5).
fn place(start: &Span, end: &Span, at: &str) -> Placement {
    if start.lo.as_str() > at || end.hi.as_str() <= at {
        Placement::Outside
    } else if start.hi.as_str() <= at && end.lo.as_str() > at {
        Placement::Inside
    } else {
        Placement::Indeterminate
    }
}

/// Where one Assertion's *written* interval lies at `at`, without
/// succession: what `FIND … FOR TIME` restricts a row by, since a row is one
/// Assertion rather than a slot's line. `valid_time` is the wire object, its
/// endpoints exact Timestamps or time bounds (§25.5).
pub(crate) fn place_written(valid_time: &Json, asserted_at: &str, at: &str) -> Placement {
    let endpoint = |name: &str| Point::read(valid_time.get(name), name).ok().flatten();
    let (from, until) = (endpoint("from"), endpoint("until"));
    let (start, end) = written(from.as_ref(), until.as_ref(), asserted_at);
    place(&start, &end, at)
}

impl Timed {
    pub fn new(row: &AssertionRow, proposition: ElementId, partition: String) -> Self {
        let from = Point::load(&row.valid_from);
        let until = Point::load(&row.valid_until);
        let (start, end) = written(from.as_ref(), until.as_ref(), &row.asserted_at);
        let start_key = match &from {
            Some(Point::Exact(at)) => at.clone(),
            Some(Point::Bound {
                latest: Some(at), ..
            }) => at.clone(),
            _ => row.asserted_at.clone(),
        };
        // The written set; projection replaces it with the merge-resolved one.
        let mut context: Vec<String> = row.context_refs.iter().map(|v| v.to_string()).collect();
        context.sort();
        context.dedup();
        Timed {
            proposition,
            partition,
            actor: row.asserted_by_key.clone(),
            context: context.join("\u{1f}"),
            stance: row.stance.clone(),
            takes_part: matches!(row.mode.as_str(), "stated" | "observed") || from.is_some(),
            from_exact: from.as_ref().is_some_and(Point::is_exact),
            until_written: until.is_some(),
            start_key,
            start,
            end,
        }
    }

    /// Where the effective interval lies at `at` (§25.5).
    pub fn place(&self, at: &str) -> Placement {
        place(&self.start, &self.end, at)
    }

    /// The finite instants after `at` at which this interval's placement can
    /// change, for `next_invalid_at` (§21.12).
    pub fn boundaries_after<'a>(&'a self, at: &'a str) -> impl Iterator<Item = &'a str> {
        [&self.start.lo, &self.start.hi, &self.end.lo, &self.end.hi]
            .into_iter()
            .map(String::as_str)
            .filter(move |t| *t > at && *t != TIME_MAX)
    }
}

/// Narrows every interval by temporal succession (§25.4).
///
/// `slot` enables slot lines — the functional or `functional_by` case — where
/// distinct values of one slot by one actor succeed one another; proposition
/// lines, where one actor's opposite stances on one Proposition do, always
/// apply. Only the rows passed in take part, so a retracted, superseded,
/// hidden or out-of-context Assertion never changes a visible one.
pub(crate) fn succeed(rows: &mut [Timed], slot: bool) {
    use std::collections::BTreeMap;
    let mut lines: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        if row.actor.is_empty() || !row.takes_part {
            continue;
        }
        lines
            .entry(format!(
                "prop\u{1f}{}\u{1f}{}\u{1f}{}",
                row.actor, row.context, row.proposition
            ))
            .or_default()
            .push(index);
        if slot && row.stance == "support" {
            lines
                .entry(format!(
                    "slot\u{1f}{}\u{1f}{}\u{1f}{}",
                    row.actor, row.context, row.partition
                ))
                .or_default()
                .push(index);
        }
    }
    let mut starts: Vec<Span> = rows.iter().map(|r| r.start.clone()).collect();
    let mut ends: Vec<Span> = rows.iter().map(|r| r.end.clone()).collect();
    for (key, line) in &lines {
        let is_slot = key.starts_with("slot");
        let disagree = |x: &Timed, y: &Timed| {
            if is_slot {
                x.proposition != y.proposition
            } else {
                x.stance != y.stance
            }
        };
        // A predecessor narrows a start that was not written exactly.
        let mut line_start: BTreeMap<usize, Span> = BTreeMap::new();
        for &r in line {
            let me = &rows[r];
            let mut span = me.start.clone();
            if !me.from_exact
                && let Some(q) = line
                    .iter()
                    .map(|&q| &rows[q])
                    .filter(|q| disagree(q, me) && q.start_key < me.start_key)
                    .max_by(|a, b| a.start_key.cmp(&b.start_key))
            {
                span = Span {
                    lo: me.start.lo.clone().max(q.start_key.clone()),
                    hi: me.start_key.clone(),
                };
            }
            let cur = &starts[r];
            starts[r] = Span {
                lo: cur.lo.clone().max(span.lo.clone()),
                hi: cur.hi.clone().max(span.hi.clone()),
            };
            line_start.insert(r, span);
        }
        // A successor ends an open interval at its own effective start; tied
        // nearest successors combine bound by bound, so arrival order never
        // decides which one ended it.
        for &r in line {
            let me = &rows[r];
            if me.until_written {
                continue;
            }
            let successors: Vec<usize> = line
                .iter()
                .copied()
                .filter(|&n| disagree(&rows[n], me) && rows[n].start_key > me.start_key)
                .collect();
            let Some(nearest) = successors.iter().map(|&n| &rows[n].start_key).min() else {
                continue;
            };
            for &n in successors
                .iter()
                .filter(|&&n| &rows[n].start_key == nearest)
            {
                let e = &line_start[&n];
                let cur = &ends[r];
                ends[r] = Span {
                    lo: cur.lo.clone().min(e.lo.clone()),
                    hi: cur.hi.clone().min(e.hi.clone()),
                };
            }
        }
    }
    for (index, row) in rows.iter_mut().enumerate() {
        row.start = starts[index].clone();
        row.end = ends[index].clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        proposition: u64,
        actor: &str,
        stance: &str,
        mode: &str,
        asserted_at: &str,
        from: &str,
        until: &str,
    ) -> Timed {
        let row = AssertionRow {
            asserted_by_key: actor.to_string(),
            stance: stance.to_string(),
            mode: mode.to_string(),
            asserted_at: asserted_at.to_string(),
            valid_from: from.to_string(),
            valid_until: until.to_string(),
            ..Default::default()
        };
        Timed::new(
            &row,
            ElementId::new(anda_kip::ElementKind::Proposition, proposition),
            String::new(),
        )
    }

    const JAN: &str = "2026-01-10T00:00:00.000Z";
    const SEP: &str = "2026-09-01T00:00:00.000Z";

    #[test]
    fn a_world_change_ends_the_old_value_without_rewriting_it() {
        let mut rows = vec![
            row(1, "alice", "support", "stated", JAN, "", ""),
            row(2, "alice", "support", "stated", SEP, SEP, ""),
        ];
        succeed(&mut rows, true);
        assert_eq!(rows[0].place("2026-06-01T00:00:00.000Z"), Placement::Inside);
        assert_eq!(
            rows[0].place("2026-09-20T00:00:00.000Z"),
            Placement::Outside
        );
        assert_eq!(rows[1].place("2026-09-20T00:00:00.000Z"), Placement::Inside);
        // A claim with no stated start is indeterminate before it was made.
        assert_eq!(
            rows[0].place("2026-01-05T00:00:00.000Z"),
            Placement::Indeterminate
        );
    }

    #[test]
    fn different_actors_and_inferences_never_succeed() {
        let mut actors = vec![
            row(1, "alice", "support", "stated", JAN, "", ""),
            row(2, "bob", "support", "stated", SEP, SEP, ""),
        ];
        succeed(&mut actors, true);
        assert_eq!(
            actors[0].place("2026-09-20T00:00:00.000Z"),
            Placement::Inside
        );
        let mut inferred = vec![
            row(1, "brain", "support", "inferred", JAN, "", ""),
            row(2, "brain", "support", "inferred", SEP, "", ""),
        ];
        succeed(&mut inferred, true);
        assert_eq!(
            inferred[0].place("2026-09-20T00:00:00.000Z"),
            Placement::Inside
        );
    }

    #[test]
    fn tied_successors_combine_bound_by_bound() {
        // T2: two successors with the same start key end the predecessor at
        // the earliest of their starts, whatever order they arrived in.
        let bound =
            r#"{"earliest":"2026-03-01T00:00:00.000Z","latest":"2026-09-01T00:00:00.000Z"}"#;
        for order in [[1, 2], [2, 1]] {
            let mut rows = vec![row(3, "alice", "support", "stated", JAN, JAN, "")];
            for p in order {
                rows.push(if p == 1 {
                    row(1, "alice", "support", "stated", SEP, SEP, "")
                } else {
                    row(2, "alice", "support", "stated", SEP, bound, "")
                });
            }
            succeed(&mut rows, true);
            assert_eq!(rows[0].end.lo, "2026-03-01T00:00:00.000Z");
            assert_eq!(rows[0].end.hi, SEP);
        }
    }
}
