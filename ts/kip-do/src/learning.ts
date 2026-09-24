/** Final-transaction CognitiveMemory invariants. Shared semantics with tx/learning.rs. */
import { errors } from './errors.js'
import { asJsonMap, canonicalJson, type Json, type JsonMap } from './json.js'
import { parseElementId, formatElementId, type ElementKind } from './id.js'
import { State, TABLES, type Element } from './store/rows.js'
import { referenceText } from './store/references.js'
import { render } from './view.js'
import { digest } from './schema/contracts.js'
import { artifactValue, requireArtifactMaterial } from './control.js'
import { type EvaluationPolicy, type EvaluationSamples } from './cognitive.js'
import { baseElement, diffPlanes } from './tx.js'
import type { Transaction, Staged } from './tx.js'

import { PROFILE_PREFIX as PROFILE, profileFacet } from './schema/profile-ref.js'
const obj = asJsonMap
const refs = (v: Json | undefined): string[] =>
  Array.isArray(v) ? v.map(referenceText) : []
const same = (a: Json | undefined, b: Json | undefined): boolean =>
  canonicalJson(a ?? null) === canonicalJson(b ?? null)
const sameRefs = (a: Json | undefined, b: Json | undefined): boolean =>
  same(refs(a).sort(), refs(b).sort())
const facet = profileFacet
const edge = (e: Element, name: string): string[] =>
  refs(e.row.structural[PROFILE + name])
const type = (e: Element, name: string): boolean =>
  e.kind === 'Concept' && e.row.schema_ref === PROFILE + name
const fail = (message: string): never => {
  throw errors.constraintViolation(message)
}
const idOf = (e: Element): string =>
  formatElementId({ kind: e.kind, seq: e.row.id })

export function validateLearning(tx: Transaction): void {
  if (tx.cx.origin.import) return
  const { store, authority, auth } = tx,
    space = tx.cx.space
  const pending = [...tx.staged].filter(
    ([, s]) => s.changed && s.verb !== 'purge',
  )
  if (
    !pending.some(
      ([, s]) =>
        type(s.element, 'Skill') ||
        type(s.element, 'SkillRevision') ||
        [
          'DecisionRecord',
          'AttemptRecord',
          'OutcomeRecord',
          'TrialRecord',
          'EvaluationRecord',
        ].some((n) => facet(s.element, n)),
    )
  )
    return
  const final = (reference: string): Element => {
    const row =
      tx.staged.get(reference)?.element ?? store.load(parseElementId(reference))
    if (!row || (row.row.space !== space && !tx.staged.get(reference)?.isNew))
      throw errors.notFoundOrNotVisible('contract reference unavailable')
    if (tx.staged.get(reference)?.isNew) return row
    const visibility = authority.mayRead(row, auth)
    if (!visibility?.content || visibility.constraints.fields.length)
      throw errors.notFoundOrNotVisible('complete contract input unavailable')
    return row
  }
  const record = (reference: string, name: string): [Element, JsonMap] => {
    const row = final(reference),
      value = facet(row, name)
    if (
      !value ||
      (row.row.state !== State.ACTIVE && !tx.staged.get(reference)?.isNew)
    )
      fail('required active standard record missing')
    return [row, value!]
  }
  const revision = (reference: string): Element => {
    const row = final(reference)
    if (!type(row, 'SkillRevision'))
      fail('behavior must name exact SkillRevision')
    return row
  }
  const origin = (row: Element): string =>
    row.row.origin.import
      ? ''
      : tx.staged.get(idOf(row))?.isNew
        ? auth.principal_id
        : String(row.row.origin.principal_id ?? '')
  const universe = (kind: ElementKind): Element[] => {
    const rows = new Map<string, Element>()
    for (const row of store.all<Element['row']>(
      TABLES[kind],
      `SELECT * FROM ${TABLES[kind]} WHERE space = ?`,
      space,
    )) {
      const element = { kind, row } as Element
      rows.set(idOf(element), element)
    }
    for (const [id, s] of tx.staged)
      if (s.element.kind === kind) rows.set(id, s.element)
    return [...rows.values()]
  }
  // Read only by the evaluation checks, which reason over a whole trial; an
  // ordinary attempt or outcome never pays for reading every record.
  const universes = new Map<ElementKind, Element[]>()
  const all = (kind: ElementKind): Element[] => {
    let found = universes.get(kind)
    if (found === undefined) universes.set(kind, (found = universe(kind)))
    return found
  }
  /** The staged elements this transaction created, in creation order. */
  const created = [...tx.staged.values()]
    .filter((s) => s.isNew)
    .map((s) => s.element)
  /**
   * Every Activity that lists `id` among its outputs, as this transaction
   * would leave it: the stored ones through the reverse index, with the
   * staged Activities in their place, in id order.
   */
  const producers = (id: string): Element[] => {
    const found = new Map<string, Element>()
    for (const a of store.activitiesWithOutput(space, parseElementId(id))) found.set(idOf(a), a)
    for (const [key, s] of tx.staged) if (s.element.kind === 'Activity') found.set(key, s.element)
    return [...found.values()].sort((a, b) => a.row.id - b.row.id)
  }
  // Space-unique record identities. Only a value this transaction holds can
  // be newly duplicated, so each is looked up rather than every record read.
  const unique = (
    kind: 'Activity' | 'Evidence',
    name: string,
    member: string,
    message: string,
  ): void => {
    const values = new Set<string>()
    for (const s of tx.staged.values()) {
      const r = s.element.kind === kind ? facet(s.element, name) : null
      if (!r) continue
      const value = String(r[member])
      if (values.has(value)) fail(message)
      values.add(value)
    }
    for (const value of values) {
      const rows = store.sql
        .exec<{ id: number }>(
          `SELECT id FROM ${TABLES[kind]}
             WHERE space = ? AND CAST(json_extract(facets, ?) AS TEXT) = ?`,
          space,
          `$."${PROFILE}${name}".${member}`,
          value,
        )
        .toArray()
      if (rows.some((row) => !tx.staged.has(formatElementId({ kind, seq: row.id }))))
        fail(message)
    }
  }
  unique('Activity', 'AttemptRecord', 'attempt_id', 'attempt_id must be Space-unique')
  unique(
    'Evidence',
    'OutcomeRecord',
    'observation_key',
    'observation_key must deduplicate source events',
  )
  const guarded = (id: string, s: Staged): void => {
    if (!s.baseRow || tx.guarded.get(id)?.has('version')) return
    const required = [...diffPlanes(s.element, s.baseRow).planes]
    if (required.some((p) => !tx.guarded.get(id)?.has(p)))
      throw errors.versionConflict(
        'every changed lifecycle/cache plane requires a version guard',
      )
  }
  const evaluationPolicy = (
    pin: JsonMap,
    seq = Number.MAX_SAFE_INTEGER,
  ): EvaluationPolicy => {
    const row = store.controlAt(space, `evaluation_policy/${pin.id}`, seq)
    if (
      !row ||
      obj(row.value).version !== pin.version ||
      digest(row.value) !== pin.content_digest
    )
      fail('evaluation policy pin does not match protected bytes')
    return row!.value as unknown as EvaluationPolicy
  }
  const validateTrial = (trial: JsonMap, row: Element): void => {
    const policy = evaluationPolicy(obj(trial.evaluation_policy))
    if (
      !policy.allowed_rules.includes(String(obj(trial.rule).content_digest)) ||
      !policy.allowed_parameters.includes(
        String(obj(trial.parameters).content_digest),
      ) ||
      obj(trial.comparability).observer_control_digest !==
        policy.observer_control_digest
    )
      fail('trial artifacts or observer control not allowed by policy')
    if (Number(trial.quota) < policy.minimum_independent_attempts)
      fail('trial quota below policy minimum')
    if (!store.evaluationRules.supports(String(obj(trial.rule).content_digest)))
      throw errors.unsupportedCapability(
        'pinned trial rule has no trusted host evaluator',
      )
    const rule = artifactValue(store, space, obj(trial.rule)),
      parameters = artifactValue(store, space, obj(trial.parameters)),
      replay = obj(artifactValue(store, space, obj(trial.replay_artifact)))
    if (
      !same(replay.rule, rule) ||
      !same(replay.parameters, parameters) ||
      !same(replay.basis, trial.basis)
    )
      fail('trial replay must retain exact rule, parameters and basis')
    for (const ref of refs(trial.baseline_attempt_refs)) {
      const [a] = record(ref, 'AttemptRecord')
      if (
        tx.staged.get(ref)?.isNew ||
        (!tx.staged.get(idOf(row))?.isNew && a.row.seq >= row.row.seq)
      )
        fail('baseline attempts must precede trial assignment')
    }
    for (const ref of refs(trial.baseline_outcome_refs))
      record(ref, 'OutcomeRecord')
    requireArtifactMaterial(store, space, obj(trial.replay_artifact), [
      ...refs(trial.revision_refs),
      ...refs(trial.baseline_attempt_refs),
      ...refs(trial.baseline_outcome_refs),
    ])
    const baselineAttempts: JsonMap = {},
      baselineOutcomes: JsonMap = {}
    for (const ref of refs(trial.baseline_attempt_refs)) {
      const [row, value] = record(ref, 'AttemptRecord')
      baselineAttempts[ref] = { record: value, principal_id: origin(row) }
    }
    for (const ref of refs(trial.baseline_outcome_refs)) {
      const [row, value] = record(ref, 'OutcomeRecord')
      if (row.kind === 'Evidence')
        baselineOutcomes[ref] = {
          record: value,
          status: row.row.status,
          corrected_by: row.row.corrected_by,
          principal_id: origin(row),
          observed_at: row.row.observed_at,
        }
    }
    if (
      !same(replay.baseline_attempts, baselineAttempts) ||
      !same(replay.baseline_outcomes, baselineOutcomes)
    )
      fail('trial replay must retain exact predeclared baseline inputs')
  }
  const validateEvaluation = (evaluation: JsonMap, row: Element): void => {
    if (String(evaluation.cutoff) > tx.cx.at)
      fail('evaluation cutoff cannot be in future')
    const from = String(evaluation.from_status),
      to = String(evaluation.to_status)
    for (const ref of refs(evaluation.revision_refs))
      for (const familyRef of edge(revision(ref), 'revision_of')) {
        const family = final(familyRef),
          before = store.load(parseElementId(familyRef)) ?? family
        if (
          obj(render(before).attributes).status !== from ||
          obj(render(family).attributes).status !== to ||
          !same(edge(family, 'current_revision'), [ref])
        )
          fail(
            'verdict must describe actual selected revision and lifecycle transition',
          )
        if (
          row.kind !== 'Activity' ||
          !refs(row.row.outputs as Json).includes(familyRef)
        )
          fail('verdict must name affected Skill in outputs')
      }
    requireArtifactMaterial(
      store,
      space,
      obj(evaluation.replay_artifact),
      refs(evaluation.revision_refs),
    )
    const replay = obj(
      artifactValue(store, space, obj(evaluation.replay_artifact)),
    )
    if (evaluation.trial_ref === null) {
      if (
        to !== 'revoked' ||
        refs(evaluation.attempt_refs).length ||
        refs(evaluation.outcome_refs).length ||
        !['safety_failure', 'withdrawal'].includes(
          String(obj(evaluation.comparison).status),
        )
      )
        fail('a verdict without trial can only withdraw or urgently demote')
      if (!same(replay.comparison, evaluation.comparison))
        fail('withdrawal replay must retain reason')
      return
    }
    const trialRef = String(evaluation.trial_ref),
      [trialRow, trial] = record(trialRef, 'TrialRecord'),
      comparisonPolicy = obj(trial.comparability)
    if (
      !sameRefs(evaluation.revision_refs, trial.revision_refs) ||
      evaluation.rule_digest !== obj(trial.rule).content_digest ||
      evaluation.parameters_digest !== obj(trial.parameters).content_digest
    )
      fail('evaluation must bind trial revisions and artifacts')
    if (
      to === 'revoked' &&
      ['safety_failure', 'withdrawal'].includes(
        String(obj(evaluation.comparison).status),
      ) &&
      !refs(evaluation.attempt_refs).length &&
      !refs(evaluation.outcome_refs).length
    ) {
      if (!same(replay.comparison, evaluation.comparison))
        fail('withdrawal replay must retain reason')
      return
    }
    const policy = evaluationPolicy(
      obj(trial.evaluation_policy),
      tx.staged.get(trialRef)?.isNew ? tx.snapshotSeq : trialRow.row.seq,
    )
    const current = store.controlAt(space, `evaluation_policy/${policy.id}`)
      ?.value as unknown as EvaluationPolicy | undefined
    if (
      !current ||
      !current.allowed_rules.includes(String(evaluation.rule_digest)) ||
      !current.allowed_parameters.includes(
        String(evaluation.parameters_digest),
      ) ||
      current.observer_control_digest !== policy.observer_control_digest
    )
      fail('current policy no longer allows evaluation')
    if (Number(trial.quota) < current!.minimum_independent_attempts)
      fail('trial below current policy minimum')
    const selected = refs(evaluation.attempt_refs),
      outcomes = refs(evaluation.outcome_refs),
      missing = refs(evaluation.missing_attempt_refs)
    const allAttempts = [...selected, ...refs(trial.baseline_attempt_refs)],
      allOutcomes = [...outcomes, ...refs(trial.baseline_outcome_refs)]
    requireArtifactMaterial(store, space, obj(evaluation.replay_artifact), [
      ...allAttempts,
      ...allOutcomes,
      ...refs(trial.revision_refs),
      trialRef,
    ])
    if (new Set(allAttempts).size !== allAttempts.length)
      fail('treatment and baseline attempts must be distinct and unique')
    const samples: EvaluationSamples = { treatment: {}, baseline: {} },
      units = new Set<string>(),
      replayAttempts: JsonMap = {},
      replayOutcomes: JsonMap = {}
    for (const ref of allAttempts) {
      const [attemptRow, attempt] = record(ref, 'AttemptRecord'),
        treatment = selected.includes(ref)
      if (
        treatment &&
        (attempt.trial_ref !== trialRef ||
          !sameRefs(attempt.applied_revisions, trial.revision_refs))
      )
        fail('treatment belongs to another trial or revision bundle')
      if (
        attempt.environment_digest !== comparisonPolicy.environment_digest ||
        attempt.preconditions_satisfied !== 'yes' ||
        String(attempt.started_at) > String(evaluation.cutoff)
      )
        fail('attempt outside comparability or cutoff')
      const unit =
        comparisonPolicy.sampling_unit === 'attempt'
          ? String(attempt.attempt_id)
          : String(
              obj(attempt.context)[String(comparisonPolicy.sampling_unit)] ??
                '',
            )
      if (!unit || units.has(`${treatment}:${unit}`))
        fail('evaluation requires independent predeclared sampling units')
      units.add(`${treatment}:${unit}`)
      let measured: number | null = null
      for (const outcomeRef of allOutcomes) {
        const [outcomeRow, outcome] = record(outcomeRef, 'OutcomeRecord')
        if (outcome.attempt_ref !== ref) continue
        if (outcomeRow.kind !== 'Evidence') fail('invalid outcome')
        const e = (outcomeRow as Extract<Element, { kind: 'Evidence' }>).row
        if (
          e.status === 'corrected' ||
          e.corrected_by.length ||
          e.observed_at > String(evaluation.cutoff) ||
          outcome.terminal !== true ||
          outcome.metric !== comparisonPolicy.metric ||
          outcome.window !== trial.observation_window
        )
          fail(
            'selected outcome is corrected, intermediate or outside cutoff/metric/window',
          )
        const principal = origin(outcomeRow)
        if (
          !principal ||
          ((!policy.allow_same_principal_observer ||
            !current!.allow_same_principal_observer) &&
            principal === origin(attemptRow)) ||
          !policy.observers.some(
            (o) =>
              o.principal_id === principal &&
              o.configuration_digest === outcome.observer_config_digest,
          )
        )
          fail(
            'outcome instrument lacks protected observer-control eligibility',
          )
        if (measured !== null)
          fail('multiple terminal aggregates require an adjudication rule')
        measured =
          outcome.outcome_status === 'success'
            ? 1
            : outcome.outcome_status === 'partial'
              ? Number(outcome.magnitude ?? 0)
              : 0
        replayOutcomes[outcomeRef] = {
          record: outcome,
          status: e.status,
          corrected_by: e.corrected_by,
          principal_id: principal,
          observed_at: e.observed_at,
        }
      }
      if (treatment && measured === null && !missing.includes(ref))
        fail('missing attempt must be explicitly accounted for')
      if (measured !== null && missing.includes(ref))
        fail('observed attempt cannot also be missing')
      const stratum = String(obj(attempt.context).stratum ?? 'all')
      if (!(stratum in obj(comparisonPolicy.strata_weights)))
        fail('attempt stratum was not predeclared')
      const into = treatment ? samples.treatment : samples.baseline
      ;(into[stratum] ??= []).push(measured ?? 0)
      replayAttempts[ref] = {
        record: attempt,
        principal_id: origin(attemptRow),
      }
    }
    if (Object.keys(replayOutcomes).length !== allOutcomes.length)
      fail('evaluation includes unlinked outcomes')
    const excluded = Array.isArray(evaluation.excluded_samples)
      ? evaluation.excluded_samples.map(obj)
      : []
    for (const a of all('Activity')) {
      const r = facet(a, 'AttemptRecord')
      if (
        r?.trial_ref === trialRef &&
        String(r.started_at) <= String(evaluation.cutoff) &&
        !selected.includes(idOf(a)) &&
        r.preconditions_satisfied === 'yes'
      )
        fail('eligible assigned attempts cannot be excluded by author reason')
      if (
        r?.trial_ref === trialRef &&
        String(r.started_at) <= String(evaluation.cutoff) &&
        !selected.includes(idOf(a)) &&
        !excluded.some(
          (e) =>
            e.ref === idOf(a) &&
            typeof e.reason === 'string' &&
            e.reason.length,
        )
      )
        fail('trial attempt omitted without exclusion accounting')
    }
    for (const e of all('Evidence')) {
      const r = facet(e, 'OutcomeRecord')
      if (
        r?.terminal === true &&
        selected.includes(String(r.attempt_ref)) &&
        r.metric === comparisonPolicy.metric &&
        r.window === trial.observation_window &&
        !outcomes.includes(idOf(e)) &&
        e.kind === 'Evidence' &&
        e.row.status !== 'corrected' &&
        !e.row.corrected_by.length &&
        e.row.observed_at <= String(evaluation.cutoff)
      )
        fail(
          'conflicting terminal observations need supported adjudication rule',
        )
      if (
        r?.terminal === true &&
        selected.includes(String(r.attempt_ref)) &&
        r.metric === comparisonPolicy.metric &&
        r.window === trial.observation_window &&
        !outcomes.includes(idOf(e)) &&
        !excluded.some((x) => x.ref === idOf(e))
      )
        fail('terminal observation omitted without adjudication accounting')
    }
    const rule = obj(artifactValue(store, space, obj(trial.rule))),
      parameters = obj(artifactValue(store, space, obj(trial.parameters)))
    if (
      !same(replay.rule, rule) ||
      !same(replay.parameters, parameters) ||
      !same(replay.trial_record, trial) ||
      !same(replay.attempts, replayAttempts) ||
      !same(replay.outcomes, replayOutcomes)
    )
      fail(
        'evaluation replay must retain exact rule, parameters, trial and material inputs',
      )
    const quota = Math.max(
      Number(trial.quota),
      policy.minimum_independent_attempts,
      current!.minimum_independent_attempts,
    )
    const comparison = store.evaluationRules.evaluate({
      rule,
      parameters,
      trial,
      attempts: replayAttempts,
      outcomes: replayOutcomes,
      samples,
      minimum_independent_attempts: quota,
    })
    if (selected.length < quota && comparison.status !== 'insufficient')
      fail('rule must report insufficient below independent-attempt quota')
    if (!same(comparison, evaluation.comparison))
      fail('evaluation comparison disagrees with deterministic replay')
    if (
      from === 'trialed' &&
      to === 'adopted' &&
      (comparison.status !== 'improved' ||
        typeof comparison.effect !== 'number' ||
        comparison.effect < Number(comparisonPolicy.minimum_effect))
    )
      fail('adoption requires comparable independent improvement')
    if (
      from === 'adopted' &&
      to === 'adopted' &&
      comparison.status === 'insufficient' &&
      (!policy.retain_adoption_on_insufficient ||
        !current!.retain_adoption_on_insufficient)
    )
      fail('policy does not retain adoption on insufficient monitoring')
    if (
      from === 'revoked' &&
      to === 'trialed' &&
      !tx.staged.get(trialRef)?.isNew
    ) {
      const last = Math.max(
        0,
        ...all('Activity')
          .filter(
            (a) =>
              !a.row.origin.import &&
              facet(a, 'EvaluationRecord')?.to_status === 'revoked' &&
              sameRefs(
                facet(a, 'EvaluationRecord')!.revision_refs,
                evaluation.revision_refs,
              ),
          )
          .map((a) => a.row.seq),
      )
      if (trialRow.row.seq <= last)
        fail('re-entry requires trial opened after revocation')
    }
  }
  for (const [id, s] of pending) {
    const row = s.element
    if (type(row, 'SkillRevision')) {
      const families = edge(row, 'revision_of')
      if (families.length !== 1 || !type(final(families[0]!), 'Skill'))
        fail('SkillRevision requires one revision_of Skill')
      const old = baseElement(s)
      if (old && !same(edge(old, 'revision_of'), families))
        throw errors.immutableField('revision cannot change family')
    }
    if (type(row, 'Skill')) {
      if (
        created.some(
          (a) =>
            facet(a, 'EvaluationRecord') &&
            a.kind === 'Activity' &&
            refs(a.row.outputs as Json).includes(id),
        )
      )
        guarded(id, s)

      const revisions = edge(row, 'current_revision')
      if (revisions.length !== 1) fail('Skill requires one current_revision')
      const rev = revision(revisions[0]!)
      if (!same(edge(rev, 'revision_of'), [id]))
        fail('current_revision and revision_of must be bidirectional')
      const before = baseElement(s),
        status = obj(render(row).attributes).status
      const trialPtr = edge(row, 'current_trial'),
        evaluationPtr = edge(row, 'current_evaluation')
      if (!before || !same(edge(before, 'current_revision'), revisions)) {
        guarded(id, s)
        if (status !== 'proposed' || trialPtr.length > 0 || evaluationPtr.length > 0)
          fail(
            'selecting new behavior resets standing to proposed and clears current_trial and current_evaluation',
          )
      } else if (
        obj(render(before).attributes).status !== status ||
        !same(edge(before, 'current_trial'), trialPtr) ||
        !same(edge(before, 'current_evaluation'), evaluationPtr)
      ) {
        guarded(id, s)
        const evaluation = created.find((a) => {
          const r = facet(a, 'EvaluationRecord')
          return (
            r &&
            r.from_status === obj(render(before).attributes).status &&
            r.to_status === status &&
            refs(r.revision_refs).includes(revisions[0]!) &&
            a.kind === 'Activity' &&
            refs(a.row.outputs as Json).includes(id)
          )
        })
        if (!evaluation)
          fail(
            'lifecycle and its pointers change only with a new validated EvaluationRecord in the same transaction',
          )
        // The pointers select immutable records; they are never a second
        // copy that could disagree with them (GradingState is computed).
        if (evaluationPtr.length > 0 && !same(evaluationPtr, [idOf(evaluation!)]))
          fail('current_evaluation must point to the verdict of this transaction')
        if (
          trialPtr.length > 0 &&
          (facet(evaluation!, 'EvaluationRecord')!.trial_ref !== trialPtr[0] ||
            !refs(record(trialPtr[0]!, 'TrialRecord')[1].revision_refs).includes(revisions[0]!))
        )
          fail('current_trial must select the evaluation\'s trial of the current revision')
      }
    }
    const decision = facet(row, 'DecisionRecord')
    if (decision) {
      const contract = facet(row, 'DependencyBasis')
      if (
        contract &&
        (!same(contract.policy_basis, decision.basis) ||
          contract.basis_seq !== obj(decision.basis).snapshot_seq)
      )
        fail('DecisionRecord and DependencyBasis must describe same read basis')
      for (const ref of [
        ...refs(decision.retrieved_refs),
        ...refs(decision.used_refs),
      ])
        final(ref)
      for (const ref of refs(decision.applied_revisions)) {
        revision(ref)
        if (
          row.kind !== 'Activity' ||
          !refs(row.row.inputs as Json).includes(ref)
        )
          fail('applied revisions must occur in decision inputs')
      }
    }
    const attempt = facet(row, 'AttemptRecord')
    if (attempt) {
      const [decisionRow, d] = record(
        String(attempt.decision_ref),
        'DecisionRecord',
      )
      if (
        !sameRefs(attempt.applied_revisions, d.applied_revisions) ||
        d.decision !== 'act'
      )
        fail('attempt must bind acting decision exact revision bundle')
      const started = String(attempt.started_at)
      if (
        started > tx.cx.at ||
        (!tx.staged.get(idOf(decisionRow))?.isNew &&
          started < decisionRow.row.created_at)
      )
        fail('attempt must follow decision and cannot be future-dated')
      artifactValue(store, space, obj(attempt.selection_policy))
      if (typeof attempt.trial_ref === 'string') {
        const [trialRow, trial] = record(attempt.trial_ref, 'TrialRecord')
        if (
          !sameRefs(attempt.applied_revisions, trial.revision_refs) ||
          (trialRow.row.seq >= decisionRow.row.seq &&
            !tx.staged.get(idOf(decisionRow))?.isNew) ||
          tx.staged.get(idOf(trialRow))?.isNew
        )
          fail('trial must precede decision; no retrospective enrollment')
        if (
          attempt.environment_digest !==
          obj(trial.comparability).environment_digest
        )
          fail('attempt environment outside trial')
      }
    }
    const outcome = facet(row, 'OutcomeRecord')
    if (outcome && typeof outcome.attempt_ref === 'string') {
      const [attemptRow, a] = record(outcome.attempt_ref, 'AttemptRecord')
      if (tx.staged.get(idOf(attemptRow))?.isNew)
        fail('attempt must be committed before observing outcome')
      if (row.kind !== 'Evidence') fail('OutcomeRecord needs Evidence')
      const e = (row as Extract<Element, { kind: 'Evidence' }>).row
      if (e.observed_at < String(a.started_at))
        fail('observation cannot predate its attempt')
      const observer = e.generated_by
        ? final(e.generated_by)
        : producers(id).find(
            (a) =>
              a.kind === 'Activity' &&
              a.row.activity_class === 'outcome_observation' &&
              a.row.status === 'completed' &&
              refs(a.row.outputs as Json).includes(id),
          )
      if (
        !observer ||
        observer.kind !== 'Activity' ||
        observer.row.activity_class !== 'outcome_observation' ||
        observer.row.status !== 'completed' ||
        !refs(observer.row.inputs as Json).includes(outcome.attempt_ref) ||
        !refs(observer.row.inputs as Json).includes(String(a.decision_ref))
      )
        fail('outcome observation must link attempt and decision')
      if (
        !(s.isNew && tx.staged.get(idOf(observer!))?.isNew) &&
        observer!.row.created_tx !== row.row.created_tx
      )
        fail('observation cannot be retrospectively attached to outcome')
    }
    const trial = facet(row, 'TrialRecord')
    if (
      trial &&
      (!s.baseRow || !facet(store.load(parseElementId(id))!, 'TrialRecord'))
    ) {
      for (const ref of refs(trial.revision_refs)) revision(ref)
      validateTrial(trial, row)
    }
    const evaluation = facet(row, 'EvaluationRecord')
    if (
      evaluation &&
      (!s.baseRow ||
        !facet(store.load(parseElementId(id))!, 'EvaluationRecord'))
    )
      validateEvaluation(evaluation, row)
  }
}
