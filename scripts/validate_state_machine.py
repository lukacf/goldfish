#!/usr/bin/env python3
"""Formal validation of the Stage Execution State Machine.

Validates the state machine specification for:
1. Reachability - All states reachable from initial state
2. Deadlock freedom - All non-terminal states have outgoing transitions
3. Guard completeness - Guards cover all cases (no undefined behavior)
4. Determinism - Exactly one transition per (state, event, context)
5. Terminal correctness - Terminal states have no outgoing transitions
6. Event coverage - All events handled in appropriate states

Run: python scripts/validate_state_machine.py
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto

# =============================================================================
# State Machine Definition (extracted from spec)
# =============================================================================


class State(Enum):
    """All possible states."""

    PREPARING = auto()
    BUILDING = auto()
    LAUNCHING = auto()
    RUNNING = auto()
    FINALIZING = auto()
    # Terminal states
    COMPLETED = auto()
    FAILED = auto()
    CANCELED = auto()
    TERMINATED = auto()
    UNKNOWN = auto()


class Event(Enum):
    """All possible events."""

    BUILD_START = auto()
    BUILD_OK = auto()
    BUILD_FAIL = auto()
    LAUNCH_OK = auto()
    LAUNCH_FAIL = auto()
    EXIT_SUCCESS = auto()
    EXIT_FAILURE = auto()
    EXIT_MISSING = auto()
    FINALIZE_OK = auto()
    FINALIZE_FAIL = auto()
    INSTANCE_LOST = auto()
    TIMEOUT = auto()
    USER_CANCEL = auto()
    PREPARE_FAIL = auto()
    SVS_BLOCK = auto()


# Terminal states have no outgoing transitions (except UNKNOWN which has admin escapes)
TERMINAL_STATES = {State.COMPLETED, State.FAILED, State.CANCELED, State.TERMINATED}

# UNKNOWN is special - it's a "limbo" state that requires resolution
# It's only reachable via migration, not normal operation
LIMBO_STATES = {State.UNKNOWN}

# States that are intentionally only reachable via migration/admin action
# These are excluded from normal reachability checks
MIGRATION_ONLY_STATES = {State.UNKNOWN}

# Active states that should always have paths to terminal states
ACTIVE_STATES = {State.PREPARING, State.BUILDING, State.LAUNCHING, State.RUNNING, State.FINALIZING}

# Initial state
INITIAL_STATE = State.PREPARING


@dataclass
class Guard:
    """Represents a guard condition."""

    name: str
    condition: Callable[[dict], bool]
    description: str = ""


@dataclass
class Transition:
    """A state transition definition."""

    from_state: State
    event: Event
    to_state: State
    guard: Guard | None = None


# Define guards
GUARD_INSTANCE_DEAD = Guard(
    "instance_confirmed_dead",
    lambda ctx: ctx.get("instance_confirmed_dead") is True,
    "Instance verified as stopped/deleted",
)

GUARD_CRITICAL_TRUE = Guard(
    "critical=True", lambda ctx: ctx.get("critical") is True, "Finalization failure is critical"
)

GUARD_CRITICAL_FALSE = Guard(
    "critical=False", lambda ctx: ctx.get("critical") is False, "Finalization failure is non-critical"
)

GUARD_PHASES_DONE = Guard(
    "critical_phases_done=True",
    lambda ctx: ctx.get("critical_phases_done") is True,
    "Critical phases (output sync, recording) completed",
)

GUARD_PHASES_NOT_DONE = Guard(
    "critical_phases_done=False", lambda ctx: ctx.get("critical_phases_done") is False, "Critical phases not completed"
)


# =============================================================================
# Transition Table (from spec v3.4)
# =============================================================================

TRANSITIONS: list[Transition] = [
    # PREPARING
    Transition(State.PREPARING, Event.BUILD_START, State.BUILDING),
    Transition(State.PREPARING, Event.PREPARE_FAIL, State.FAILED),
    Transition(State.PREPARING, Event.SVS_BLOCK, State.FAILED),
    Transition(State.PREPARING, Event.INSTANCE_LOST, State.TERMINATED),
    Transition(State.PREPARING, Event.TIMEOUT, State.TERMINATED),
    Transition(State.PREPARING, Event.USER_CANCEL, State.CANCELED),
    # BUILDING
    Transition(State.BUILDING, Event.BUILD_OK, State.LAUNCHING),
    Transition(State.BUILDING, Event.BUILD_FAIL, State.FAILED),
    Transition(State.BUILDING, Event.INSTANCE_LOST, State.TERMINATED),
    Transition(State.BUILDING, Event.TIMEOUT, State.TERMINATED),
    Transition(State.BUILDING, Event.USER_CANCEL, State.CANCELED),
    # LAUNCHING
    Transition(State.LAUNCHING, Event.LAUNCH_OK, State.RUNNING),
    Transition(State.LAUNCHING, Event.LAUNCH_FAIL, State.FAILED),
    Transition(State.LAUNCHING, Event.INSTANCE_LOST, State.TERMINATED),
    Transition(State.LAUNCHING, Event.TIMEOUT, State.TERMINATED),
    Transition(State.LAUNCHING, Event.USER_CANCEL, State.CANCELED),
    # RUNNING
    Transition(State.RUNNING, Event.EXIT_SUCCESS, State.FINALIZING),
    Transition(State.RUNNING, Event.EXIT_FAILURE, State.FAILED),
    Transition(State.RUNNING, Event.EXIT_MISSING, State.TERMINATED, GUARD_INSTANCE_DEAD),
    Transition(State.RUNNING, Event.INSTANCE_LOST, State.TERMINATED),
    Transition(State.RUNNING, Event.TIMEOUT, State.TERMINATED),
    Transition(State.RUNNING, Event.USER_CANCEL, State.CANCELED),
    # FINALIZING
    Transition(State.FINALIZING, Event.FINALIZE_OK, State.COMPLETED),
    Transition(State.FINALIZING, Event.FINALIZE_FAIL, State.FAILED, GUARD_CRITICAL_TRUE),
    Transition(State.FINALIZING, Event.FINALIZE_FAIL, State.COMPLETED, GUARD_CRITICAL_FALSE),
    Transition(State.FINALIZING, Event.INSTANCE_LOST, State.TERMINATED),
    Transition(State.FINALIZING, Event.TIMEOUT, State.COMPLETED, GUARD_PHASES_DONE),
    Transition(State.FINALIZING, Event.TIMEOUT, State.FAILED, GUARD_PHASES_NOT_DONE),
    Transition(State.FINALIZING, Event.USER_CANCEL, State.CANCELED),
    # UNKNOWN (limbo state - needs resolution)
    Transition(State.UNKNOWN, Event.TIMEOUT, State.TERMINATED),
]


# =============================================================================
# Validation Functions
# =============================================================================


def build_transition_graph() -> dict[State, list[Transition]]:
    """Build adjacency list representation of state machine."""
    graph: dict[State, list[Transition]] = defaultdict(list)
    for t in TRANSITIONS:
        graph[t.from_state].append(t)
    return graph


def validate_reachability() -> tuple[bool, list[str]]:
    """Check that all states are reachable from INITIAL_STATE.

    Uses BFS to find all reachable states.
    Migration-only states (like UNKNOWN) are expected to be unreachable
    via normal transitions and are reported as INFO, not errors.
    """
    errors = []
    graph = build_transition_graph()

    # BFS from initial state
    visited = {INITIAL_STATE}
    queue = [INITIAL_STATE]

    while queue:
        current = queue.pop(0)
        for transition in graph.get(current, []):
            if transition.to_state not in visited:
                visited.add(transition.to_state)
                queue.append(transition.to_state)

    # Check all states are reachable
    all_states = set(State)
    unreachable = all_states - visited

    # Separate migration-only states from truly unreachable states
    expected_unreachable = unreachable & MIGRATION_ONLY_STATES
    unexpected_unreachable = unreachable - MIGRATION_ONLY_STATES

    if expected_unreachable:
        print("  INFO: Migration-only states (intentionally unreachable via normal transitions):")
        for s in expected_unreachable:
            print(f"    - {s.name}")

    if unexpected_unreachable:
        errors.append(f"Unexpectedly unreachable states: {[s.name for s in unexpected_unreachable]}")

    return len(errors) == 0, errors


def validate_deadlock_freedom() -> tuple[bool, list[str]]:
    """Check that all non-terminal states have outgoing transitions."""
    errors = []
    graph = build_transition_graph()

    for state in ACTIVE_STATES:
        if state not in graph or len(graph[state]) == 0:
            errors.append(f"Deadlock: {state.name} has no outgoing transitions")

    return len(errors) == 0, errors


def validate_terminal_states() -> tuple[bool, list[str]]:
    """Check that terminal states have no outgoing transitions (except admin overrides)."""
    errors = []
    graph = build_transition_graph()

    for state in TERMINAL_STATES:
        transitions = graph.get(state, [])
        if transitions:
            # Terminal states should have NO transitions
            events = [t.event.name for t in transitions]
            errors.append(f"Terminal state {state.name} has outgoing transitions: {events}")

    return len(errors) == 0, errors


def validate_guard_completeness() -> tuple[bool, list[str]]:
    """Check that guards cover all cases for events with multiple transitions.

    For an event E in state S with guards G1, G2, ..., the guards should be:
    1. Mutually exclusive (at most one can be true)
    2. Exhaustive (at least one must be true for valid contexts)
    """
    errors = []
    warnings = []

    # Group transitions by (from_state, event)
    grouped: dict[tuple[State, Event], list[Transition]] = defaultdict(list)
    for t in TRANSITIONS:
        grouped[(t.from_state, t.event)].append(t)

    for (state, event), transitions in grouped.items():
        if len(transitions) == 1:
            # Single transition - guard is optional
            continue

        # Multiple transitions - need guard analysis
        guards = [t.guard for t in transitions]

        # Check: all transitions must have guards (except one fallback)
        unguarded = [t for t in transitions if t.guard is None]
        if len(unguarded) > 1:
            errors.append(
                f"Multiple unguarded transitions for ({state.name}, {event.name}): "
                f"destinations = {[t.to_state.name for t in unguarded]}"
            )
        elif len(unguarded) == 0:
            # All have guards - check for completeness
            # This is a heuristic check for common patterns
            guard_names = {g.name for g in guards if g}

            # Check for complementary guards
            if "critical=True" in guard_names and "critical=False" not in guard_names:
                warnings.append(f"({state.name}, {event.name}): has critical=True guard but no critical=False")
            if "critical=False" in guard_names and "critical=True" not in guard_names:
                warnings.append(f"({state.name}, {event.name}): has critical=False guard but no critical=True")
            if "critical_phases_done=True" in guard_names and "critical_phases_done=False" not in guard_names:
                warnings.append(f"({state.name}, {event.name}): has phases_done=True guard but no phases_done=False")

    # Print warnings
    for w in warnings:
        print(f"  WARNING: {w}")

    return len(errors) == 0, errors


def validate_determinism() -> tuple[bool, list[str]]:
    """Check that transitions are deterministic (no ambiguity).

    For each (state, event) pair, either:
    1. There's exactly one unguarded transition, OR
    2. All transitions have mutually exclusive guards
    """
    errors = []

    # Group transitions by (from_state, event)
    grouped: dict[tuple[State, Event], list[Transition]] = defaultdict(list)
    for t in TRANSITIONS:
        grouped[(t.from_state, t.event)].append(t)

    for (state, event), transitions in grouped.items():
        if len(transitions) == 1:
            continue

        # Check for duplicate unguarded transitions
        unguarded = [t for t in transitions if t.guard is None]
        if len(unguarded) > 1:
            errors.append(f"Non-deterministic: ({state.name}, {event.name}) has {len(unguarded)} unguarded transitions")

        # Check for duplicate guards
        guarded = [t for t in transitions if t.guard is not None]
        guard_names = [t.guard.name for t in guarded]
        if len(guard_names) != len(set(guard_names)):
            from collections import Counter

            duplicates = [name for name, count in Counter(guard_names).items() if count > 1]
            errors.append(f"Non-deterministic: ({state.name}, {event.name}) has duplicate guards: {duplicates}")

    return len(errors) == 0, errors


def validate_event_coverage() -> tuple[bool, list[str]]:
    """Check which events are handled in which states.

    This is informational - not all events need to be handled in all states.
    """
    warnings = []

    # Events that should be handled in all active states
    universal_events = {Event.USER_CANCEL, Event.TIMEOUT}

    # Group transitions by state
    by_state: dict[State, set[Event]] = defaultdict(set)
    for t in TRANSITIONS:
        by_state[t.from_state].add(t.event)

    for state in ACTIVE_STATES:
        handled = by_state.get(state, set())
        missing_universal = universal_events - handled
        if missing_universal:
            warnings.append(f"{state.name} missing universal events: {[e.name for e in missing_universal]}")

    for w in warnings:
        print(f"  INFO: {w}")

    return True, []  # Warnings only, not errors


def validate_path_to_terminal() -> tuple[bool, list[str]]:
    """Check that all active states can reach terminal states.

    Uses BFS to verify terminal state reachability.
    """
    errors = []
    graph = build_transition_graph()

    def can_reach_terminal(start: State) -> bool:
        """BFS to check if terminal state is reachable."""
        visited = {start}
        queue = [start]

        while queue:
            current = queue.pop(0)
            if current in TERMINAL_STATES:
                return True

            for transition in graph.get(current, []):
                if transition.to_state not in visited:
                    visited.add(transition.to_state)
                    queue.append(transition.to_state)

        return False

    for state in ACTIVE_STATES:
        if not can_reach_terminal(state):
            errors.append(f"{state.name} cannot reach any terminal state")

    # Also check UNKNOWN can reach terminal
    if not can_reach_terminal(State.UNKNOWN):
        errors.append("UNKNOWN cannot reach any terminal state")

    return len(errors) == 0, errors


def validate_no_self_loops() -> tuple[bool, list[str]]:
    """Check for self-loop transitions (state -> same state).

    Self-loops are usually bugs unless explicitly intended.
    """
    errors = []

    for t in TRANSITIONS:
        if t.from_state == t.to_state:
            errors.append(f"Self-loop: {t.from_state.name} --{t.event.name}--> {t.to_state.name}")

    return len(errors) == 0, errors


def generate_transition_matrix() -> str:
    """Generate a visual transition matrix."""
    lines = []
    lines.append("\n=== TRANSITION MATRIX ===")
    lines.append("(States × Events → Target State)")
    lines.append("")

    # Group by state
    by_state: dict[State, dict[Event, list[str]]] = defaultdict(lambda: defaultdict(list))
    for t in TRANSITIONS:
        target = t.to_state.name
        if t.guard:
            target += f" [{t.guard.name}]"
        by_state[t.from_state][t.event].append(target)

    for state in list(ACTIVE_STATES) + [State.UNKNOWN]:
        lines.append(f"\n{state.name}:")
        events = by_state.get(state, {})
        if not events:
            lines.append("  (no transitions)")
        else:
            for event in sorted(events.keys(), key=lambda e: e.name):
                targets = events[event]
                lines.append(f"  {event.name:20} → {', '.join(targets)}")

    return "\n".join(lines)


def validate_guard_mutual_exclusivity() -> tuple[bool, list[str]]:
    """Check that guards for the same (state, event) are mutually exclusive.

    For boolean guards like critical=True/False, they should be complements.
    """
    errors = []

    # Group transitions by (from_state, event)
    grouped: dict[tuple[State, Event], list[Transition]] = defaultdict(list)
    for t in TRANSITIONS:
        grouped[(t.from_state, t.event)].append(t)

    # Known complementary guard pairs
    COMPLEMENTARY_PAIRS = {
        frozenset({"critical=True", "critical=False"}),
        frozenset({"critical_phases_done=True", "critical_phases_done=False"}),
    }

    for (state, event), transitions in grouped.items():
        if len(transitions) <= 1:
            continue

        guarded = [t for t in transitions if t.guard is not None]
        if len(guarded) < 2:
            continue

        # Check that all guards form complementary pairs
        guard_names = {t.guard.name for t in guarded}

        # Find which complementary pair this matches
        matched_pair = None
        for pair in COMPLEMENTARY_PAIRS:
            if guard_names == pair:
                matched_pair = pair
                break

        if matched_pair is None and len(guard_names) > 1:
            # Check if it's a subset of a complementary pair (partial coverage)
            for pair in COMPLEMENTARY_PAIRS:
                if guard_names < pair:  # Proper subset
                    missing = pair - guard_names
                    errors.append(
                        f"({state.name}, {event.name}): Incomplete guard coverage. "
                        f"Has {guard_names}, missing {missing}"
                    )
                    break

    return len(errors) == 0, errors


def validate_happy_path() -> tuple[bool, list[str]]:
    """Verify the happy path exists: PREPARING → BUILDING → LAUNCHING → RUNNING → FINALIZING → COMPLETED."""
    errors = []
    graph = build_transition_graph()

    happy_path = [
        (State.PREPARING, Event.BUILD_START, State.BUILDING),
        (State.BUILDING, Event.BUILD_OK, State.LAUNCHING),
        (State.LAUNCHING, Event.LAUNCH_OK, State.RUNNING),
        (State.RUNNING, Event.EXIT_SUCCESS, State.FINALIZING),
        (State.FINALIZING, Event.FINALIZE_OK, State.COMPLETED),
    ]

    for from_state, event, to_state in happy_path:
        transitions = graph.get(from_state, [])
        matching = [t for t in transitions if t.event == event and t.to_state == to_state]

        if not matching:
            errors.append(f"Happy path broken: {from_state.name} --{event.name}--> {to_state.name} not found")
        elif matching[0].guard is not None:
            errors.append(
                f"Happy path guarded: {from_state.name} --{event.name}--> {to_state.name} "
                f"requires guard [{matching[0].guard.name}]"
            )

    if not errors:
        print("  Happy path: PREPARING → BUILDING → LAUNCHING → RUNNING → FINALIZING → COMPLETED")

    return len(errors) == 0, errors


def validate_failure_paths() -> tuple[bool, list[str]]:
    """Verify that all active states can fail gracefully."""
    errors = []
    graph = build_transition_graph()

    # Each active state should have at least one path to FAILED, CANCELED, or TERMINATED
    failure_states = {State.FAILED, State.CANCELED, State.TERMINATED}

    for state in ACTIVE_STATES:
        transitions = graph.get(state, [])
        failure_transitions = [t for t in transitions if t.to_state in failure_states]

        if not failure_transitions:
            errors.append(f"{state.name} has no direct path to failure states")
        else:
            # Check for common failure events
            events = {t.event for t in failure_transitions}
            if Event.USER_CANCEL not in events:
                print(f"  WARNING: {state.name} doesn't handle USER_CANCEL")
            if Event.TIMEOUT not in events:
                print(f"  WARNING: {state.name} doesn't handle TIMEOUT")

    return len(errors) == 0, errors


def count_statistics() -> str:
    """Generate statistics about the state machine."""
    lines = []
    lines.append("\n=== STATISTICS ===")
    lines.append(f"Total states: {len(State)}")
    lines.append(f"  Active states: {len(ACTIVE_STATES)}")
    lines.append(f"  Terminal states: {len(TERMINAL_STATES)}")
    lines.append(f"  Limbo states: {len(LIMBO_STATES)}")
    lines.append(f"Total events: {len(Event)}")
    lines.append(f"Total transitions: {len(TRANSITIONS)}")

    # Transitions per state
    by_state = defaultdict(int)
    for t in TRANSITIONS:
        by_state[t.from_state] += 1

    lines.append("\nTransitions per state:")
    for state in sorted(by_state.keys(), key=lambda s: s.name):
        lines.append(f"  {state.name}: {by_state[state]}")

    # Guarded transitions
    guarded = sum(1 for t in TRANSITIONS if t.guard)
    lines.append(f"\nGuarded transitions: {guarded}")
    lines.append(f"Unguarded transitions: {len(TRANSITIONS) - guarded}")

    return "\n".join(lines)


# =============================================================================
# Main
# =============================================================================


def main():
    print("=" * 60)
    print("STATE MACHINE VALIDATION")
    print("=" * 60)

    validators = [
        ("Reachability", validate_reachability),
        ("Deadlock Freedom", validate_deadlock_freedom),
        ("Terminal States", validate_terminal_states),
        ("Guard Completeness", validate_guard_completeness),
        ("Guard Mutual Exclusivity", validate_guard_mutual_exclusivity),
        ("Determinism", validate_determinism),
        ("Path to Terminal", validate_path_to_terminal),
        ("No Self-Loops", validate_no_self_loops),
        ("Happy Path", validate_happy_path),
        ("Failure Paths", validate_failure_paths),
        ("Event Coverage", validate_event_coverage),
    ]

    all_passed = True
    results = []

    for name, validator in validators:
        print(f"\n[{name}]")
        passed, errors = validator()

        if passed:
            print("  ✓ PASSED")
        else:
            print("  ✗ FAILED")
            for error in errors:
                print(f"    - {error}")
            all_passed = False

        results.append((name, passed, errors))

    # Print statistics
    print(count_statistics())

    # Print transition matrix
    print(generate_transition_matrix())

    # Summary
    print("\n" + "=" * 60)
    if all_passed:
        print("✓ ALL VALIDATIONS PASSED")
    else:
        print("✗ SOME VALIDATIONS FAILED")
        failed = [name for name, passed, _ in results if not passed]
        print(f"  Failed: {', '.join(failed)}")
    print("=" * 60)

    return 0 if all_passed else 1


if __name__ == "__main__":
    exit(main())
