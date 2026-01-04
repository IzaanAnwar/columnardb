# Contributing to Columnar

Thanks for your interest in contributing.

This project is intentionally small in scope. Contributions are welcome,
but only if they respect the design constraints described below.

---

## Before You Contribute

Please read the README and understand what this project **does not** aim to be.

If your change moves the project toward:
- SQL features
- transactional semantics
- in-place updates
- distributed systems

it will likely be rejected.

This is not a value judgment. It is a scope decision.

---

## Design Constraints (Non-Negotiable)

- Append-only storage
- Immutable segments
- Column-oriented layout
- Explicit schemas
- No hidden background processes
- No automatic “magic” optimizations

Any contribution that violates these constraints will not be accepted.

---

## What Contributions Are Welcome

Good contributions include:

- Bug fixes
- Clear performance improvements
- Better encoding strategies
- Improved documentation
- Benchmarks and profiling results
- Validation and consistency checks
- Debugging and inspection tools

Especially valuable:
- Changes that make the system easier to reason about
- Changes that make performance behavior more explicit

---

## What Contributions Are Not Welcome

Please do not submit PRs that:

- Add joins or relational features
- Introduce in-place mutation of data
- Add background compaction without explicit control
- Hide IO or memory costs behind abstractions
- Add dependencies without strong justification

---

## Development Guidelines

- Prefer clarity over abstraction
- Avoid premature generalization
- Keep files small and focused
- Write code that can be read without context
- If something is complex, document *why*, not *what*

---

## Testing

All changes should include:
- Unit tests where applicable
- Benchmarks if performance-related

If a change affects on-disk layout:
- Call it out explicitly
- Explain backward compatibility implications

---

## Commit Messages

Write clear, descriptive commit messages.

Good:
```

segment: fix null bitmap offset calculation

```

Bad:
```

fix bug

```

---

## Discussion First

For non-trivial changes, open an issue first.

This avoids wasted effort and keeps the project coherent.

---

## Code of Conduct

Be respectful and constructive.
Technical disagreement is expected.
Personal attacks are not acceptable.

---

## Final Note

This project values:
- correctness
- restraint
- engineering discipline

If that excites you, you’re welcome here.

