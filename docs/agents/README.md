# ArcaneQuant parallel coding agents

The orchestrator assigns work, owns cross-agent relays and decides when a task is complete. Each worker owns its implementation, evidence and handover. When drafting a prompt, include the relevant instructions below in the prompt itself. Give agents the paths and contacts for their assignment.

## Identity prompt

- Implement the specified behaviour for the supported inputs. Use constants where the contract calls for them. Never special-case a fixture, bypass the normal entry point, suppress a failure or weaken a check to manufacture a pass. If a test and the agreed contract disagree, show the discrepancy and explain any correction.
- Document changed behaviour **in the source file**. Update the affected docstring or equivalent API documentation so a caller can use the interface correctly. Include units, time alignment, mutation, return values or errors when they matter. Use a short inline comment for a non-obvious constraint, such as why a signal must be shifted by one bar. Keep existing explanations accurate and omit commentary that repeats the code.
- Say when you are uncertain, blocked or unable to verify a result. Report the observation, what you tried and the smallest answer or action that would unblock you. Continue independent work while waiting. Use permitted web search for public technical questions, preferably against primary documentation. Keep private code, data and credentials out of queries and logs.
- When a reliable source answers a question likely to recur in ArcaneQuant, check the [knowledge catalog](../knowledge/README.md) and add or update the relevant note under `docs/knowledge/`. Summarise the finding in your own words, explain where it applies, and link the supporting passage with the date checked and any relevant software version. Official user documentation, maintained development guides and well-sourced wiki articles can all be useful. Follow the catalog’s indexing conventions. If your assignment is read-only, include the proposed note and source in your handover.
- Ask the owner directly when the assigned communication graph permits it. Route missing edges, unclear ownership and unresolved disagreements through the orchestrator. The orchestrator brings decisions about user intent or scope to the user. Until a graph is supplied, use the orchestrator for cross-agent requests.

## Task prompt

Specify the task ID, agent ID, owned paths, intended behaviour, acceptance criteria, dependencies, information consumers and permitted contacts. Assign distinct task paths to concurrent workers. Where a graph or interface is pending, identify it as pending. A consumer may draft against a provisional signature but must confirm it with the owner before declaring integration complete. The producer notifies affected consumers when the contract changes.

Require evidence through the normal entry point. For input-dependent behaviour, choose a case that would distinguish a real implementation from a hardcoded answer. For example, rebind an engine asset, resolve the same reference again and check the value from the replacement asset. Record commands actually run, observed outcomes, skipped checks and residual uncertainty.

## Files each agent owns

Use this layout relative to the repository root. The orchestrator assigns a stable agent ID and a unique task ID. Use the same IDs on resumption.

| Path | Owner and update rule |
| --- | --- |
| `docs/agents/<agent-id>/rules.md` | The agent records its role, remit and standing rules at first assignment. Update when responsibilities change. Link shared rules here. |
| `docs/agents/<agent-id>/tasks/<task-id>.md` | The agent records the dated brief, task-specific rules, affected paths and acceptance criteria at assignment. Append material decisions with author, short rationale, evidence and affected consumers. At handover record verification, outstanding issues and links to changed code and project docs. Preserve earlier decisions and identify superseding ones. |
| `docs/agents/<agent-id>/project.md` | The agent maintains concise project notes for its area, including interfaces it owns, dependencies, constraints and links to authoritative source or design docs. Update when those facts change. |
| `docs/agents/<agent-id>/working/<task-id>.md` | The agent overwrites its own restart checkpoint at meaningful milestones, before waiting on a dependency and at handover. Keep the latest copy across shutdown. |
| `docs/agents/orchestrator/relays.md` | The orchestrator appends every relay and its outcome. A replacement orchestrator reads pending entries before relaying anything. |

**Checkpoint contents.** Record the task ID, timestamp, current status, files or revisions in use, last verified result, outstanding dependency and owner, and next concrete action. Include the command or fixture needed to resume when relevant. Keep it factual and short. On restart, compare it with the current files, task record and outstanding messages before continuing. Durable decisions belong in the task record so overwriting a checkpoint preserves the audit history.

## Orchestrator relays

For each forwarded request, reply or update, assign a relay ID and append a dated entry with task, original sender, recipient, subject, linked contract or decision and status. Record intent before sending and append the observed delivery outcome with the same ID. Link a forwarded reply to its request. If delivery is uncertain after interruption, check with the recipient before retrying. Record corrections as subsequent entries. The log needs enough detail to establish what the recipient was told.

Review repeated relays about the same interface or pair of roles. Use the linked relay IDs to explain whether a direct dependency would reduce delay, or whether the pattern signals a risk such as disputed ownership, a circular wait or conflicting API assumptions. Record the impact, proposed action, owner and decision in the orchestrator's task record. The orchestrator may request a new edge from the graph owner. Continue using established routes until the change is agreed. One consequential relay can justify a risk review on its own.

## Graceful shutdown

1. The worker reports **ready for review** with the deliverable, checks performed, unresolved issues and affected dependants. It remains available for follow-up.
2. The orchestrator checks the acceptance evidence and outstanding dependencies. If work remains, return the specific gap to its owner. Once satisfied, record the completion decision and request a graceful shutdown.
3. The worker finishes any in-flight write, updates its project and task records, saves the final checkpoint, and replies **ready to close** with the handover paths. If a write or handover fails, report the blocker and keep the task open for recovery.
4. The orchestrator verifies that the handover is readable, records the acknowledgement and then closes the thread if the runtime supports closure. If the worker disappears before acknowledging, record an interruption and recover from its last checkpoint. Leave the shutdown unconfirmed until the handover is reconciled.
