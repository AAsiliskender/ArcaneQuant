# Guardrails, human review, and agent evaluation

Last verified 2026-11-24.

## Where do guardrails run in the Agents SDK?

An input guardrail checks the first agent's input. An output guardrail checks the reply from the agent that finishes the run. A handoff does not reapply those checks to each specialist. Attach a tool guardrail to a custom function when its arguments or result need validation on each call. A blocking input check finishes before the agent starts. Running the check in parallel can reduce latency, but the agent may begin work before the result arrives. [SDK guardrails and approvals](https://developers.openai.com/api/docs/guides/agents/guardrails-approvals)

## When should a person approve a tool call?

For a consequential action such as cancelling a booking, show the reviewer the proposed action and its target before execution. With an approval-enabled tool, the SDK returns the pending call and resumable state. Record the reviewer's decision and resume that run so the decision applies to the original request. [SDK approval lifecycle](https://developers.openai.com/api/docs/guides/agents/guardrails-approvals#approval-lifecycle)

## How should an agent handle instructions inside retrieved text?

Treat retrieved passages as evidence. A sentence in a policy document can establish the terms of a refund. It cannot authorize the agent to issue one. Keep outside text out of developer instructions, and pass only the fields needed by a downstream tool. A structured output schema can constrain the shape of those fields. The application still needs to verify their meaning and the caller's authority. [Agent safety guidance](https://developers.openai.com/api/docs/guides/agent-builder-safety) · [Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs)

## How can a trace explain a wrong answer?

Follow one run through its model calls, tool calls, handoffs, and guardrail results. For a policy question, inspect whether the agent requested a lookup, what passage it received, and how it used that passage in the final answer. Each observation points to a different fix. Turn a recurring failure into an evaluation case with an expected source and answer. [SDK tracing](https://developers.openai.com/api/docs/guides/agents/integrations-observability#tracing) · [Workflow evaluation](https://developers.openai.com/api/docs/guides/agent-evals)

## How do you evaluate retrieval quality and answer faithfulness separately?

Before a test run, record the expected evidence for each question. Include paraphrases and a question the collection cannot answer. Score two outcomes independently. [Evaluation best practices](https://developers.openai.com/api/docs/guides/evaluation-best-practices)

- **Retrieval quality.** Did the selected passages include the evidence needed to answer? Context recall measures coverage of the expected evidence. Context precision measures how much of what came back was relevant. [Document Q&A evaluation example](https://developers.openai.com/api/docs/guides/evaluation-best-practices#example-qa-over-docs)
- **Answer faithfulness.** Can a reviewer support each material claim with a passage the agent actually read? If a passage gives a return window but says nothing about return postage, an answer promising free postage fails even when retrieval found the right policy. An answer can cite the window and identify the missing postage information. [Retrieval and grounded synthesis](https://developers.openai.com/api/docs/guides/retrieval)

## Which tradeoffs matter when changing a production agent?

Compare alternatives on the same representative questions. Record task success, elapsed time, and cost. For example, limiting File Search results can shorten the context and reduce latency, but can also omit the evidence needed for an answer. Recheck coverage and answer quality before adopting that limit. [File Search](https://developers.openai.com/api/docs/guides/tools-file-search) · [Production track](https://developers.openai.com/tracks/ai-application-development)

For teams maintaining older evals, OpenAI listed its hosted Evals dashboard, API, and eval graders as read-only on the verification date, with shutdown scheduled for 2026-11-30. Its migration guide covers moving cases and checks to Promptfoo. [Deprecations](https://developers.openai.com/api/docs/deprecations) · [Evals migration guide](https://developers.openai.com/cookbook/examples/evaluation/moving-from-openai-evals-to-promptfoo)
