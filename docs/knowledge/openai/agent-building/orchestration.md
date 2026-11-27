# Agent orchestration

Last verified 2026-11-24.

## What happens inside a single-agent run?

The Python Agents SDK starts with an `Agent` and an input to `Runner.run()`. The runner calls the model and examines its output. A function tool call leads to execution and another model turn with the result. A handoff switches the active agent. A final output ends the run and is available as `result.final_output`. Set a turn limit for tasks with repeated lookups. The default error for exceeding the configured limit is `MaxTurnsExceeded`. [SDK quickstart](https://openai.github.io/openai-agents-python/quickstart/) · [Running agents](https://openai.github.io/openai-agents-python/running_agents/)

## When does a second agent help?

Start with one agent when the same instructions and tools cover the task. Add a specialist when a distinct responsibility needs its own instructions or tools, or when several independent subtasks can run concurrently. For example, a report writer can ask specialists to examine different documents in parallel. The writer must wait for their evidence before synthesizing the report. [Building agents](https://developers.openai.com/tracks/building-agents) · [SDK orchestration](https://openai.github.io/openai-agents-python/multi_agent/)

## Should I use a handoff or an agent as a tool?

With a **handoff**, the selected specialist takes over the conversation for that part of the turn. In OpenAI's airline demo, triage can route a request to change a seat to the specialist that can handle it. A short handoff description tells the routing agent when a specialist is appropriate. [Handoffs](https://openai.github.io/openai-agents-python/handoffs/) · [Airline customer service demo](https://github.com/openai/openai-cs-agents-demo)

With an **agent as a tool**, the caller gives a specialist a bounded assignment and receives its result. A manager that calls two translators via `Agent.as_tool()` can combine their translations into a single response and remains responsible for what the user sees. Decide who should own the final answer before choosing the pattern. [SDK orchestration](https://openai.github.io/openai-agents-python/multi_agent/)

## Who should choose the next step?

Put fixed dependencies and required checks in application code. If every document needs extraction, validation, and then a summary, code can enforce that order and handle a failed validation explicitly. Let the model choose among relevant tools when the next step depends on what it learns, such as deciding which part of a manual to search after a first lookup. Both approaches can coexist in one workflow. [SDK orchestration](https://openai.github.io/openai-agents-python/multi_agent/)

## What context goes to a specialist?

The SDK makes local application context available to tools and callbacks. A database client in `RunContextWrapper.context`, for example, stays in application code. The model sees the information you include in its inputs, instructions, or tool results. A handoff can forward conversation history. With locally managed history, a handoff input filter can select what the specialist receives. With server-managed history, start a separate run with fresh history when the specialist may receive only selected material. [SDK context management](https://openai.github.io/openai-agents-python/context/) · [Handoff input filters](https://openai.github.io/openai-agents-python/handoffs/)

## How do I diagnose a wrong route or a stalled run?

A trace records the sequence of model calls, tools, handoffs, and guardrail checks. For a wrong route, inspect the handoff the model chose and its description. For a repeated lookup, check the tool result the next model turn received. A turn-limit error tells you where the run stopped. Compare the trace with the expected sequence to find where it first diverged. [SDK tracing](https://openai.github.io/openai-agents-python/tracing/) · [Running agents](https://openai.github.io/openai-agents-python/running_agents/)

For choosing the API and continuing a conversation across turns, see [Agent foundations and runtime selection](foundations-and-runtimes.md).
