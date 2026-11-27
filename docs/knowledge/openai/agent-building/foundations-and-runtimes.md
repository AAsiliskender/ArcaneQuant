# Agent foundations and runtime selection

Last verified 2026-11-24.

## What makes a workflow an agent?

An agent uses a model to decide how to carry out a task. Its instructions define the objective, its tools give it access to information or actions, and guardrails check conditions that matter to the application. Consider a support request about a missing order. The agent can look up the order, inspect the result, and decide whether it has enough evidence to answer or needs a tracking lookup. Each tool result can change its next step. [Building agents](https://developers.openai.com/tracks/building-agents)

## How do I choose between the Responses API, Agents SDK, and Agents API?

Choose based on who runs the loop, who owns the application state, and how much control you need over each step. The Building Agents track introduces the Responses API and Agents SDK. The current platform guide also covers a managed Agents API. [Building agents](https://developers.openai.com/tracks/building-agents) · [Compare agent runtimes](https://developers.openai.com/api/docs/guides/agents) · [Using tools](https://developers.openai.com/api/docs/guides/tools)

| Runtime | Choose it when | Who does what |
| --- | --- | --- |
| Responses API | Your application needs direct access to model output and tool calls. | Your code submits input, inspects output items, executes custom function calls, and sends their results back. OpenAI runs hosted tools such as File Search. |
| Agents SDK | You want agents, handoffs, and a reusable runner in your own application. | The SDK coordinates model calls and tool results. Your application supplies instructions, tool implementations, and deployment. |
| Agents API | You want a managed agent that can resume long-running work. | OpenAI runs the Codex harness and persists session progress. Your application selects the tools and any execution environment the task requires. |

## How does a follow-up remember the previous turn?

For the Responses API, choose one way to supply history. Send the relevant prior messages and output items yourself, set `previous_response_id` to continue a particular response, or use a Conversations API identifier to associate turns with a durable conversation. For example, for a follow-up such as “What about the second one?”, continue the same history so the model can identify the list the user means. [Conversation state](https://developers.openai.com/api/docs/guides/conversation-state)

When chaining with `previous_response_id`, send any still-applicable system or developer instructions from the previous request’s `instructions` parameter again. That parameter applies to the individual response, even when the conversation history continues. [Create a model response](https://developers.openai.com/api/reference/resources/responses/methods/create)

In the Python Agents SDK, a `session` can load and save history for the runner. The quickstart also shows manual continuation using `result.to_input_list()` and OpenAI-managed continuation using a conversation or previous response. Pick a single history mechanism for a given interaction so the same prior turns reach the model once. [Agents SDK quickstart](https://openai.github.io/openai-agents-python/quickstart/)

## Which model or reasoning setting fits the task?

Compare candidates on examples of the actual work. For a lookup agent, check whether it selects the right tool, provides valid arguments, and uses the returned evidence. For a planning agent, include tasks that require several dependent steps. Measure accuracy, latency, and cost, then choose a configuration that meets the task's requirements. Check the current model guide for available reasoning controls, input types, and supported tools. [Building agents](https://developers.openai.com/tracks/building-agents) · [Model selection](https://developers.openai.com/api/docs/guides/model-selection)

For delegation and control flow, see [Agent orchestration](orchestration.md).
