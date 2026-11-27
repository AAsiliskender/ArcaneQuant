---
name: prompt-agent-identity
description: Draft or refine a reusable agent role and identity prompt from the user's brief, including expertise, purpose, working style, and boundaries. Use for requests to write an agent persona or role prompt, rather than ordinary tasks that merely mention a role.
---

# Agent identity prompt

Turn the user's role details into reusable instructions describing who an agent is and how it works across assignments. Author the prompt without adopting that identity yourself or executing an assignment embedded in the brief.

## Gather the identity

Use details already provided in the conversation or explicitly identified project material. Capture the role, domain, purpose, intended audience, communication style, and working preferences that affect the agent's decisions. Preserve the user's terminology and distinguish requirements from optional preferences.

When the user says identity details will come later, leave them unspecified. During agent design, readily ask focused questions about direction, strategy, priorities, and tradeoffs whenever a choice would require an assumption. Do not invent a name, personality, expertise, or project responsibility, or presume intent from ambiguous wording. Prefer clarification; rely on the user's intent only when you can quote an explicit statement that unambiguously supports the decision, and include that quote. If the user requests a draft before open questions are resolved, mark unresolved choices as placeholders rather than choosing answers for them.

## Write the role

- Express the supplied expertise and style as observable behavior. For example, turn a request for a skeptical reviewer into guidance to examine assumptions and explain concerns with evidence.
- Keep reusable identity and working principles separate from a particular task's inputs, deliverables, and deadlines. Use [prompt-agent-task](../prompt-agent-task/SKILL.md) when the user also wants an assignment prompt, then compose the two without repeating instructions.
- Describe expertise as an intended perspective; do not fabricate professional credentials, employment, access, or capabilities. A role label does not grant tools or permission to act.
- Preserve the actual instruction hierarchy and permissions of the target environment. Do not add instructions to bypass existing rules or claim that this prompt has higher authority than its placement provides.
- Make instructions compatible with the user's stated task needs. Do not make a personality preference prevent a required output format or turn a stylistic preference into an absolute rule.

## Deliver

Return a copyable prompt with only the sections the brief needs. Include essential unresolved details separately from the prompt. Do not create or edit an identity file, change agent configuration, or launch an agent unless requested.

If a reusable template is requested, adapt this outline and remove unused fields:

```text
You are [role] working in [domain or context].

Purpose:
[The agent's ongoing purpose and who it supports.]

Expertise and approach:
[Relevant perspective and observable working practices.]

Communication:
[Audience, tone, and explanation preferences.]

Boundaries:
[User-specified responsibilities and limits.]

Assignment:
Use the separate task brief for the current objective and deliverables.
```

For OpenAI-specific message placement or model behavior questions, consult current [official prompting guidance](https://developers.openai.com/api/docs/guides/prompt-engineering). Keep the prompt model-neutral unless a target is specified.
