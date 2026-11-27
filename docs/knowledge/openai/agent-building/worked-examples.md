# Worked examples from the Building Agents track

Last verified 2026-11-24.

## How does the support demo combine File Search with human approval?

A customer asks how to cancel an order. The Responses API uses hosted File Search over the demo's support articles. A support representative can inspect the relevant articles, edit the suggested reply, and send it. If the customer then asks to cancel a particular order, the agent proposes `cancel_order`. The representative reviews that action separately from the wording of the reply. The demo uses placeholder functions, so approving a cancellation leaves the sample order intact. [Support agent demo and limitations](https://github.com/openai/openai-support-agent-demo#demo-flow) · [File Search](https://developers.openai.com/api/docs/guides/tools-file-search)

## How does the airline demo hand a disrupted trip to another agent?

A traveller reports a delay on the Paris–New York leg of a trip to Austin. The triage agent routes the request to the flight information specialist, which checks the mock itinerary and finds alternate flights. It hands the conversation to the booking specialist, which uses `book_new_flight` and returns updated booking details. A later request for a particular seat goes to the seat specialist. This gives a reviewer concrete steps to evaluate independently, including routing, the choice of replacement flight, and the final itinerary. [Customer service agents demo](https://github.com/openai/openai-cs-agents-demo#demo-flows)

## How does the frontend testing demo use a browser to check a workflow?

The tester supplies a task and target URL. The sample task asks the agent to find two garments within a price limit and complete checkout on a sample shop. A Node service drives a Playwright browser. After each action the service can return the screen to the model, which chooses what to do next. A reviewer can compare the observed checkout result with the requested items. [Testing agent demo](https://github.com/openai/openai-testing-agent-demo) · [Sample test case](https://github.com/openai/openai-testing-agent-demo/blob/main/frontend/lib/constants.ts)

The demo README describes a preview-era model and limits the example to test environments. OpenAI retired `computer-use-preview` on 2026-07-23. The reusable part is the cycle of observing the application, performing an action, and checking the result. For a new integration, consult the current guide for the supported interface and isolation guidance. [Demo limitations](https://github.com/openai/openai-testing-agent-demo#security-notes) · [Deprecations](https://developers.openai.com/api/docs/deprecations) · [Computer use](https://developers.openai.com/api/docs/guides/tools-computer-use)
