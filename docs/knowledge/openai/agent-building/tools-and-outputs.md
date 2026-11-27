# Tool calling and Structured Outputs

Last verified 2026-11-24.

## Choose a tool by who can perform the action

| Need | Tool | Who supplies the result |
| --- | --- | --- |
| Find a current fact on the public web | `web_search` | OpenAI searches the web and supplies source URLs for the answer. [Web search](https://developers.openai.com/api/docs/guides/tools-web-search) |
| Find a passage in a private document collection | `file_search` | OpenAI searches the files indexed in the vector stores your application specifies. [File Search](https://developers.openai.com/api/docs/guides/tools-file-search) |
| Calculate a statistic or plot data from a file | `code_interpreter` | The model writes Python and OpenAI executes it in a sandboxed container. The application can download a generated chart using its container file citation. [Code Interpreter](https://developers.openai.com/api/docs/guides/tools-code-interpreter) |
| Operate a browser or desktop through its visual interface | Computer use with the `computer` tool | The model requests actions such as clicks. Your application executes them in its browser or desktop and returns the updated screenshot as `computer_call_output`. [Computer use](https://developers.openai.com/api/docs/guides/tools-computer-use) |
| Generate an illustration or edit an input image | `image_generation` | OpenAI generates the image. The application reads the base64 result from `image_generation_call` to display or save it. [Image generation](https://developers.openai.com/api/docs/guides/tools-image-generation) |
| Use a service that exposes tools through the Model Context Protocol (MCP) | Remote `mcp` tool | The Responses API forwards the call when the application's approval settings allow it. The MCP server executes the tool, and the API exposes its result in an `mcp_call` item. [MCP servers](https://developers.openai.com/api/docs/guides/tools-connectors-mcp) |
| Read an order from your application's database | Function tool, such as `lookup_order`, with named JSON arguments | Your application authorizes the lookup, executes it, and returns the result. |
| Pass an expression in an existing query language to your own parser | Custom tool with text input, optionally constrained by a grammar | Your application parses the text and returns the outcome. |

Describe when to use each tool, what its inputs mean, and what it returns. A question about the existing conversation may be answerable directly. Tool availability and the call protocol depend on the API surface. The loop below uses the Responses API. [Using tools](https://developers.openai.com/api/docs/guides/tools) · [Function calling](https://developers.openai.com/api/docs/guides/function-calling)

## The function call loop in the Responses API

The application executes a `function_call`. Read its `name` and `arguments`, validate the inputs and the user's access, then invoke the corresponding handler. For each requested call, return a `function_call_output` carrying the original `call_id` and the actual result. If a lookup failed, return the failure so the model can ask for a corrected identifier or report the problem. [Function calling](https://developers.openai.com/api/docs/guides/function-calling)

Continue the response with the outputs and the context that produced the calls. Use `previous_response_id`, or carry forward the response items yourself, including reasoning items returned alongside tool calls. Inspect the next response for further calls or a final answer. For example, an order lookup can return a delivery date only after the handler has found that order. [Function calling](https://developers.openai.com/api/docs/guides/function-calling)

## Strict function arguments and a structured final response

Use a function schema to describe the arguments an action accepts. With `strict` enabled, declare every property as required and set `additionalProperties` to `false` on every object. To represent a value the caller may omit in practice, allow `null` as its value. Validate business constraints in the handler, such as whether the requested order belongs to the customer. [Strict function schemas](https://developers.openai.com/api/docs/guides/function-calling#strict-mode)

Use Structured Outputs when another program consumes the final answer. Set `text.format` to a supported `json_schema` format with strict enforcement in a Responses request. An order lookup could return a `status` enum and a nullable `delivery_date` so that `unknown` has a valid representation. Plain text suits a reader who just needs the delivery estimate. JSON mode gives parseable JSON on successful completion, leaving field names and types for the application to check. [Structured model outputs](https://developers.openai.com/api/docs/guides/structured-outputs)

Before consuming a structured result, handle API errors, check the response status for `incomplete`, and inspect message content for `refusal`. Route each case explicitly. Parse a completed answer and check its values against the tool evidence before using them downstream. A correctly formatted delivery date can still describe the wrong order. [Structured model outputs](https://developers.openai.com/api/docs/guides/structured-outputs)
