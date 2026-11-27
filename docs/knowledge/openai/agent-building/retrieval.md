# Retrieval-augmented generation with local files and hosted File Search

Last verified 2026-11-24.

## Local RAG with Markdown and `rg`

Retrieval-augmented generation (RAG) means selecting relevant source text and using it as evidence for an answer. An agent with workspace read access can search this collection offline. Start with a term from the question, open the matching section, then cite the passage that supports the claim. For a paraphrased question, try related words and API identifiers from the local index.

For example, to answer “Who returns the result of a function the model calls?”, run these read-only commands from the repository root in PowerShell.

```powershell
rg -n -i '^## .*function call loop' docs/knowledge/openai/agent-building
Get-Content -Encoding UTF8 -LiteralPath 'docs/knowledge/openai/agent-building/tools-and-outputs.md'
```

The matching section supports this answer. The application runs the function and sends its result back as `function_call_output` with the matching `call_id`. Link to that heading so the reader can check the answer offline. If a second search also turns up no supporting passage, say which part the available material leaves unanswered. [The function call loop in the Responses API](tools-and-outputs.md#the-function-call-loop-in-the-responses-api)

## Hosted File Search and vector stores

OpenAI File Search is a hosted option for an application that needs the Responses API to search uploaded documents. The application uploads selected files and attaches them to a vector store. OpenAI splits the files into chunks, computes embeddings, and indexes the chunks. Indexing is asynchronous. Check that each intended file has reached `completed` before relying on it for answers. A local directory path alone gives a hosted run no access to those files. [Retrieval guide](https://developers.openai.com/api/docs/guides/retrieval) · [File Search guide](https://developers.openai.com/api/docs/guides/tools-file-search)

The application supplies `file_search` and `vector_store_ids` on a Responses request. When the model calls the tool, OpenAI combines semantic and keyword search. Semantic matches help when the question uses a paraphrase, while keyword matches help with exact identifiers. File attributes can restrict search to a relevant subset, such as a particular product or language. An application that needs to examine ranked passages itself can call `vector_stores.search` directly. [File Search guide](https://developers.openai.com/api/docs/guides/tools-file-search) · [Retrieval guide](https://developers.openai.com/api/docs/guides/retrieval)

## File citations and search diagnostics

For a hosted answer, inspect the output message's `file_citation` annotations for the cited file ID and filename. To diagnose a missing answer, request `include=["file_search_call.results"]` and inspect the returned passages. This helps distinguish a search that missed the evidence from an answer that overlooked a relevant passage. Verify that the text supports the claim before presenting the file citation. [File Search guide](https://developers.openai.com/api/docs/guides/tools-file-search)
