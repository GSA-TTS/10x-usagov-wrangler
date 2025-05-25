import os
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

# model = "o4-mini" # 250k tpm, 250 rpm
# model = "o3" # 250k tpm, 250 rpm
# model = "o3-mini" # 2.5M tpm, 250 rpm
# model = "gpt-4.1" # 1M tpm, 1k rpm, 1M context
model = "gpt-4.1-mini"  # 1M tpm, 1k rpm, 1M context
api_version = "2024-12-01-preview"
east_api_key = os.getenv("CHAT_CLIENT_API_KEY")
east_endpoint = os.getenv("CHAT_CLIENT_ENDPOINT")
CHAT_CLIENT = AzureOpenAI(
    api_version=api_version,
    azure_endpoint=east_endpoint,
    api_key=east_api_key,
)

SYSTEM_PROMPT = f"""
You are going to break a markdown document into chunks of roughly 1000 characters each for a RAG pipeline.

At the beginning of each chunk, include both the title of the document and two example queries. Write the example queries concisely, more like web searches than natural conversation. Make sure these queries are relevant to each chunk. It's okay if they are repeated across chunks, as long as they are relevant. Include the metadata at the top of each chunk in a metadata section. Here's an example:

--- Begin Chunk ---
--- Begin Chunk Metadata ---
Full URL: https://www.usa.gov/death-certificate
Title: How to get a certified copy of a death certificate
Example Query One: How do I get a death certificate?
Example Query Two: Get death certificate
--- End Chunk Metadata ---
Roughly 1000 characters worth of markdown content from the provided document."
--- End Chunk ---

Make sure to create a chunk for all relevant content. When in doubt, include content rather than leave it out. You may skip content like "last updated date" "sharing links" and any navigation that is unrelated to the title of the document.

Please respond only with the chunks as in the chunk example above and nothing else.
"""  # noqa: E501


def chunk_prompt(url: str, title: str, doc_content: str) -> str:
    return f"""
Here's the markdown document I want you to break into chunks. It is titled "{title}" and its full URL is "{url}":

```markdown
{doc_content}
```
"""


def chat_completion(prompt, client, system_prompt, model="gpt-4.1-mini"):
    """
    Get chat completion.
    """
    response = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        max_completion_tokens=10000,
        model=model,
    )
    return response.choices[0].message.content
