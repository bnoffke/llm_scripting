This is a personal tool to help me document personal dbt projects at scale. It leverages the OpenAI API to make LLM calls that generate columns/model descriptions.
The result of running this script for a set of dbt model is:
  1. A universal .md file is created, which contains AI-generated column descriptions with the docs blocks macro
  2. A .yml file for each model processed, which will contain an AI-generated model description, and docs macros for column descriptions that link out to the universal .md file.

To use, execute the script in the src folder with the following command line arguments:
  1: Model directory (required)
  2: Model name (optional)

If the model name isn't specified, the script will loop over all SQL files in the directory.

Much of the context is for municipal data, so the prompts have that context baked in. This could be further generalized to parameterize the context of expertise.

This uses my personal OpenAI API key, so for now this is limited to my personal work.
