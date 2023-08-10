# SQL Script splitter

Script to split big SQL queries (DBT queries also supported) that use lots of CTEs into smaller files, to speed up processing.

Files are expected to be in the following format:

```
{{ config() }}

with first_cte as
(
--query goes here
)
,nth_cte as
(
--query goes here
)

select ...
```

Notes:
- DBT section is optional
- Only the first CTE is required. Any other ones are optional.
- The comma is in the same line as the CTE table name. Whitespace between them can be whatever you prefer, or none.
- In the last CTE, only comments/whitespace can be present between the closing of the parenthesis and the final select.

# GitHub Action

To use this as a GitHub action, Python need to have been already setup in previous steps.
In your repo, you need to have a file named `sql_script_splitter.yaml`.

By default, this action will look for it at the root of the repo.
If it is in a different location, just indicate it as an input.

Example:
```
- uses: nilsonneto/sql_script_splitter@v1
  with:
    yaml-path: $GITHUB_WORKSPACE/configs/sql_script_splitter.yaml

```

# Development notes

Performance testing was done using cProfile like this:
`python -m cProfile -o p0.prof sql_script_splitter.py yaml`
And results analyzed with Snakeviz:
`snakeviz p0.prof`
