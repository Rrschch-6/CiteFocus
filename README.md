# CiteFocus `litev2`

- Parses a PDF and extracts bibliography entries plus citation context.
- Routes each citation to the most relevant local sources: `arxiv`, `dblp`, and `openalex`.
- Runs exact matching first using identifiers and normalized titles.
- Runs lexical retrieval next and ranks candidates across local databases.
- In lexical `all` mode, falls back to Crossref only if all local DBs return no candidates.
- Fuses exact and lexical outputs into one selected candidate plus backups when scores are close.
- Verifies the selected citation bibliographically using title, author, year, DOI, URL, and arXiv ID checks.
- Runs semantic support checking when candidate abstracts and citation context are available.
- Builds final JSON, CSV, and chart-based reports for review.
- Enriches DBLP records with abstracts during index build using local OpenAlex and arXiv metadata when possible.
