# airtable-to-notion

Migrate an [Airtable](https://airtable.com) base's tables into
[Notion](https://notion.so) databases over the API, preserving field types and
rebuilding linked-record fields as live Notion relations rather than flattening
them to text.

A CSV export loses relations, select options, and field types: every column
arrives in Notion as text and linked records become bare strings. This tool
reads the Airtable schema and records directly through the API, maps each field
to the closest Notion property type, and runs a second pass that reconstructs
linked-record fields as real Notion relations.

## How it works

The migration runs in two passes in a single command:

1. **Schema + records.** For each Airtable table it reads the full schema and
   all records (paginated), creates one Notion database with the scalar
   properties, and inserts a page per record — recording an
   `airtable_record_id -> notion_page_id` map.
2. **Relations.** For each Airtable linked-record field it adds a relation
   property to the source data source pointing at the target data source
   (resolved from the field's `linkedTableId` in the schema), then patches each
   page to set the actual links. Two-way Airtable links are detected via each
   field's `inverseLinkFieldId` and created as a single synced Notion relation
   (`dual_property`), so a bidirectional link becomes one two-way relation rather
   than two disconnected one-way ones.

Progress is checkpointed to `migration_state_<base_id>.json` as it runs, so an
interrupted migration can be resumed rather than restarted.

This targets **Notion API version `2025-09-03`**, in which a "database" is a
container and the table of records is a "data source". Pages are created under a
`data_source_id` parent and relations point at data source IDs.

## Requirements

- Python 3.9+
- `pip install -r requirements.txt` (`requests`; `python-dotenv` optional)

## Setup

1. **Create an Airtable personal access token** at
   [https://airtable.com/create/tokens](https://airtable.com/create/tokens) with the scopes `schema.bases:read` and
   `data.records:read`, and grant it access to the base you are migrating.
2. **Find the base id.** It starts with `app` and appears in the base URL
   (`airtable.com/appXXXXXXXX/...`) or in the Airtable API docs for your base.
3. **Create a Notion internal integration** at
   [https://www.notion.so/my-integrations](https://www.notion.so/my-integrations) and copy its secret.
4. **Connect the integration to a parent page.** Open the Notion page you want
   the databases created under, then `•••` -\> **Connections** -\> select your
   integration. This step is required: without it, the API returns
   `object_not_found` even with a correct page ID.

## Configuration

Copy `.env.example` to `.env` and fill in the values:

```
AIRTABLE_API_TOKEN=pat_...
AIRTABLE_BASE_ID=appXXXXXXXXXXXXXX
NOTION_API_TOKEN=ntn_...
NOTION_PARENT_PAGE_ID=...
```

With `python-dotenv` installed, the script loads `.env` automatically; otherwise
export the same variables in your shell. \*\*`.env` is gitignored — never commit
real tokens.\*\*

## Run

```bash
python airtable_to_notion.py
```

The log reports each table created, rows inserted, and relations linked, ending
with a summary.

**Resuming.** If a run is interrupted — a crash, a network timeout, or `Ctrl+C` —
run the same command again. Completed tables are skipped, a partially inserted
table continues from the last checkpoint, and databases already created are
reused rather than duplicated. Use `--restart` to discard saved progress and
begin the base from scratch (this does not delete databases already created in
Notion; remove those manually first to avoid duplicates).

**Filtering tables.** By default every table in the base is migrated. Use a
deny-list to exclude tables, or an allow-list to migrate only a named few:

```bash
python airtable_to_notion.py --skip "Imported table, Archive"
python airtable_to_notion.py --only "Projects, Clients"
```

The same lists can be set via `AIRTABLE_SKIP_TABLES` / `AIRTABLE_ONLY_TABLES` in
`.env`; command-line flags override them.

## What is preserved, and what is not

Preserved: text, numbers (with currency/percent formatting), rating, duration,
count, autoNumber, dates and created/modified times, checkboxes,
email/URL/phone, single- and multiple-select (options read straight from the
schema), numeric and date formula/rollup results, and linked-record fields
rebuilt as Notion relations.

Lossy or skipped, by design:

- **Attachments** are stored as their URLs in a text property. Airtable
  attachment URLs are time-limited, so treat them as a record of what was
  attached rather than durable links.
- **Collaborator** and **created/modified by** fields become the person's name
  as text, because Notion's people property needs matching workspace user IDs.
- **Lookup** and non-numeric **formula/rollup** fields become text, since their
  result shape varies per record.
- **Button** fields are skipped.
- Linked records pointing at a table that was not migrated (for example, one
  excluded by a filter) are logged and left unlinked rather than guessed at.

## Notes and limits

- Airtable's API allows about 5 requests/second per base and Notion's about 3;
  the script throttles to both and backs off on rate limits, timeouts, and
  transient errors, so large bases simply take a while.
- Resume assumes the databases recorded in the state file still exist in Notion.
  If you manually delete a database the state considers complete, use `--restart`
  (and clear the corresponding Notion databases) rather than resuming.

## Engineering notes

A few design decisions worth calling out.

**Relational integrity is the point.** A flat export collapses every
linked-record field into a string. Preserving those links requires a two-pass
approach, because a relation can only be created once both records exist with
stable IDs. Pass 1 creates every database and record and records an
`airtable_record_id -> notion_page_id` map; pass 2 walks the linked-record
fields and resolves each Airtable record ID to the Notion page created in pass 1.
Airtable makes this cleaner than most sources, since the schema states each
linked field's target table directly and record links come back as plain ID
arrays.

**Schema-driven, not data-guessed.** Airtable's Metadata API returns the full
typed schema up front, so field types, select choices, and relation targets are
read from the schema rather than inferred by sampling records. That makes the
type mapping deterministic and the select options complete even for values that
never appear in the data.

**Built for the current Notion data model.** Notion's `2025-09-03` API version
made a "database" a container holding one or more "data sources," moving record
creation, schema edits, and relation targets to `data_source_id`. The loader
targets that model directly rather than the superseded shape.

**Idempotent, resumable, and rate-aware.** Real migrations of thousands of
records run long enough that failure is a certainty. Progress is checkpointed to
disk per base, so an interruption resumes from the last saved row instead of
restarting; created databases are reused and rows matched by source ID so
nothing duplicates. Every request is throttled to the provider's limit and
retried with exponential backoff, covering `429`/`5xx`, read timeouts, dropped
connections, and a transient `400` Notion returns while a new database settles.

**Type mapping with explicit tradeoffs.** Each Airtable field maps to the
closest Notion property; where no faithful mapping exists the loss is deliberate
and documented rather than silent (attachments become URLs, collaborators become
names, buttons are skipped). The guiding rule is to never fabricate data to fill
a type that cannot be honestly populated.

## License

MIT — see [LICENSE](LICENSE).
