<!-- 
This file provides your users an overview of your extension. All content is optional, but this is the recommended format. Your users will see the contents of this file when they run the `firebase ext:info` command.

Include any important functional details as well as a brief description for any additional setup required by the user (both pre- and post-installation).

Learn more about writing a PREINSTALL.md file in the docs:
https://firebase.google.com/docs/extensions/publishers/user-documentation#writing-preinstall
-->

# BigQuery Sync (Multi-Collection)

Sync multiple Firestore collections to BigQuery tables using a single Firebase Extension instance.

## Features

- **Multi-Collection Support**: Configure multiple collections in a single extension instance
- **JSON Configuration**: Define collections using simple JSON files
- **Automatic Table Creation**: BigQuery datasets and tables are created automatically
- **Real-time Sync**: Document changes are tracked in real-time via Firestore triggers
- **Scheduled Sync**: A scheduled function consolidates changes to main tables
- **Backfill Support**: Optionally backfill existing data during setup
- **Subcollection Support**: Sync subcollections with parent ID tracking

## Configuration

### Option 1: JSON Configuration Files (Recommended)

Create JSON files in the `collections/` directory at the root of your project:

```json
// collections/users.json
{
  "id": "users",
  "collectionPaths": ["users"],
  "datasetId": "firestore_sync",
  "tableId": "users",
  "datasetLocation": "eu",
  "backfill": true,
  "schedule": "0 0 * * *",
  "timeZone": "UTC",
  "fields": [
    { "name": "email", "type": "STRING" },
    { "name": "displayName", "type": "STRING" },
    { "name": "createdOn", "type": "TIMESTAMP" }
  ]
}
```

Then generate the configuration:

```bash
cd functions
npm run generate-config
npm run build
```

### Option 2: Environment Variable

Set the `SYNC_CONFIG` parameter during installation with the full JSON configuration.

### Collection Configuration Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Unique identifier for this collection sync |
| `collectionPaths` | string[] | Yes | Firestore collection paths to sync (supports `{wildcards}`) |
| `collectionGroup` | string | No | Collection group name for group queries |
| `datasetId` | string | Yes | BigQuery dataset ID |
| `tableId` | string | Yes | BigQuery table ID |
| `datasetLocation` | string | Yes | BigQuery dataset location (e.g., "eu", "us") |
| `fields` | Field[] | Yes | Field definitions for the table |
| `backfill` | boolean | No | Whether to backfill existing data (default: true) |
| `includeParentIdInDocumentId` | boolean | No | Include parent doc ID in document ID |
| `schedule` | string | No | Cron schedule for sync (default: "0 0 * * *") |
| `timeZone` | string | No | Timezone for schedule (default: "UTC") |
| `transformUrl` | string | No | URL to transform data before writing |

### Field Definition Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Field name in BigQuery |
| `type` | string | Yes | BigQuery type (STRING, NUMERIC, TIMESTAMP, BOOL, ARRAY, JSON) |
| `accessor` | string | No | JSON pointer path to access nested data |
| `arrayType` | string | No | Element type for ARRAY fields |
| `method` | string | No | Custom transformation function (as string) |

### Supported BigQuery Types

- `STRING` - Text data
- `NUMERIC` / `FLOAT64` / `INT64` - Numbers
- `TIMESTAMP` / `DATETIME` / `DATE` - Date/time values
- `BOOL` - Boolean values
- `ARRAY` - Repeated fields (specify `arrayType` for element type)
- `JSON` - JSON data

### Example: Subcollection Configuration

```json
{
  "id": "user-notifications",
  "collectionPaths": [
    "users/{parentId}/notifications"
  ],
  "collectionGroup": "notifications",
  "datasetId": "firestore_sync",
  "tableId": "notifications",
  "datasetLocation": "eu",
  "backfill": true,
  "includeParentIdInDocumentId": true,
  "fields": [
    { "name": "parentId", "type": "STRING", "accessor": "/parentId" },
    { "name": "type", "type": "STRING" },
    { "name": "message", "type": "STRING" },
    { "name": "read", "type": "BOOL" },
    { "name": "createdOn", "type": "TIMESTAMP" }
  ]
}
```

### Example: Custom Field Transformation

```json
{
  "id": "medications",
  "collectionPaths": ["tests/{parentId}/medications"],
  "datasetId": "firestore_sync",
  "tableId": "medications",
  "datasetLocation": "eu",
  "fields": [
    { "name": "value", "type": "STRING" },
    {
      "name": "isHRT",
      "type": "BOOL",
      "accessor": "/value",
      "method": "(value) => { return value && value.toLowerCase().includes('hrt'); }"
    }
  ]
}
```

<!-- We recommend keeping the following section to explain how billing for Firebase Extensions works -->
# Billing

This extension uses other Firebase or Google Cloud Platform services which may have associated charges:

<!-- List all products the extension interacts with -->
- Cloud Functions
- BigQuery

When you use Firebase Extensions, you're only charged for the underlying resources that you use. A paid-tier billing plan is only required if the extension uses a service that requires a paid-tier plan, for example calling to a Google Cloud Platform API or making outbound network requests to non-Google services. All Firebase services offer a free tier of usage. [Learn more about Firebase billing.](https://firebase.google.com/pricing)

