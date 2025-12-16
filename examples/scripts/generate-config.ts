#!/usr/bin/env node

/**
 * Config Generator Script
 *
 * This script reads JSON collection config files from the examples/collections/ directory
 * and generates configuration files that can be used with the Firebase extension.
 *
 * Usage:
 *   npx ts-node examples/scripts/generate-config.ts
 *
 * The generated config is placed in examples/generated/
 */

import * as fs from "fs";
import * as path from "path";

// Types copied here to avoid dependency on functions/src
interface FieldDefinition {
  name: string;
  type: string;
  accessor?: string;
  formater?: string;
  method?: string;
  arrayType?: string;
}

interface CollectionSyncConfig {
  id: string;
  collectionPaths: string[];
  collectionGroup?: string;
  datasetId: string;
  tableId: string;
  datasetLocation: string;
  fields: FieldDefinition[];
  backfill: boolean;
  includeParentIdInDocumentId?: boolean;
  transformUrl?: string;
  schedule: string;
  timeZone: string;
}

interface SyncConfiguration {
  defaultDatasetLocation: string;
  defaultDatasetId: string;
  defaultSchedule: string;
  defaultTimeZone: string;
  collections: CollectionSyncConfig[];
}

const COLLECTIONS_DIR = path.resolve(__dirname, "../collections");
const OUTPUT_DIR = path.resolve(__dirname, "../generated");
const OUTPUT_FILE = path.join(OUTPUT_DIR, "sync-config.json");

// Read defaults from environment or use fallbacks
const DEFAULT_DATASET_LOCATION = process.env.DATASET_LOCATION || "eu";
const DEFAULT_DATASET_ID = process.env.DATASET_ID || "firestore_sync";
const DEFAULT_SCHEDULE = process.env.SCHEDULE || "0 0 * * *";
const DEFAULT_TIME_ZONE = process.env.TIME_ZONE || "UTC";

interface RawCollectionConfig {
  id: string;
  collectionPaths: string[];
  collectionGroup?: string;
  datasetId?: string;
  tableId: string;
  datasetLocation?: string;
  backfill?: boolean;
  includeParentIdInDocumentId?: boolean;
  transformUrl?: string;
  schedule?: string;
  timeZone?: string;
  fields: FieldDefinition[];
}

function loadCollectionConfigs(): CollectionSyncConfig[] {
  const configs: CollectionSyncConfig[] = [];

  if (!fs.existsSync(COLLECTIONS_DIR)) {
    console.warn(`Collections directory not found: ${COLLECTIONS_DIR}`);
    console.warn("Creating empty collections directory...");
    fs.mkdirSync(COLLECTIONS_DIR, { recursive: true });
    return configs;
  }

  const files = fs
    .readdirSync(COLLECTIONS_DIR)
    .filter((f) => f.endsWith(".json"));

  console.log(`Found ${files.length} collection config files`);

  for (const file of files) {
    const filePath = path.join(COLLECTIONS_DIR, file);
    try {
      const content = fs.readFileSync(filePath, "utf-8");
      const rawConfig: RawCollectionConfig = JSON.parse(content);

      // Validate required fields
      if (!rawConfig.id) {
        console.error(`  ❌ ${file}: Missing required field 'id'`);
        continue;
      }
      if (!rawConfig.collectionPaths || !rawConfig.collectionPaths.length) {
        console.error(`  ❌ ${file}: Missing required field 'collectionPaths'`);
        continue;
      }
      if (!rawConfig.tableId) {
        console.error(`  ❌ ${file}: Missing required field 'tableId'`);
        continue;
      }
      if (!rawConfig.fields || !rawConfig.fields.length) {
        console.error(`  ❌ ${file}: Missing required field 'fields'`);
        continue;
      }

      // Apply defaults
      const config: CollectionSyncConfig = {
        id: rawConfig.id,
        collectionPaths: rawConfig.collectionPaths,
        collectionGroup: rawConfig.collectionGroup,
        datasetId: rawConfig.datasetId || DEFAULT_DATASET_ID,
        tableId: rawConfig.tableId,
        datasetLocation: rawConfig.datasetLocation || DEFAULT_DATASET_LOCATION,
        backfill: rawConfig.backfill !== false,
        includeParentIdInDocumentId:
          rawConfig.includeParentIdInDocumentId || false,
        transformUrl: rawConfig.transformUrl,
        schedule: rawConfig.schedule || DEFAULT_SCHEDULE,
        timeZone: rawConfig.timeZone || DEFAULT_TIME_ZONE,
        fields: rawConfig.fields,
      };

      configs.push(config);
      console.log(
        `  ✅ ${file}: Loaded config for '${config.id}' with ${config.fields.length} fields`
      );
    } catch (error: any) {
      console.error(`  ❌ ${file}: ${error.message}`);
    }
  }

  return configs;
}

function generateEnvConfig(configs: CollectionSyncConfig[]): string {
  const lines: string[] = [
    "# Auto-generated BigQuery Sync Configuration",
    `# Generated at: ${new Date().toISOString()}`,
    `# Total collections: ${configs.length}`,
    "",
    "# Copy this value to the SYNC_CONFIG parameter when installing the extension",
    "",
  ];

  // Generate combined config as JSON string
  const syncConfig: SyncConfiguration = {
    defaultDatasetLocation: DEFAULT_DATASET_LOCATION,
    defaultDatasetId: DEFAULT_DATASET_ID,
    defaultSchedule: DEFAULT_SCHEDULE,
    defaultTimeZone: DEFAULT_TIME_ZONE,
    collections: configs,
  };

  lines.push(`SYNC_CONFIG='${JSON.stringify(syncConfig)}'`);
  lines.push("");

  return lines.join("\n");
}

function main() {
  console.log("🔄 BigQuery Sync Config Generator\n");
  console.log(`Reading configs from: ${COLLECTIONS_DIR}`);
  console.log(`Output directory: ${OUTPUT_DIR}\n`);

  const configs = loadCollectionConfigs();

  if (configs.length === 0) {
    console.warn("\n⚠️  No valid collection configs found");
    console.log(
      "\nCreate JSON files in the examples/collections/ directory with the following structure:"
    );
    console.log(`
{
  "id": "my-collection",
  "collectionPaths": ["my-collection"],
  "tableId": "my_table",
  "fields": [
    { "name": "field1", "type": "STRING" },
    { "name": "field2", "type": "NUMERIC" }
  ]
}
`);
    return;
  }

  // Ensure output directory exists
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  // Generate the sync config JSON
  const syncConfig: SyncConfiguration = {
    defaultDatasetLocation: DEFAULT_DATASET_LOCATION,
    defaultDatasetId: DEFAULT_DATASET_ID,
    defaultSchedule: DEFAULT_SCHEDULE,
    defaultTimeZone: DEFAULT_TIME_ZONE,
    collections: configs,
  };

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(syncConfig, null, 2));
  console.log(`\n✅ Generated ${OUTPUT_FILE}`);

  // Also generate .env format for reference
  const envFile = path.join(OUTPUT_DIR, "sync-config.env");
  fs.writeFileSync(envFile, generateEnvConfig(configs));
  console.log(`✅ Generated ${envFile}`);

  // Summary
  console.log("\n📊 Summary:");
  console.log(`   Total collections: ${configs.length}`);
  configs.forEach((c) => {
    console.log(
      `   - ${c.id}: ${c.collectionPaths.join(", ")} → ${c.datasetId}.${
        c.tableId
      }`
    );
  });

  console.log("\n📋 Next steps:");
  console.log("   1. Copy the SYNC_CONFIG value from sync-config.env");
  console.log(
    "   2. Paste it into the extension's SYNC_CONFIG parameter during installation"
  );
}

main();
