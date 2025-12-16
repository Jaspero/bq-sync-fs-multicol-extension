#!/usr/bin/env node

/**
 * Config Generator Script
 * 
 * This script reads JSON collection config files from the collections/ directory
 * and generates a single sync-config.json file that the Firebase extension uses.
 * 
 * Usage:
 *   npm run generate-config
 *   npx ts-node scripts/generate-config.ts
 * 
 * The generated config is placed in functions/src/generated/sync-config.json
 */

import * as fs from 'fs';
import * as path from 'path';
import { CollectionSyncConfig, SyncConfiguration, FieldDefinition } from '../src/types/sync-config.interface';

const COLLECTIONS_DIR = path.resolve(__dirname, '../../collections');
const OUTPUT_DIR = path.resolve(__dirname, '../src/generated');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'sync-config.json');

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
    console.warn('Creating empty collections directory...');
    fs.mkdirSync(COLLECTIONS_DIR, { recursive: true });
    return configs;
  }

  const files = fs.readdirSync(COLLECTIONS_DIR).filter(f => f.endsWith('.json'));
  
  console.log(`Found ${files.length} collection config files`);
  
  for (const file of files) {
    const filePath = path.join(COLLECTIONS_DIR, file);
    try {
      const content = fs.readFileSync(filePath, 'utf-8');
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
        datasetId: rawConfig.datasetId || 'firestore_sync',
        tableId: rawConfig.tableId,
        datasetLocation: rawConfig.datasetLocation || 'eu',
        backfill: rawConfig.backfill !== false,
        includeParentIdInDocumentId: rawConfig.includeParentIdInDocumentId || false,
        transformUrl: rawConfig.transformUrl,
        schedule: rawConfig.schedule || '0 0 * * *',
        timeZone: rawConfig.timeZone || 'UTC',
        fields: rawConfig.fields
      };

      configs.push(config);
      console.log(`  ✅ ${file}: Loaded config for '${config.id}' with ${config.fields.length} fields`);
    } catch (error: any) {
      console.error(`  ❌ ${file}: ${error.message}`);
    }
  }

  return configs;
}

function generateFieldMap(config: CollectionSyncConfig): string {
  return config.fields.map(field => {
    const parts: string[] = [field.name];
    
    // Type (with array subtype if applicable)
    if (field.type === 'ARRAY' && field.arrayType) {
      parts.push(`${field.type}_${field.arrayType}`);
    } else {
      parts.push(field.type);
    }
    
    // Accessor (optional)
    if (field.accessor) {
      parts.push(field.accessor);
    } else if (field.formater || field.method) {
      parts.push(''); // Empty accessor placeholder
    }
    
    // Formater (optional)
    if (field.formater) {
      parts.push(field.formater);
    } else if (field.method) {
      parts.push(''); // Empty formater placeholder
    }
    
    // Method (optional)
    if (field.method) {
      parts.push(field.method);
    }

    return parts.join('|');
  }).join(',');
}

function generateEnvConfig(configs: CollectionSyncConfig[]): string {
  const lines: string[] = [
    '# Auto-generated BigQuery Sync Configuration',
    `# Generated at: ${new Date().toISOString()}`,
    `# Total collections: ${configs.length}`,
    '',
  ];

  // Generate combined config as JSON string
  const syncConfig: SyncConfiguration = {
    defaultDatasetLocation: 'eu',
    defaultDatasetId: 'firestore_sync',
    defaultSchedule: '0 0 * * *',
    defaultTimeZone: 'UTC',
    collections: configs
  };

  lines.push(`SYNC_CONFIG='${JSON.stringify(syncConfig)}'`);
  lines.push('');

  return lines.join('\n');
}

function main() {
  console.log('🔄 BigQuery Sync Config Generator\n');
  console.log(`Reading configs from: ${COLLECTIONS_DIR}`);
  console.log(`Output file: ${OUTPUT_FILE}\n`);

  const configs = loadCollectionConfigs();

  if (configs.length === 0) {
    console.warn('\n⚠️  No valid collection configs found');
    console.log('\nCreate JSON files in the collections/ directory with the following structure:');
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
    defaultDatasetLocation: 'eu',
    defaultDatasetId: 'firestore_sync',
    defaultSchedule: '0 0 * * *',
    defaultTimeZone: 'UTC',
    collections: configs
  };

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(syncConfig, null, 2));
  console.log(`\n✅ Generated ${OUTPUT_FILE}`);

  // Also generate .env format for reference
  const envFile = path.join(OUTPUT_DIR, 'sync-config.env');
  fs.writeFileSync(envFile, generateEnvConfig(configs));
  console.log(`✅ Generated ${envFile}`);

  // Generate TypeScript const for type safety
  const tsFile = path.join(OUTPUT_DIR, 'sync-config.ts');
  const tsContent = `// Auto-generated - do not edit manually
// Generated at: ${new Date().toISOString()}

import { SyncConfiguration } from '../types/sync-config.interface';

export const SYNC_CONFIG: SyncConfiguration = ${JSON.stringify(syncConfig, null, 2)};
`;
  fs.writeFileSync(tsFile, tsContent);
  console.log(`✅ Generated ${tsFile}`);

  // Summary
  console.log('\n📊 Summary:');
  console.log(`   Total collections: ${configs.length}`);
  configs.forEach(c => {
    console.log(`   - ${c.id}: ${c.collectionPaths.join(', ')} → ${c.datasetId}.${c.tableId}`);
  });
}

main();
