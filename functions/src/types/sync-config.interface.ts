import { BigQueryType } from "./biquery-type.type";

/**
 * Field definition for BigQuery table
 */
export interface FieldDefinition {
  /** Field name in BigQuery */
  name: string;
  /** BigQuery data type */
  type: BigQueryType;
  /** JSON pointer path to access nested data (e.g., "/nested/field") */
  accessor?: string;
  /** JSON pointer path for formatting array items */
  formater?: string;
  /** Custom transformation method as string (will be eval'd) */
  method?: string;
  /** For ARRAY type, specifies the element type */
  arrayType?: string;
}

/**
 * Collection sync configuration
 */
export interface CollectionSyncConfig {
  /** Unique identifier for this sync configuration */
  id: string;
  /** Collection paths to sync (supports wildcards like {parentId}) */
  collectionPaths: string[];
  /** Optional collection group name for collection group queries */
  collectionGroup?: string;
  /** BigQuery dataset ID */
  datasetId: string;
  /** BigQuery table ID */
  tableId: string;
  /** BigQuery dataset location */
  datasetLocation: string;
  /** Field definitions for the BigQuery table */
  fields: FieldDefinition[];
  /** Whether to backfill existing data */
  backfill: boolean;
  /** Whether to include parent document ID in the document ID */
  includeParentIdInDocumentId?: boolean;
  /** Optional transform URL for custom data transformation */
  transformUrl?: string;
  /** Cron schedule for syncing to main table */
  schedule: string;
  /** Timezone for the schedule */
  timeZone: string;
}

/**
 * Root configuration containing all collection sync configs
 */
export interface SyncConfiguration {
  /** Global BigQuery dataset location (can be overridden per collection) */
  defaultDatasetLocation: string;
  /** Global dataset ID (can be overridden per collection) */
  defaultDatasetId: string;
  /** Global schedule (can be overridden per collection) */
  defaultSchedule: string;
  /** Global timezone (can be overridden per collection) */
  defaultTimeZone: string;
  /** Collection configurations */
  collections: CollectionSyncConfig[];
}

/**
 * Parsed field definition with accessor function
 */
export interface ParsedFieldDefinition {
  key: string;
  type: string;
  formater?: string;
  accessor: (data: any) => any;
  method?: (value: any) => any;
}

/**
 * Runtime collection config with parsed fields
 */
export interface RuntimeCollectionConfig
  extends Omit<CollectionSyncConfig, "fields"> {
  fields: ParsedFieldDefinition[];
  trackerTableId: string;
  /** Regex pattern to match document paths */
  pathPattern: RegExp;
}
