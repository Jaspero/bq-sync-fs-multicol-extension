import { BigQuery } from "@google-cloud/bigquery";
import * as admin from "firebase-admin";
import { getExtensions } from "firebase-admin/extensions";
import { getFirestore } from "firebase-admin/firestore";
import { DateTime } from "luxon";
import { CONFIG, getCollectionConfigs, findMatchingConfig } from "./config";
import { ChangeType } from "./types/change-type.enum";
import { RuntimeCollectionConfig } from "./types/sync-config.interface";
import { formatDocument } from "./utils/format-document";
import { pubsub, tasks, logger, firestore } from "firebase-functions/v1";

if (admin.apps.length === 0) {
  admin.initializeApp();
}

// Load all collection configurations
const collectionConfigs = getCollectionConfigs();

logger.info(`Loaded ${collectionConfigs.length} collection configurations`);

/**
 * Handles document write events for all collections
 */
async function handleDocumentWrite(
  change: any,
  ctx: any,
  fullPath: string
): Promise<void> {
  const config = findMatchingConfig(fullPath, collectionConfigs);

  if (!config) {
    logger.warn(`No matching config found for path: ${fullPath}`);
    return;
  }

  try {
    const existsBefore = change.before.exists;
    const existsAfter = change.after.exists;
    let changeType: ChangeType;

    if (!existsBefore && existsAfter) {
      changeType = ChangeType.CREATED;
    } else if (existsBefore && existsAfter) {
      changeType = ChangeType.UPDATED;
    } else if (existsBefore && !existsAfter) {
      changeType = ChangeType.DELETED;
    } else {
      return;
    }

    // Extract parent ID from path if applicable
    const pathParts = fullPath.split("/");
    let parentId: string | undefined;

    // Look for wildcard patterns in config paths to extract parentId
    for (const configPath of config.collectionPaths) {
      const match = configPath.match(/\{(\w+)\}/);
      if (match) {
        const configParts = configPath.split("/");
        const wildcardIndex = configParts.findIndex((p) => p.startsWith("{"));
        if (wildcardIndex !== -1 && pathParts[wildcardIndex]) {
          parentId = pathParts[wildcardIndex];
          break;
        }
      }
    }

    const docId = existsAfter ? change.after.id : change.before.id;

    // Only format document for create/update (when data exists)
    let data: any = {};
    if (changeType !== ChangeType.DELETED) {
      data = await formatDocument(
        {
          ...(change.after.data() || {}),
          ...(parentId && { parentId }),
        },
        docId,
        config
      );
    }

    const bq = new BigQuery();
    await bq
      .dataset(config.datasetId)
      .table(config.trackerTableId)
      .insert({
        changeType,
        timestamp: new Date().toISOString(),
        ...(changeType === ChangeType.DELETED
          ? {
              documentId:
                config.includeParentIdInDocumentId && parentId
                  ? `${parentId}-${docId}`
                  : docId,
            }
          : data),
      });

    logger.info(
      `Processed ${changeType} for ${fullPath} → ${config.datasetId}.${config.trackerTableId}`
    );
  } catch (e: any) {
    logger.error(`Error handling onWrite for path ${fullPath}`, e);
  }
}

/**
 * Single catch-all trigger for all document writes.
 * The trigger pattern is defined in extension.yaml as {collection}/{document=**}
 * This handles all collections and routes to the appropriate config.
 */
exports.fsExportToBqOnChange = firestore
  .document("{collection}/{document=**}")
  .onWrite(async (change, ctx) => {
    const fullPath = ctx.resource.name.split("/documents/")[1];
    await handleDocumentWrite(change, ctx, fullPath);
  });

/**
 * Sync tracker table data to main table for a single collection
 */
async function syncTrackerToMainTable(
  bq: BigQuery,
  fs: FirebaseFirestore.Firestore,
  config: RuntimeCollectionConfig
): Promise<void> {
  const settingsRef = fs
    .collection("bq-sync")
    .doc(`${CONFIG.instanceId}-${config.id}`);
  const doc = await settingsRef.get();
  const { lastRunDate } = doc.data() || {};

  const endTime = DateTime.now().toISO();
  const startTime = lastRunDate
    ? DateTime.fromISO(lastRunDate).toISO()
    : DateTime.now().minus({ years: 100 }).toISO();

  const fields = config.fields;
  const selection = fields
    .map(
      ({ key, type }) =>
        `ARRAY_AGG(${
          type === "ARRAY"
            ? `ARRAY_TO_STRING(\`${key}\`, ",")`
            : type === "JSON"
            ? `TO_JSON_STRING(\`${key}\`)`
            : `\`${key}\``
        } IGNORE NULLS ORDER BY timestamp DESC)[OFFSET(0)] AS \`${key}\``
    )
    .join(",");
  const update = fields
    .map(
      ({ key, type }) =>
        `\`${key}\` = IF(n.\`${key}\` is not NULL, ${
          type === "ARRAY"
            ? `SPLIT(n.\`${key}\`, ",")`
            : type === "JSON"
            ? `PARSE_JSON(n.\`${key}\`)`
            : `n.\`${key}\``
        }, i.\`${key}\`)`
    )
    .join(",");

  const query = `
    BEGIN TRANSACTION;

    CREATE TEMP TABLE tmp AS
      SELECT
        documentId,
        ${selection},
        (SELECT COUNTIF(changeType = 'CREATED') 
         FROM \`${config.datasetId}.${config.trackerTableId}\` sub
         WHERE sub.documentId = n.documentId
         AND sub.timestamp BETWEEN "${startTime}" AND "${endTime}") AS createdCount,
        (SELECT COUNTIF(changeType = 'DELETED') 
         FROM \`${config.datasetId}.${config.trackerTableId}\` sub
         WHERE sub.documentId = n.documentId
         AND sub.timestamp BETWEEN "${startTime}" AND "${endTime}") AS deletedCount
      FROM
        \`${config.datasetId}.${config.trackerTableId}\` n
      WHERE
        timestamp >= COALESCE((
          SELECT
            MAX(timestamp)
          FROM
            \`${config.datasetId}.${config.trackerTableId}\`
          WHERE
            documentId = n.documentId
            AND changeType = 'CREATED'
        ), TIMESTAMP('1970-01-01'))
      AND timestamp BETWEEN "${startTime}" AND "${endTime}"
      GROUP BY
        documentId;

    INSERT INTO \`${config.datasetId}.${config.tableId}\`
    SELECT
      documentId,
      ${fields
        .map(
          (f) =>
            `${
              f.type === "ARRAY"
                ? `SPLIT(\`${f.key}\`, ",")`
                : f.type === "JSON"
                ? `PARSE_JSON(\`${f.key}\`)`
                : `\`${f.key}\``
            } as \`${f.key}\``
        )
        .join(",")}
    FROM tmp
    WHERE 
      createdCount > deletedCount
      AND NOT EXISTS (
        SELECT 1
        FROM \`${config.datasetId}.${config.tableId}\` i
        WHERE i.documentId = tmp.documentId
      );

    DELETE \`${config.datasetId}.${config.tableId}\` i
    WHERE EXISTS
      (SELECT * from tmp as n
      WHERE i.documentId = n.documentId AND n.deletedCount > n.createdCount);
    
    UPDATE \`${config.datasetId}.${config.tableId}\` i
    SET ${update}
    FROM tmp n
    WHERE
      i.documentId = n.documentId;

    DROP TABLE tmp;
    COMMIT TRANSACTION;  
  `;

  const [job] = await bq.createQueryJob({
    query,
    location: config.datasetLocation,
  });

  await job.getQueryResults();

  await settingsRef.set(
    {
      lastRunDate: DateTime.now().toISO(),
    },
    { merge: true }
  );
}

/**
 * Scheduled function to sync all tracker tables to main tables
 */
exports.fsUpdatePrimaryTable = pubsub
  .schedule(CONFIG.schedule)
  .timeZone(CONFIG.timeZone)
  .onRun(async () => {
    const bq = new BigQuery();
    const fs = getFirestore();

    for (const config of collectionConfigs) {
      try {
        await syncTrackerToMainTable(bq, fs, config);
        logger.info(`Successfully synced ${config.id} to main table`);
      } catch (e: any) {
        logger.error(`Failed to sync ${config.id} to main table`, e);
      }
    }
  });

/**
 * Initialize BigQuery tables for a single collection
 */
async function initializeCollection(
  bq: BigQuery,
  fs: FirebaseFirestore.Firestore,
  config: RuntimeCollectionConfig
): Promise<void> {
  // Create dataset if needed
  try {
    await bq.createDataset(config.datasetId, {
      location: config.datasetLocation,
    });
    logger.info(`Created dataset ${config.datasetId}`);
  } catch (e: any) {
    if (e.code !== 409) {
      logger.warn(`Failed creating dataset ${config.datasetId}`, e);
    }
  }

  // Prepare field schema for tracker table (JSON stored as STRING for aggregation)
  const trackerFields = config.fields.map((f) => ({
    name: f.key,
    type: f.type === "ARRAY" || f.type === "JSON" ? "STRING" : f.type,
    mode: f.type === "ARRAY" ? "REPEATED" : "NULLABLE",
  }));

  // Prepare field schema for main table (uses native JSON type)
  const mainFields = config.fields.map((f) => ({
    name: f.key,
    type: f.type === "ARRAY" ? "STRING" : f.type,
    mode: f.type === "ARRAY" ? "REPEATED" : "NULLABLE",
  }));

  // Create tracker table
  try {
    await bq.dataset(config.datasetId).createTable(config.trackerTableId, {
      timePartitioning: {
        field: "timestamp",
        type: "HOUR",
        expirationMs: (1000 * 60 * 60 * 24 * 30).toString(),
      },
      schema: [
        { name: "changeType", type: "STRING", mode: "REQUIRED" },
        { name: "timestamp", type: "TIMESTAMP", mode: "REQUIRED" },
        { name: "documentId", type: "STRING", mode: "REQUIRED" },
        ...trackerFields,
      ],
    });
    logger.info(`Created tracker table ${config.trackerTableId}`);
  } catch (e: any) {
    if (e.code !== 409) {
      logger.warn(`Failed creating tracking table ${config.trackerTableId}`, e);
    }
  }

  // Create main table
  try {
    await bq.dataset(config.datasetId).createTable(config.tableId, {
      schema: [
        { name: "documentId", type: "STRING", mode: "REQUIRED" },
        ...mainFields,
      ],
    });
    logger.info(`Created main table ${config.tableId}`);
  } catch (e: any) {
    if (e.code !== 409) {
      logger.warn(`Failed creating table ${config.tableId}`, e);
    }
  }

  // Backfill if enabled
  if (config.backfill) {
    logger.info(`Starting backfill for ${config.id}`);
    const batchSize = 500;

    if (config.collectionGroup) {
      await backfillCollectionGroup(fs, batchSize, bq, config);
    } else {
      for (const path of config.collectionPaths) {
        if (!path.includes("{")) {
          await backfillCollection(fs, batchSize, bq, config, path);
        }
      }
    }
  }
}

/**
 * Initialization task that sets up BigQuery tables for all collections
 */
exports.initBigQuerySyncFirebase = tasks.taskQueue().onDispatch(async () => {
  logger.info(
    `Initializing BigQuery Sync for ${collectionConfigs.length} collections`
  );

  const bq = new BigQuery();
  const fs = getFirestore();

  for (const config of collectionConfigs) {
    try {
      await initializeCollection(bq, fs, config);
      logger.info(`Successfully initialized ${config.id}`);
    } catch (e: any) {
      logger.error(`Failed to initialize ${config.id}`, e);
    }
  }

  getExtensions()
    .runtime()
    .setProcessingState(
      "PROCESSING_COMPLETE",
      `Setup Successful for ${collectionConfigs.length} collections`
    );
});

async function backfillCollection(
  fs: FirebaseFirestore.Firestore,
  batchSize: number,
  bq: BigQuery,
  config: RuntimeCollectionConfig,
  path: string
): Promise<void> {
  logger.info(`Backfilling collection: ${path}`);
  let ref: any = null;
  let total = 0;

  do {
    let col: any = fs.collection(path);

    if (ref) {
      col = col.startAt(ref);
    }

    const { docs } = await col.limit(batchSize + 1).get();
    const rows: any[] = [];

    total += docs.length;

    await Promise.allSettled(
      docs.slice(0, batchSize).map(async (doc: any) => {
        const data = doc.data();
        const document: any = await formatDocument(data, doc.id, config);
        rows.push(document);
      })
    );

    if (!rows.length) {
      break;
    }

    ref = docs[batchSize];

    try {
      await bq.dataset(config.datasetId).table(config.tableId).insert(rows);
    } catch (e: any) {
      logger.write({
        severity: "ERROR",
        message: `Backfill errors for collection ${path}`,
        errors: e,
      });
    }
  } while (ref);

  logger.info(`Backfilled ${total} documents for collection ${path}`);
}

async function backfillCollectionGroup(
  fs: FirebaseFirestore.Firestore,
  batchSize: number,
  bq: BigQuery,
  config: RuntimeCollectionConfig
): Promise<void> {
  logger.info(`Backfilling collection group: ${config.collectionGroup}`);
  let ref: any = null;
  let total = 0;

  do {
    let col: any = fs.collectionGroup(config.collectionGroup!);

    if (ref) {
      col = col.startAt(ref);
    }

    const { docs } = await col.limit(batchSize + 1).get();
    const rows: any[] = [];

    total += docs.length;

    await Promise.allSettled(
      docs.slice(0, batchSize).map(async (doc: any) => {
        const data = doc.data();
        data.parentId = doc.ref.parent.parent?.id;
        const document: any = await formatDocument(data, doc.id, config);
        rows.push(document);
      })
    );

    if (!rows.length) {
      break;
    }

    ref = docs[batchSize];

    try {
      await bq.dataset(config.datasetId).table(config.tableId).insert(rows);
    } catch (e: any) {
      logger.write({
        severity: "ERROR",
        message: `Backfill errors for collection group ${config.collectionGroup}`,
        errors: e,
      });
    }
  } while (ref);

  logger.info(
    `Backfilled ${total} documents for collection group ${config.collectionGroup}`
  );
}
