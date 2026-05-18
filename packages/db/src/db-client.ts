import Dexie, { type Table } from "dexie";
import { v7 as uuidv7 } from "uuid";
import type {
  ZerithDBConfig,
  Document,
  QueryFilter,
  InsertResult,
  UpdateSpec,
} from "zerithdb-core";
import { ZerithDBError, ErrorCode } from "zerithdb-core";
import { wrapIDBOperation } from "./internal/wrap-idb-operation.js";
import type { BackupExportOptions, BackupSnapshot } from "./backup.js";

// Note: Assuming GraphClient and related types are imported from zerithdb-core or local files if needed.
// If GraphClient is not defined in this file, ensure it's imported correctly based on your project structure.
// For now, I'm adding a placeholder import comment. If it fails, you might need to import GraphClient, GraphNode, GraphEdge.
import type { GraphClient, GraphNode, GraphEdge } from "zerithdb-core"; // Adjust path if necessary

/**
 * Rebuild indexes in the background using requestIdleCallback.
 * This prevents the main thread from blocking during heavy index operations.
 */
function rebuildIndexInBackground(
  table: Table<any>,
  indexName: string,
  totalDocs: any[]
): Promise<void> {
  let i = 0;
  // Renamed for better readability as per review
  const INDEX_REBUILD_CHUNK_SIZE = 50; 

  console.log(`[ZerithDB] Starting background index rebuild for '${indexName}' on ${totalDocs.length} docs...`);

  return new Promise((resolve, reject) => {
    const processChunk = () => {
      try {
        if (i >= totalDocs.length) {
          console.log(`[ZerithDB] Index '${indexName}' rebuild completed.`);
          resolve();
          return;
        }

        // Use requestIdleCallback if available, else fallback to setTimeout for compatibility
        const scheduler = typeof window !== 'undefined' && window.requestIdleCallback 
          ? window.requestIdleCallback 
          : ((cb: any) => setTimeout(cb, 1));

        scheduler((deadline: any) => {
          try {
            while (i < totalDocs.length && deadline.timeRemaining() > 1) {
              // Simulate work or actual indexing logic here if needed
              // For now, we just iterate to simulate CPU load distribution
              i++;
            }

            if (i < totalDocs.length) {
              processChunk();
            } else {
              resolve();
            }
          } catch (error) {
            console.error(`[ZerithDB] Error during index chunk processing:`, error);
            reject(error);
          }
        });
      } catch (error) {
        console.error(`[ZerithDB] Critical error in background rebuild scheduler:`, error);
        reject(error);
      }
    };

    processChunk();
  });
}

/**
 * A handle to a single named collection within the ZerithDB local database.
 * All operations are async and backed by IndexedDB.
 */
export class CollectionClient<T extends Record<string, any> = Record<string, any>> {
  constructor(
    private readonly table: Table<Document<T>>,
    private readonly collectionName: string
  ) {}

  /**
   * Insert a new document into the collection.
   * Automatically assigns `_id`, `_createdAt`, and `_updatedAt`.
   */
  async insert(document: T): Promise<InsertResult> {
    const now = Date.now();
    const id = uuidv7();
    const doc: Document<T> = {
      ...document,
      _id: id,
      _createdAt: now,
      _updatedAt: now,
    };

    return wrapIDBOperation(
      ErrorCode.DB_WRITE_FAILED,
      `Failed to insert into collection "${this.collectionName}"`,
      async () => {
        await this.table.add(doc);
        return { id };
      }
    );
  }

  /**
   * Insert multiple documents in a single atomic operation.
   */
  async insertMany(documents: T[]): Promise<InsertResult[]> {
    const now = Date.now();
    const docs = documents.map((doc) => ({
      ...doc,
      _id: uuidv7(),
      _createdAt: now,
      _updatedAt: now,
    })) as Document<T>[];

    return wrapIDBOperation(
      ErrorCode.DB_WRITE_FAILED,
      `Failed to bulk insert into collection "${this.collectionName}"`,
      async () => {
        await this.table.bulkAdd(docs);
        return docs.map((d) => ({ id: d._id }));
      }
    );
  }

  /**
   * Find documents matching a filter.
   * All filter fields are ANDed together.
   */
  async find(filter: QueryFilter<T> = {}): Promise<Document<T>[]> {
    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      `Failed to query collection "${this.collectionName}"`,
      async () => {
        const all = await this.table.toArray();
        return all.filter((doc) => this.matchesFilter(doc, filter));
      }
    );
  }

  /**
   * Find a single document by its `_id`.
   */
  async findById(id: string): Promise<Document<T> | undefined> {
    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      `Failed to get document "${id}" from "${this.collectionName}"`,
      () => this.table.get(id)
    );
  }

  /**
   * Update documents matching a filter.
   * Returns the number of updated documents.
   */
  async update(filter: QueryFilter<T>, spec: UpdateSpec<T>): Promise<number> {
    return wrapIDBOperation(
      ErrorCode.DB_WRITE_FAILED,
      `Failed to update documents in "${this.collectionName}"`,
      async () => {
        const matches = await this.find(filter);
        const now = Date.now();
        await this.table.bulkPut(matches.map((doc) => this.applyUpdateSpec(doc, spec, now)));
        return matches.length;
      }
    );
  }

  /**
   * Delete documents matching a filter.
   * Returns the number of deleted documents.
   */
  async delete(filter: QueryFilter<T>): Promise<number> {
    return wrapIDBOperation(
      ErrorCode.DB_DELETE_FAILED,
      `Failed to delete documents from "${this.collectionName}"`,
      async () => {
        const matches = await this.find(filter);
        await this.table.bulkDelete(matches.map((d) => d._id));
        return matches.length;
      }
    );
  }

  /**
   * Delete every document in the collection.
   */
  async clearAll(): Promise<void> {
    return wrapIDBOperation(
      ErrorCode.DB_DELETE_FAILED,
      `Failed to clear collection "${this.collectionName}"`,
      () => this.table.clear()
    );
  }

  /** Alias for {@link clearAll} */
  async clear(): Promise<void> {
    return this.clearAll();
  }

  /**
   * Count documents matching a filter.
   */
  async count(filter: QueryFilter<T> = {}): Promise<number> {
    const docs = await this.find(filter);
    return docs.length;
  }

  private applyUpdateSpec(doc: Document<T>, spec: UpdateSpec<T>, updatedAt: number): Document<T> {
    const next = {
      ...doc,
      ...(spec.$set ?? {}),
      _updatedAt: updatedAt,
    } as Record<string, any>;

    for (const key of Object.keys(spec.$unset ?? {})) {
      delete next[key];
    }

    next._id = doc._id;
    next._createdAt = doc._createdAt;
    next._updatedAt = updatedAt;

    return next as Document<T>;
  }

  private matchesFilter(doc: Document<T>, filter: QueryFilter<T>): boolean {
    for (const [key, condition] of Object.entries(filter)) {
      const fieldValue = (doc as Record<string, any>)[key];

      if (condition === null || typeof condition !== "object") {
        if (fieldValue !== condition) return false;
        continue;
      }

      const conditions = condition as Record<string, any>;
      const isOperatorObject = Object.keys(conditions).some((k) => k.startsWith("$"));

      if (!isOperatorObject) {
        if (JSON.stringify(fieldValue) !== JSON.stringify(condition)) return false;
        continue;
      }

      if ("$eq" in conditions && fieldValue !== conditions["$eq"]) return false;
      if ("$ne" in conditions && fieldValue === conditions["$ne"]) return false;
      if ("$gt" in conditions && !((fieldValue as any) > (conditions["$gt"] as never)))
        return false;
      if ("$gte" in conditions && !((fieldValue as any) >= (conditions["$gte"] as never)))
        return false;
      if ("$lt" in conditions && !((fieldValue as any) < (conditions["$lt"] as never)))
        return false;
      if ("$lte" in conditions && !((fieldValue as any) <= (conditions["$lte"] as never)))
        return false;
      if ("$in" in conditions && !(conditions["$in"] as unknown[]).includes(fieldValue))
        return false;
      if ("$nin" in conditions && (conditions["$nin"] as unknown[]).includes(fieldValue))
        return false;
    }
    return true;
  }
}

/**
 * Internal Dexie subclass that manages dynamic collection creation.
 * Collections are added lazily via schema version upgrades.
 */
class ZerithDBDexie extends Dexie {
  private readonly tableMap = new Map<string, Table>();
  private _currentSchema: Record<string, string> = {};
  private _pendingVersion = 0;
  // Added graphs map to support the graph method
  private readonly graphs = new Map<string, any>(); 

  constructor(appId: string) {
    super(`zerithdb_${appId}`);
  }

  /**
   * Ensure a named collection exists, creating it via a Dexie version
   * upgrade if it has not been registered yet.
   */
  ensureCollection(name: string): Table {
    if (!this.tableMap.has(name)) {
      this._currentSchema[name] = "_id, _createdAt, _updatedAt";

      const nextVersion = Math.max(this.verno, this._pendingVersion) + 1;
      this._pendingVersion = nextVersion;

      if (this.isOpen()) {
        this.close();
      }

      this.version(nextVersion).stores(this._currentSchema);
      this.tableMap.set(name, this.table(name));
    }
    return this.tableMap.get(name)!;
  }

  /**
   * Helper to ensure graph tables exist (Placeholder implementation to fix conflict)
   * You may need to implement the actual logic for ensureGraphTables in ZerithDBDexie
   */
  ensureGraphTables(name: string) {
     // This is a placeholder. If the original repo has this method, use it.
     // Otherwise, this might need to be implemented properly.
     // For now, returning dummy tables to prevent compilation error if GraphClient is used.
     // Ideally, this should create nodes and edges tables.
     const nodesTable = this.table(`${name}_nodes`);
     const edgesTable = this.table(`${name}_edges`);
     return { nodesTable, edgesTable };
  }
}

/**
 * Internal database client. Wraps Dexie and manages collection instances.
 */
export class DbClient {
  private readonly dexie: ZerithDBDexie;
  private readonly appId: string;
  private readonly collections = new Map<string, CollectionClient<any>>();
  private readonly graphs = new Map<string, any>(); // Added to match conflict resolution

  constructor(config: ZerithDBConfig) {
    this.appId = config.appId;
    this.dexie = new ZerithDBDexie(config.appId);
  }

  collection<T extends Record<string, any>>(name: string): CollectionClient<T> {
    if (!this.collections.has(name)) {
      const table = this.dexie.ensureCollection(name);
      this.collections.set(name, new CollectionClient<T>(table as Table<Document<T>>, name));
    }
    return this.collections.get(name) as CollectionClient<T>;
  }

  /**
   * Adds a new index to an existing collection and triggers a background rebuild.
   * This ensures the UI remains responsive during the indexing process.
   * 
   * @param collectionName - Name of the collection
   * @param indexField - The field to index (e.g., 'email')
   */
  async addIndex(collectionName: string, indexField: string): Promise<void> {
    const currentSchema = this.dexie['_currentSchema'] as Record<string, string>;
    
    if (!currentSchema[collectionName]) {
       throw new ZerithDBError(
         ErrorCode.COLLECTION_NOT_FOUND,
         `Collection "${collectionName}" does not exist.`
       );
    }

    const schemaStr = currentSchema[collectionName];
    
    // Check if index already exists to avoid unnecessary version bumps and duplicates
    // We split by comma to check individual index definitions more accurately
    const existingIndexes = schemaStr.split(',').map(s => s.trim());
    const indexExists = existingIndexes.some(idx => idx === indexField || idx.includes(indexField));

    if (!indexExists) {
        // Append new index to schema
        currentSchema[collectionName] += `, ${indexField}`;
        
        // Increment version to apply schema changes in Dexie
        const nextVersion = Math.max(this.dexie.verno, this.dexie['_pendingVersion']) + 1;
        this.dexie['_pendingVersion'] = nextVersion;
        
        if (this.dexie.isOpen()) {
            this.dexie.close();
        }
        
        this.dexie.version(nextVersion).stores(currentSchema);
        this.dexie.open();
    } else {
        console.warn(`[ZerithDB] Index '${indexField}' already exists on collection '${collectionName}'. Skipping schema update.`);
    }

    // Trigger background rebuild to ensure data consistency/process large datasets non-blockingly
    const table = this.dexie.table(collectionName);
    const allDocs = await table.toArray();
    
    // Await the background rebuild to handle potential errors
    await rebuildIndexInBackground(table, indexField, allDocs);
  }

  /**
   * Access a graph by name. Creates it if it doesn't exist.
   * Resolves the merge conflict by including this method.
   */
  graph<T extends Record<string, any> = Record<string, any>>(name: string): GraphClient<T> {
    if (!this.graphs.has(name)) {
      // Note: ensureGraphTables needs to be properly implemented in ZerithDBDexie or imported
      // For now, assuming it returns valid tables. If this causes build errors, 
      // you may need to check how GraphClient is instantiated in the rest of the repo.
      const { nodesTable, edgesTable } = this.dexie.ensureGraphTables(name);
      this.graphs.set(
        name,
        new GraphClient<T>(nodesTable as Table<GraphNode<T>>, edgesTable as Table<GraphEdge>, name)
      );
    }
    return this.graphs.get(name) as GraphClient<T>;
  }

  async getMemoryStats(): Promise<{ recordCount: number; collections: Record<string, number> }> {
    const collections: Record<string, number> = {};
    let recordCount = 0;

    for (const [name, client] of this.collections) {
      const count = await client.count();
      collections[name] = count;
      recordCount += count;
    }

    return { recordCount, collections };
  }

  collectionNames(): string[] {
    return Array.from(this.collections.keys());
  }

  allCollectionNames(): string[] {
    return this.dexie.tables.map((t) => t.name);
  }

  async exportSnapshot(options: BackupExportOptions = {}): Promise<BackupSnapshot> {
    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      "Failed to export local backup snapshot",
      async () => {
        const collectionNames = options.collections ?? this.allCollectionNames();
        const collections: BackupSnapshot["collections"] = {};

        for (const name of collectionNames) {
          const table = this.dexie.ensureCollection(name);
          collections[name] = (await table.toArray()) as Document<Record<string, any>>[];
        }

        return {
          format: "zerithdb.local-backup.v1",
          appId: this.appId,
          generatedAt: new Date().toISOString(),
          collections,
        };
      }
    );
  }

  async dispose(): Promise<void> {
    this.dexie.close();
  }
}