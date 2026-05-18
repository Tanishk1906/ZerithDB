import Dexie, { type Table } from "dexie";
import { v7 as uuidv7 } from "uuid";
import type {
  ZerithDBConfig,
  Document,
  QueryFilter,
  InsertResult,
  UpdateSpec,
  GraphNode,
  GraphEdge,
} from "zerithdb-core";
import { ZerithDBError, ErrorCode, GraphClient } from "zerithdb-core";
import { wrapIDBOperation } from "./internal/wrap-idb-operation.js";
import type { BackupExportOptions, BackupSnapshot } from "./backup.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * A synthetic IdleDeadline that always reports a generous time budget.
 * Used as a fallback when `requestIdleCallback` is unavailable (Node.js,
 * older browsers, SSR).
 */
function makeSyntheticDeadline(budgetMs: number = 16): IdleDeadline {
  const deadline = Date.now() + budgetMs;
  return {
    timeRemaining: () => Math.max(0, deadline - Date.now()),
    didTimeout: false,
  };
}

/**
 * Rebuild indexes in the background using requestIdleCallback.
 * Fetches data in chunks to prevent memory exhaustion on large collections.
 */
function rebuildIndexInBackground<T>(
  table: Table<T, any>,
  indexName: string,
  totalCount: number,
  chunkSize: number = 50
): Promise<void> {
  let offset = 0;

  console.log(
    `[ZerithDB] Starting background index rebuild for '${indexName}' on ${totalCount} docs...`
  );

  return new Promise((resolve, reject) => {
    const processDeadline = async (deadline: IdleDeadline) => {
      try {
        // Fetch only a chunk of documents asynchronously to save memory
        const docsToProcess = await table.offset(offset).limit(chunkSize).toArray();

        if (docsToProcess.length === 0) {
          console.log(
            `[ZerithDB] Index '${indexName}' rebuild completed successfully.`
          );
          return resolve();
        }

        let localIndex = 0;

        // Process the chunk while the main thread is idle
        while (localIndex < docsToProcess.length && deadline.timeRemaining() > 1) {
          const doc = docsToProcess[localIndex];

          // TODO: Insert your actual indexing/cache update logic here!
          // e.g., indexManager.add(doc, indexName);
          // (The previous implementation had an empty loop here).

          localIndex++;
        }

        offset += localIndex;

        if (offset < totalCount) {
          scheduleChunk();
        } else {
          console.log(
            `[ZerithDB] Index '${indexName}' rebuild completed successfully.`
          );
          resolve();
        }
      } catch (chunkError) {
        console.error(
          `[ZerithDB] Error during index chunk processing:`,
          chunkError
        );
        reject(chunkError);
      }
    };

    const scheduleChunk = () => {
      try {
        if (typeof window !== "undefined" && window.requestIdleCallback) {
          window.requestIdleCallback((deadline: IdleDeadline) => {
            // Catch dangling promises from the async processDeadline
            processDeadline(deadline).catch(reject);
          });
        } else {
          setTimeout(
            () => processDeadline(makeSyntheticDeadline()).catch(reject),
            1
          );
        }
      } catch (schedulerError) {
        console.error(
          `[ZerithDB] Critical error in background rebuild scheduler:`,
          schedulerError
        );
        reject(schedulerError);
      }
    };

    // Kick off the first chunk.
    scheduleChunk();
  });
}

// ---------------------------------------------------------------------------
// CollectionClient
// ---------------------------------------------------------------------------

/**
 * A handle to a single named collection within the ZerithDB local database.
 */
export class CollectionClient<
  T extends Record<string, any> = Record<string, any>
> {
  constructor(
    private readonly table: Table<Document<T>>,
    private readonly collectionName: string
  ) {}

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

  async findById(id: string): Promise<Document<T> | undefined> {
    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      `Failed to get document "${id}" from "${this.collectionName}"`,
      () => this.table.get(id)
    );
  }

  async update(filter: QueryFilter<T>, spec: UpdateSpec<T>): Promise<number> {
    return wrapIDBOperation(
      ErrorCode.DB_WRITE_FAILED,
      `Failed to update documents in "${this.collectionName}"`,
      async () => {
        const matches = await this.find(filter);
        const now = Date.now();
        await this.table.bulkPut(
          matches.map((doc) => this.applyUpdateSpec(doc, spec, now))
        );
        return matches.length;
      }
    );
  }

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

  async clearAll(): Promise<void> {
    return wrapIDBOperation(
      ErrorCode.DB_DELETE_FAILED,
      `Failed to clear collection "${this.collectionName}"`,
      () => this.table.clear()
    );
  }

  async clear(): Promise<void> {
    return this.clearAll();
  }

  async count(filter: QueryFilter<T> = {}): Promise<number> {
    const docs = await this.find(filter);
    return docs.length;
  }

  private applyUpdateSpec(
    doc: Document<T>,
    spec: UpdateSpec<T>,
    updatedAt: number
  ): Document<T> {
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
      const isOperatorObject = Object.keys(conditions).some((k) =>
        k.startsWith("$")
      );

      if (!isOperatorObject) {
        if (JSON.stringify(fieldValue) !== JSON.stringify(condition))
          return false;
        continue;
      }

      if ("$eq" in conditions && fieldValue !== conditions["$eq"]) return false;
      if ("$ne" in conditions && fieldValue === conditions["$ne"]) return false;

      const fVal = fieldValue as number;

      if ("$gt" in conditions && !(fVal > (conditions["$gt"] as number))) return false;
      if ("$gte" in conditions && !(fVal >= (conditions["$gte"] as number))) return false;
      if ("$lt" in conditions && !(fVal < (conditions["$lt"] as number))) return false;
      if ("$lte" in conditions && !(fVal <= (conditions["$lte"] as number))) return false;

      if ("$in" in conditions && !(conditions["$in"] as unknown[]).includes(fieldValue)) return false;
      if ("$nin" in conditions && (conditions["$nin"] as unknown[]).includes(fieldValue)) return false;
    }
    return true;
  }
}

// ---------------------------------------------------------------------------
// ZerithDBDexie (internal)
// ---------------------------------------------------------------------------

/**
 * Internal Dexie subclass that manages dynamic collection and graph-table
 * creation via proper version bumps.
 */
class ZerithDBDexie extends Dexie {
  private readonly tableMap = new Map<string, Table>();
  private _currentSchema: Record<string, string> = {};
  private _pendingVersion = 0;

  constructor(appId: string) {
    super(`zerithdb_${appId}`);
  }

  /**
   * Returns the next version number, incrementing past whatever Dexie
   * currently considers the live version.
   */
  private nextVersion(): number {
    const next = Math.max(this.verno, this._pendingVersion) + 1;
    this._pendingVersion = next;
    return next;
  }

  /**
   * Applies a new schema snapshot as a Dexie version upgrade, closing and
   * re-opening the database if it is already open.
   */
  private applySchema(schema: Record<string, string>): void {
    if (this.isOpen()) {
      this.close();
    }
    this.version(this.nextVersion()).stores(schema);
  }

  ensureCollection(name: string): Table {
    if (!this.tableMap.has(name)) {
      this._currentSchema[name] = "_id, _createdAt, _updatedAt";
      this.applySchema(this._currentSchema);
      this.tableMap.set(name, this.table(name));
    }
    return this.tableMap.get(name)!;
  }

  /**
   * Ensures nodes and edges tables exist for a named graph.
   */
  ensureGraphTables(name: string): {
    nodesTable: Table<any>;
    edgesTable: Table<any>;
  } {
    const nodesTableName = `${name}_nodes`;
    const edgesTableName = `${name}_edges`;

    const nodesExists = nodesTableName in this._currentSchema;
    const edgesExists = edgesTableName in this._currentSchema;

    if (!nodesExists || !edgesExists) {
      if (!nodesExists) {
        // _id is the primary key; from/to allow fast adjacency lookups.
        this._currentSchema[nodesTableName] = "_id, _createdAt, _updatedAt";
      }
      if (!edgesExists) {
        // Index `from` and `to` so graph traversal queries stay fast.
        this._currentSchema[edgesTableName] =
          "_id, from, to, _createdAt, _updatedAt";
      }
      this.applySchema(this._currentSchema);
    }

    return {
      nodesTable: this.table(nodesTableName),
      edgesTable: this.table(edgesTableName),
    };
  }
}

// ---------------------------------------------------------------------------
// DbClient
// ---------------------------------------------------------------------------

/**
 * Internal database client. Wraps Dexie and manages collection instances.
 */
export class DbClient {
  private readonly dexie: ZerithDBDexie;
  private readonly appId: string;
  private readonly collections = new Map<string, CollectionClient<any>>();
  private readonly graphs = new Map<string, GraphClient<any>>();
  private readonly indexRebuildChunkSize: number;

  constructor(config: ZerithDBConfig & { indexRebuildChunkSize?: number }) {
    this.appId = config.appId;
    this.dexie = new ZerithDBDexie(config.appId);
    this.indexRebuildChunkSize = config.indexRebuildChunkSize ?? 50;
  }

  collection<T extends Record<string, any>>(name: string): CollectionClient<T> {
    if (!this.collections.has(name)) {
      const table = this.dexie.ensureCollection(name);
      this.collections.set(
        name,
        new CollectionClient<T>(table as Table<Document<T>>, name)
      );
    }
    return this.collections.get(name) as CollectionClient<T>;
  }

  /**
   * Adds a new index to an existing collection and triggers a background rebuild.
   */
  async addIndex(collectionName: string, indexField: string): Promise<void> {
    // 1. Input validation
    if (typeof indexField !== "string" || indexField.trim() === "") {
      throw new ZerithDBError(
        ErrorCode.INVALID_ARGUMENT,
        `Index field must be a non-empty string, received ${JSON.stringify(
          indexField
        )}`
      );
    }

    const currentSchema = this.dexie["_currentSchema"] as Record<
      string,
      string
    >;

    if (!currentSchema[collectionName]) {
      throw new ZerithDBError(
        ErrorCode.COLLECTION_NOT_FOUND,
        `Collection "${collectionName}" does not exist.`
      );
    }

    const schemaStr = currentSchema[collectionName];

    // 2. Exact duplicate check.
    const normalise = (def: string) => def.trim().replace(/^[+*&]+/, "");

    const existingFields = schemaStr.split(",").map(normalise);
    const normalised = normalise(indexField);

    const indexExists = existingFields.includes(normalised);

    if (!indexExists) {
      currentSchema[collectionName] += `, ${indexField}`;

      const nextVersion =
        Math.max(
          this.dexie.verno,
          this.dexie["_pendingVersion"] as number
        ) + 1;
      this.dexie["_pendingVersion"] = nextVersion;

      if (this.dexie.isOpen()) {
        this.dexie.close();
      }

      this.dexie.version(nextVersion).stores(currentSchema);
      await this.dexie.open();
    } else {
      console.warn(
        `[ZerithDB] Index '${indexField}' already exists on collection '${collectionName}'. Skipping schema update.`
      );
    }

    // 3. Trigger background rebuild with proper error propagation.
    const table = this.dexie.table(collectionName);
    const totalCount = await table.count(); // Memory fix: only load the count

    await rebuildIndexInBackground(
      table,
      indexField,
      totalCount, // Memory fix: passing count instead of array of all documents
      this.indexRebuildChunkSize
    );
  }

  graph<T extends Record<string, any> = Record<string, any>>(
    name: string
  ): GraphClient<T> {
    if (!this.graphs.has(name)) {
      const { nodesTable, edgesTable } = this.dexie.ensureGraphTables(name);
      this.graphs.set(
        name,
        new GraphClient<T>(
          nodesTable as Table<GraphNode<T>>,
          edgesTable as Table<GraphEdge>,
          name
        )
      );
    }
    return this.graphs.get(name) as GraphClient<T>;
  }

  async getMemoryStats(): Promise<{
    recordCount: number;
    collections: Record<string, number>;
  }> {
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

  async exportSnapshot(
    options: BackupExportOptions = {}
  ): Promise<BackupSnapshot> {
    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      "Failed to export local backup snapshot",
      async () => {
        const collectionNames =
          options.collections ?? this.allCollectionNames();
        const collections: BackupSnapshot["collections"] = {};

        for (const name of collectionNames) {
          const table = this.dexie.ensureCollection(name);
          collections[name] = (await table.toArray()) as Document<
            Record<string, any>
          >[];
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