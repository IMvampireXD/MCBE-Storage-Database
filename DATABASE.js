import { world, system } from "@minecraft/server";

const DB_INSTANCES = new WeakMap();

/**
 * A Storage Database class for Minecraft Bedrock Script API.
 * Splits JSON data if reaches 32k byte limit of dynamic properties.
 */
export class Database {
    /**@private */ static MAX_CHUNK_SIZE = 32767;
    
    /**@private */ static INTERNAL_DB_PREFIX = "§▌";
    /**@private */ static KEY_TYPE_NATIVE = "N_";
    /**@private */ static KEY_TYPE_STRING = "S_";
    /**@private */ static KEY_TYPE_META_CHUNK = "M_";
    /**@private */ static KEY_TYPE_DATA_CHUNK = "D_";

    /**@private */ _storageSource;
    /**@private */ _databaseId;
    /**@private */ _fullPrefixBase;
    /**@private */ _dataCache = new Map();
    /**@private */ _knownKeys = new Set();
    /**@private */ _autoCache = true;
    
    /**
     * Create a new database instance
     * @param {string} id - Unique database identifier
     * @param {World|Entity} [source=world] - Storage target (world or entity)
     */
    constructor(id, source = world) {
        if (typeof id !== "string" || !id.trim()) throw new Error("Invalid database ID");
        if (DB_INSTANCES.get(source)?.has(id)) return DB_INSTANCES.get(source).get(id);
        
        this._databaseId = id;
        this._storageSource = source;
        this._fullPrefixBase = `${Database.INTERNAL_DB_PREFIX}${id}_`;
        this._initializeFromProperties();

        if (!DB_INSTANCES.has(source)) DB_INSTANCES.set(source, new Map());
        DB_INSTANCES.get(source).set(id, this);
    }

    /**
     * Store a value in the database
     * @param {string} key - Key to store under
     * @param {any} value - Value to store
     * @returns {Database} Self for chaining
     */
    set(key, value) {
        if (typeof key !== 'string' || !key.trim()) throw new Error("Key must be a non-empty string");
        this._deleteKeyChunks(key);
        this._dataCache.delete(key);

        if (value === undefined) {
            this._knownKeys.delete(key);
            return this;
        }

        let processed = value;
        if (typeof value === 'object' && !this._isVector(value)) {
            processed = JSON.stringify(value);
        }

        if (typeof processed === 'string' && processed.length > Database.MAX_CHUNK_SIZE) {
            const chunks = this._splitIntoChunks(processed);
            this._storageSource.setDynamicProperty(this._buildKey(key, Database.KEY_TYPE_META_CHUNK), chunks.length);
            chunks.forEach((chunk, i) => {
                this._storageSource.setDynamicProperty(this._buildKey(key, Database.KEY_TYPE_DATA_CHUNK, i), chunk);
            });
        } else if (this._isVector(value)) {
            this._storageSource.setDynamicProperty(this._buildKey(key, Database.KEY_TYPE_NATIVE), value);
        } else {
            this._storageSource.setDynamicProperty(this._buildKey(key, 
                typeof processed === 'string' ? Database.KEY_TYPE_STRING : Database.KEY_TYPE_NATIVE), processed);
        }

        this._knownKeys.add(key);
        if (this._autoCache) this._dataCache.set(key, value);
        return this;
    }
    
    /**
     * Retrieve a stored value
     * @param {string} key - Key to retrieve
     * @returns {any} Stored value or undefined
     */
    get(key) {
        if (typeof key !== 'string' || !key.trim()) throw new Error("Key must be a non-empty string");
        if (this._dataCache.has(key)) return this._dataCache.get(key);
        if (!this._knownKeys.has(key) && !this._checkKeyExists(key)) return;

        const nativeVal = this._storageSource.getDynamicProperty(this._buildKey(key, Database.KEY_TYPE_NATIVE));
        if (nativeVal !== undefined) return this._cacheValue(key, nativeVal);

        let stringVal = this._storageSource.getDynamicProperty(this._buildKey(key, Database.KEY_TYPE_STRING));
        if (stringVal === undefined) {
            const chunkCount = this._storageSource.getDynamicProperty(this._buildKey(key, Database.KEY_TYPE_META_CHUNK));
            if (typeof chunkCount === 'number') {
                stringVal = '';
                for (let i = 0; i < chunkCount; i++) {
                    stringVal += this._storageSource.getDynamicProperty(this._buildKey(key, Database.KEY_TYPE_DATA_CHUNK, i));
                }
            }
        }

        try {
            return this._cacheValue(key, stringVal ? JSON.parse(stringVal) : undefined);
        } catch {
            return this._cacheValue(key, stringVal);
        }
    }
    
    /**
     * Delete a key and its associated data
     * @param {string} key - Key to delete
     * @returns {boolean} True if key existed
     */
    deleteKey(key) {
        if (typeof key !== 'string' || !key.trim()) throw new Error("Key must be a non-empty string");
        const exists = this.has(key);
        this._deleteKeyChunks(key);
        this._dataCache.delete(key);
        this._knownKeys.delete(key);
        return exists;
    }
    
    /**
     * Delete value inside key.
     * @param {string} key - The key which the data was saved in.
     */
    deleteValue(key) {
        this.set(key, undefined);
    }

    /** Clear all database entries */
    clear() {
        this._storageSource.getDynamicPropertyIds()
            .filter(id => id.startsWith(this._fullPrefixBase))
            .forEach(id => this._storageSource.setDynamicProperty(id));
        this._dataCache.clear();
        this._knownKeys.clear();
    }

    /**
     * Check if key exists
     * @param {string} key - Key to check
     * @returns {boolean} - Existence status
     */
    has(key) {
        return this._dataCache.has(key) || this._knownKeys.has(key) || this._checkKeyExists(key);
    }

    /** Iterate over all keys */
    *keys() {
        yield* this._knownKeys.values();
    }

    /** Iterate over all values */
    *values() {
        for (const key of this.keys()) yield this.get(key);
    }

    /** Iterate over key-value pairs */
    *entries() {
        for (const key of this.keys()) yield [key, this.get(key)];
    }

    [Symbol.iterator]() {
        return this.entries();
    }

    /** Get total stored keys count */
    get size() {
        return this._knownKeys.size;
    }
    
    /**
     * Preload value into cache
     * @param {string} key - Key to load
     */
    load(key) {
        if (!this._dataCache.has(key)) this.get(key);
    }
    
    /**
     * Remove value from cache
     * @param {string} key - Key to unload
     */
    unload(key) {
        this._dataCache.delete(key);
    }
    
    /**
     * Asynchronously get value
     * @async
     * @param {string} key - Key to retrieve
     */
    async getAsync(key) {
        return new Promise(resolve => 
            system.run(() => resolve(this.get(key)))
        );
    }
    
    /**
     * Asynchronously set value
     * @async
     * @param {string} key - Key to store
     * @param {any} value - Value to store
     */
    async setAsync(key, value) {
        return new Promise(resolve =>
            system.run(() => {
                this.set(key, value);
                resolve();
            })
        );
    }

    enableCache() { this._autoCache = true; }
    disableCache() { this._autoCache = false; }

    /**@private */
    _cacheValue(key, value) {
        if (this._autoCache) this._dataCache.set(key, value);
        return value;
    }

    /**@private */
    _isVector(value) {
        return value && ['x','y','z'].every(k => typeof value[k] === 'number') && 
            Object.keys(value).length === 3;
    }

    /**@private */
    _checkKeyExists(key) {
        return [Database.KEY_TYPE_NATIVE, Database.KEY_TYPE_STRING, Database.KEY_TYPE_META_CHUNK].some(t =>
            this._storageSource.getDynamicProperty(this._buildKey(key, t)) !== undefined
        );
    }

    /**@private */
    _initializeFromProperties() {
        const props = this._storageSource.getDynamicPropertyIds().filter(id => 
            id.startsWith(this._fullPrefixBase));
        const keyRegex = new RegExp(`${this._fullPrefixBase}(?:${Database.KEY_TYPE_NATIVE}|${Database.KEY_TYPE_STRING}|${Database.KEY_TYPE_META_CHUNK})(.+)|${this._fullPrefixBase}${Database.KEY_TYPE_DATA_CHUNK}(.+?)_\\d+$`);
        
        props.forEach(prop => {
            const match = prop.match(keyRegex);
            if (match) this._knownKeys.add(match[1] || match[2]);
        });
    }

    /**@private */
    _splitIntoChunks(str) {
        const chunks = [];
        for (let i = 0; i < str.length; i += Database.MAX_CHUNK_SIZE) {
            chunks.push(str.substring(i, i + Database.MAX_CHUNK_SIZE));
        }
        return chunks;
    }

    /**@private */
    _buildKey(key, type, index) {
        return type === Database.KEY_TYPE_DATA_CHUNK 
            ? `${this._fullPrefixBase}${type}${key}_${index}`
            : `${this._fullPrefixBase}${type}${key}`;
    }

    /**@private */
    _deleteKeyChunks(key) {
        [Database.KEY_TYPE_NATIVE, Database.KEY_TYPE_STRING, Database.KEY_TYPE_META_CHUNK].forEach(t => 
            this._storageSource.setDynamicProperty(this._buildKey(key, t)));
        
        const chunkCount = this._storageSource.getDynamicProperty(this._buildKey(key, Database.KEY_TYPE_META_CHUNK));
        if (typeof chunkCount === 'number') {
            for (let i = 0; i < chunkCount; i++) {
                this._storageSource.setDynamicProperty(this._buildKey(key, Database.KEY_TYPE_DATA_CHUNK, i));
            }
        }
    }
}
