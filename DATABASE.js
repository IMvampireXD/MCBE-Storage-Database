import { World, world, Entity, system } from "@minecraft/server";

const DB_INSTANCES = new WeakMap();

export class Database {
    static MAX_CHUNK_SIZE = 32767;
    static INTERNAL_DB_PREFIX = "§▌";
    static KEY_TYPE_NATIVE = "N_";
    static KEY_TYPE_STRING = "S_";
    static KEY_TYPE_META_CHUNK = "M_";
    static KEY_TYPE_DATA_CHUNK = "D_";

    /** @private */ _storageSource;
    /** @private */ _databaseId;
    /** @private */ _fullPrefixBase;
    /** @private */ _dataCache = new Map();
    /** @private */ _knownKeys = new Set();
    /** @private */ _autoCache = true;

    /**
     * @param {string} id 
     * @param {World|Entity} [source=world]
     */
    constructor(id, source = world) {
        if (DB_INSTANCES.get(source)?.has(id)) return DB_INSTANCES.get(source).get(id);
        
        this._databaseId = id;
        this._storageSource = source;
        this._fullPrefixBase = `${Database.INTERNAL_DB_PREFIX}${id}_`;
        this._initializeFromProperties();

        if (!DB_INSTANCES.has(source)) DB_INSTANCES.set(source, new Map());
        DB_INSTANCES.get(source).set(id, this);
    }

    /** @private */
    _initializeFromProperties() {
        const props = this._storageSource.getDynamicPropertyIds().filter(id => 
            id.startsWith(this._fullPrefixBase));
        const keyRegex = new RegExp(`${this._fullPrefixBase}(?:${Database.KEY_TYPE_NATIVE}|${Database.KEY_TYPE_STRING}|${Database.KEY_TYPE_META_CHUNK})(.+)|${this._fullPrefixBase}${Database.KEY_TYPE_DATA_CHUNK}(.+?)_\\d+$`);
        
        props.forEach(prop => {
            const match = prop.match(keyRegex);
            if (match) this._knownKeys.add(match[1] || match[2]);
        });
    }

    /** @private */
    _buildKey(key, type, index) {
        return type === Database.KEY_TYPE_DATA_CHUNK 
            ? `${this._fullPrefixBase}${type}${key}_${index}`
            : `${this._fullPrefixBase}${type}${key}`;
    }

    /** @private */
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

    /**
     * @param {string} key
     * @param {any} value
     * @returns {Database}
     */
    set(key, value) {
        if (typeof key !== 'string') return this;
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
            const chunkCount = Math.ceil(processed.length / Database.MAX_CHUNK_SIZE);
            this._storageSource.setDynamicProperty(this._buildKey(key, Database.KEY_TYPE_META_CHUNK), chunkCount);
            for (let i = 0; i < chunkCount; i++) {
                const chunk = processed.substr(i * Database.MAX_CHUNK_SIZE, Database.MAX_CHUNK_SIZE);
                this._storageSource.setDynamicProperty(this._buildKey(key, Database.KEY_TYPE_DATA_CHUNK, i), chunk);
            }
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
     * @param {string} key
     * @returns {any}
     */
    get(key) {
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

    /** @private */
    _cacheValue(key, value) {
        if (this._autoCache) this._dataCache.set(key, value);
        return value;
    }

    /** @private */
    _isVector(value) {
        return value && ['x','y','z'].every(k => typeof value[k] === 'number') && 
            Object.keys(value).length === 3;
    }

    /** @private */
    _checkKeyExists(key) {
        return [Database.KEY_TYPE_NATIVE, Database.KEY_TYPE_STRING, Database.KEY_TYPE_META_CHUNK].some(t =>
            this._storageSource.getDynamicProperty(this._buildKey(key, t)) !== undefined
        );
    }

    /**
     * @param {string} key
     * @returns {boolean}
     */
    delete(key) {
        const exists = this.has(key);
        this._deleteKeyChunks(key);
        this._dataCache.delete(key);
        this._knownKeys.delete(key);
        return exists;
    }

    clear() {
        this._storageSource.getDynamicPropertyIds()
            .filter(id => id.startsWith(this._fullPrefixBase))
            .forEach(id => this._storageSource.setDynamicProperty(id));
        this._dataCache.clear();
        this._knownKeys.clear();
    }

    /**
     * @param {string} key
     * @returns {boolean}
     */
    has(key) {
        return this._dataCache.has(key) || this._knownKeys.has(key) || this._checkKeyExists(key);
    }

    *keys() {
        yield* this._knownKeys.values();
    }

    *values() {
        for (const key of this.keys()) yield this.get(key);
    }

    *entries() {
        for (const key of this.keys()) yield [key, this.get(key)];
    }

    [Symbol.iterator]() {
        return this.entries();
    }

    get size() {
        return this._knownKeys.size;
    }

    /**
     * @param {string} key
     */
    load(key) {
        if (!this._dataCache.has(key)) this.get(key);
    }

    /**
     * @param {string} key
     */
    unload(key) {
        this._dataCache.delete(key);
    }

    enableCache() { this._autoCache = true; }
    disableCache() { this._autoCache = false; }

    async getAsync(key) {
        return new Promise(resolve => 
            system.run(() => resolve(this.get(key)))
        );
    }

    async setAsync(key, value) {
        return new Promise(resolve =>
            system.run(() => {
                this.set(key, value);
                resolve();
            })
        );
    }
}
