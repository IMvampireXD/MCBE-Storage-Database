import * as mc from "@minecraft/server";

class JSONStorage {

    constructor(key, chunkSize = 32000) {
        this.key = key;
        this.chunkSize = chunkSize;
    }

    #getChunkKey(index) { return `${this.key}:${index}`; }

    #getCountKey() { return `${this.key}.count`; }

    save(data) {
        const jsonString = JSON.stringify(data);
        const chunks = this.#splitIntoChunks(jsonString);
        mc.world.setDynamicProperty(this.#getCountKey(), chunks.length);
        chunks.forEach((chunk, index) => {
            mc.world.setDynamicProperty(this.#getChunkKey(index), chunk);
        });
        const oldCount = mc.world.getDynamicProperty(this.#getCountKey()) || 0;
        if (oldCount > chunks.length) {
            for (let i = chunks.length; i < oldCount; i++) {
                mc.world.setDynamicProperty(this.#getChunkKey(i), undefined);
            }
        }
        return data;
    }
    load() {
        const count = mc.world.getDynamicProperty(this.#getCountKey());
        if (count === undefined || count === 0) {
            return "";
        }

        const chunks = [];
        for (let i = 0; i < count; i++) {
            const chunk = mc.world.getDynamicProperty(this.#getChunkKey(i));
            if (typeof chunk !== 'string') {
                console.warn(`JSONStorage: Missing chunk at index ${i}`);
                return "";
            }
            chunks.push(chunk);
        }

        try {
            return chunks.length ? JSON.parse(chunks.join('')) : "";
        } catch (e) {
            console.error(`JSONStorage: Failed to parse JSON:`, e);
            return "";
        }
    }
    clear() {
        const count = mc.world.getDynamicProperty(this.#getCountKey()) || 0;

        for (let i = 0; i < count; i++) {
            mc.world.setDynamicProperty(this.#getChunkKey(i), undefined);
        }
        mc.world.setDynamicProperty(this.#getCountKey(), undefined);
    }
    access() {
        const idx = mc.world.getDynamicProperty(`${this.key}.idx`);
        if (idx !== undefined) {
            const strings = [];
            for (let i = 0; i < idx; i++) {
                strings.push(mc.world.getDynamicProperty(`${this.key}:${i}`));
            }
            const type = mc.world.getDynamicProperty(`${this.key}.type`);
            const combinedString = strings.join("");
            return type === 'json' ? JSON.parse(combinedString) : combinedString;
        }
        return mc.world.getDynamicProperty(this.key);
    }

    clearChunkData() {
        mc.world.setDynamicProperty(`${this.key}.idx`, undefined);
        mc.world.setDynamicProperty(`${this.key}.type`, undefined);
        let i = 0;
        while (mc.world.getDynamicProperty(`${this.key}:${i}`) !== undefined) {
            mc.world.setDynamicProperty(`${this.key}:${i}`, undefined);
            i++;
        }
    }

    #splitIntoChunks(str) {
        const chunks = [];
        const numChunks = Math.ceil(str.length / this.chunkSize);

        for (let i = 0; i < numChunks; i++) {
            chunks.push(str.slice(i * this.chunkSize, (i + 1) * this.chunkSize));
        }
        return chunks;
    }
}

export { JSONStorage }
