import { Injectable, Logger, OnModuleInit } from '@nestjs/common';

export interface CacheItem<T = any> {
  key: string;
  value: T;
  timestamp: number;
  ttl: number;
  accessCount: number;
  lastAccessed: number;
  size: number;
}

export interface L1CacheConfig {
  maxSize: number;
  defaultTtl: number;
  cleanupInterval: number;
  maxSizeBytes?: number;
}

export interface CacheStats {
  hits: number;
  misses: number;
  sets: number;
  deletes: number;
  evictions: number;
  currentSize: number;
  currentSizeBytes: number;
  hitRate: number;
  averageAccessCount: number;
}

@Injectable()
export class L1CacheService implements OnModuleInit {
  private readonly logger = new Logger(L1CacheService.name);
  private cache: Map<string, CacheItem> = new Map();
  private accessOrder: string[] = [];
  private config: L1CacheConfig;
  private stats: CacheStats = {
    hits: 0,
    misses: 0,
    sets: 0,
    deletes: 0,
    evictions: 0,
    currentSize: 0,
    currentSizeBytes: 0,
    hitRate: 0,
    averageAccessCount: 0,
  };
  private cleanupTimer?: NodeJS.Timeout;

  constructor() {
    this.config = {
      maxSize: 1000,
      defaultTtl: 300000, // 5 minutes
      cleanupInterval: 60000, // 1 minute
      maxSizeBytes: 100 * 1024 * 1024, // 100MB
    };
  }

  async onModuleInit() {
    this.logger.log('Initializing L1 Cache Service...');
    this.startCleanupTimer();
    this.logger.log(`L1 Cache Service initialized with max size: ${this.config.maxSize}`);
  }

  /**
   * Get value from cache
   */
  get<T = any>(key: string): T | null {
    const item = this.cache.get(key);
    
    if (!item) {
      this.stats.misses++;
      this.updateHitRate();
      return null;
    }

    // Check if item has expired
    if (Date.now() > item.timestamp + item.ttl) {
      this.cache.delete(key);
      this.removeFromAccessOrder(key);
      this.stats.misses++;
      this.updateHitRate();
      return null;
    }

    // Update access information
    item.lastAccessed = Date.now();
    item.accessCount++;
    this.moveToFrontOfAccessOrder(key);
    
    this.stats.hits++;
    this.updateHitRate();
    
    this.logger.debug(`L1 Cache HIT: ${key}`);
    return item.value;
  }

  /**
   * Set value in cache
   */
  set<T = any>(key: string, value: T, ttl?: number): void {
    const now = Date.now();
    const itemTtl = ttl || this.config.defaultTtl;
    const size = this.calculateSize(value);

    // Check if we need to evict items
    while (this.cache.size >= this.config.maxSize || 
           this.getCurrentSizeBytes() + size > (this.config.maxSizeBytes || Infinity)) {
      this.evictLRU();
    }

    const item: CacheItem<T> = {
      key,
      value,
      timestamp: now,
      ttl: itemTtl,
      accessCount: 1,
      lastAccessed: now,
      size,
    };

    this.cache.set(key, item);
    this.addToAccessOrder(key);
    
    this.stats.sets++;
    this.stats.currentSize = this.cache.size;
    this.stats.currentSizeBytes = this.getCurrentSizeBytes();
    
    this.logger.debug(`L1 Cache SET: ${key} (size: ${size} bytes)`);
  }

  /**
   * Delete value from cache
   */
  delete(key: string): boolean {
    const item = this.cache.get(key);
    if (!item) {
      return false;
    }

    this.cache.delete(key);
    this.removeFromAccessOrder(key);
    
    this.stats.deletes++;
    this.stats.currentSize = this.cache.size;
    this.stats.currentSizeBytes = this.getCurrentSizeBytes();
    
    this.logger.debug(`L1 Cache DELETE: ${key}`);
    return true;
  }

  /**
   * Clear all cache items
   */
  clear(): void {
    const size = this.cache.size;
    this.cache.clear();
    this.accessOrder = [];
    
    this.stats.currentSize = 0;
    this.stats.currentSizeBytes = 0;
    
    this.logger.debug(`L1 Cache CLEAR: ${size} items removed`);
  }

  /**
   * Check if key exists
   */
  has(key: string): boolean {
    const item = this.cache.get(key);
    if (!item) {
      return false;
    }

    // Check if item has expired
    if (Date.now() > item.timestamp + item.ttl) {
      this.cache.delete(key);
      this.removeFromAccessOrder(key);
      this.stats.currentSize = this.cache.size;
      this.stats.currentSizeBytes = this.getCurrentSizeBytes();
      return false;
    }

    return true;
  }

  /**
   * Get multiple keys
   */
  mget<T = any>(keys: string[]): (T | null)[] {
    return keys.map(key => this.get<T>(key));
  }

  /**
   * Set multiple keys
   */
  mset<T = any>(items: Array<{ key: string; value: T; ttl?: number }>): void {
    for (const item of items) {
      this.set(item.key, item.value, item.ttl);
    }
  }

  /**
   * Get cache statistics
   */
  getStats(): CacheStats {
    this.updateAverageAccessCount();
    return { ...this.stats };
  }

  /**
   * Reset statistics
   */
  resetStats(): void {
    this.stats = {
      hits: 0,
      misses: 0,
      sets: 0,
      deletes: 0,
      evictions: 0,
      currentSize: this.cache.size,
      currentSizeBytes: this.getCurrentSizeBytes(),
      hitRate: 0,
      averageAccessCount: 0,
    };
  }

  /**
   * Get cache size
   */
  size(): number {
    return this.cache.size;
  }

  /**
   * Get all keys
   */
  keys(): string[] {
    return Array.from(this.cache.keys());
  }

  /**
   * Get items matching pattern
   */
  async getKeysByPattern(pattern: string): Promise<string[]> {
    const regex = new RegExp(pattern.replace(/\*/g, '.*'));
    return Array.from(this.cache.keys()).filter(key => regex.test(key));
  }

  /**
   * Warm up cache with hot keys
   */
  async warmUp(hotKeys: Array<{ key: string; value: any; ttl?: number }>): Promise<void> {
    this.logger.log(`Warming up L1 cache with ${hotKeys.length} hot keys`);
    
    for (const item of hotKeys) {
      this.set(item.key, item.value, item.ttl);
    }
    
    this.logger.log(`L1 cache warm-up completed. Current size: ${this.cache.size}`);
  }

  /**
   * Evict least recently used item
   */
  private evictLRU(): void {
    if (this.accessOrder.length === 0) {
      return;
    }

    const lruKey = this.accessOrder[this.accessOrder.length - 1];
    const item = this.cache.get(lruKey);
    
    if (item) {
      this.cache.delete(lruKey);
      this.accessOrder.pop();
      
      this.stats.evictions++;
      this.stats.currentSize = this.cache.size;
      this.stats.currentSizeBytes = this.getCurrentSizeBytes();
      
      this.logger.debug(`L1 Cache EVICT: ${lruKey} (accessed ${item.accessCount} times)`);
    }
  }

  /**
   * Move key to front of access order
   */
  private moveToFrontOfAccessOrder(key: string): void {
    this.removeFromAccessOrder(key);
    this.accessOrder.unshift(key);
  }

  /**
   * Remove key from access order
   */
  private removeFromAccessOrder(key: string): void {
    const index = this.accessOrder.indexOf(key);
    if (index > -1) {
      this.accessOrder.splice(index, 1);
    }
  }

  /**
   * Add key to access order
   */
  private addToAccessOrder(key: string): void {
    if (!this.accessOrder.includes(key)) {
      this.accessOrder.unshift(key);
    }
  }

  /**
   * Calculate approximate size of value
   */
  private calculateSize(value: any): number {
    if (value === null || value === undefined) {
      return 0;
    }
    
    try {
      return JSON.stringify(value).length * 2; // Rough estimate
    } catch {
      return 100; // Default size for non-serializable objects
    }
  }

  /**
   * Get current cache size in bytes
   */
  private getCurrentSizeBytes(): number {
    let totalSize = 0;
    for (const item of this.cache.values()) {
      totalSize += item.size;
    }
    return totalSize;
  }

  /**
   * Update hit rate
   */
  private updateHitRate(): void {
    const total = this.stats.hits + this.stats.misses;
    this.stats.hitRate = total > 0 ? (this.stats.hits / total) * 100 : 0;
  }

  /**
   * Update average access count
   */
  private updateAverageAccessCount(): void {
    if (this.cache.size === 0) {
      this.stats.averageAccessCount = 0;
      return;
    }

    let totalAccessCount = 0;
    for (const item of this.cache.values()) {
      totalAccessCount += item.accessCount;
    }

    this.stats.averageAccessCount = totalAccessCount / this.cache.size;
  }

  /**
   * Clean up expired items
   */
  private cleanupExpired(): void {
    const now = Date.now();
    const expiredKeys: string[] = [];

    for (const [key, item] of this.cache.entries()) {
      if (now > item.timestamp + item.ttl) {
        expiredKeys.push(key);
      }
    }

    for (const key of expiredKeys) {
      this.cache.delete(key);
      this.removeFromAccessOrder(key);
    }

    if (expiredKeys.length > 0) {
      this.stats.currentSize = this.cache.size;
      this.stats.currentSizeBytes = this.getCurrentSizeBytes();
      this.logger.debug(`L1 Cache cleanup: removed ${expiredKeys.length} expired items`);
    }
  }

  /**
   * Start cleanup timer
   */
  private startCleanupTimer(): void {
    this.cleanupTimer = setInterval(() => {
      this.cleanupExpired();
    }, this.config.cleanupInterval);
  }

  /**
   * Stop cleanup timer
   */
  private stopCleanupTimer(): void {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = undefined;
    }
  }

  /**
   * Configure cache settings
   */
  configure(config: Partial<L1CacheConfig>): void {
    this.config = { ...this.config, ...config };
    this.logger.log(`L1 Cache configuration updated: ${JSON.stringify(this.config)}`);
  }

  async onModuleDestroy() {
    this.stopCleanupTimer();
    this.logger.log('L1 Cache Service destroyed');
  }
}
