import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { EventEmitter } from 'events';
import { L1CacheService, CacheItem, CacheStats as L1CacheStats } from './l1-cache.service';
import { L2CacheService, CacheStats as L2CacheStats } from './l2-cache.service';

export enum CacheStrategy {
  LOCAL = 'LOCAL',
  SHARED = 'SHARED',
  HYBRID = 'HYBRID',
}

export interface MultiLevelCacheConfig {
  strategy: CacheStrategy;
  l1Config?: {
    maxSize: number;
    defaultTtl: number;
    maxSizeBytes?: number;
  };
  l2Config?: {
    keyPrefix: string;
    defaultTtl: number;
    maxRetries: number;
  };
  invalidationDelay: number; // Delay for L1 invalidation after L2 update
  warmUpKeys?: Array<{ key: string; value: any; ttl?: number }>;
}

export interface CacheStats {
  l1: L1CacheStats;
  l2: L2CacheStats;
  combined: {
    hits: number;
    misses: number;
    hitRate: number;
    totalRequests: number;
    l1HitRate: number;
    l2HitRate: number;
  };
  crossLevelInvalidations: number;
}

export interface CacheResult<T = any> {
  value: T;
  source: 'L1' | 'L2' | 'MISS';
  responseTime: number;
}

@Injectable()
export class MultiLevelCacheService extends EventEmitter implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MultiLevelCacheService.name);
  private config: MultiLevelCacheConfig;
  private stats: CacheStats = {
    l1: {
      hits: 0,
      misses: 0,
      sets: 0,
      deletes: 0,
      evictions: 0,
      currentSize: 0,
      currentSizeBytes: 0,
      hitRate: 0,
      averageAccessCount: 0,
    },
    l2: {
      hits: 0,
      misses: 0,
      sets: 0,
      deletes: 0,
      errors: 0,
      hitRate: 0,
      averageResponseTime: 0,
    },
    combined: {
      hits: 0,
      misses: 0,
      hitRate: 0,
      totalRequests: 0,
      l1HitRate: 0,
      l2HitRate: 0,
    },
    crossLevelInvalidations: 0,
  };

  constructor(
    private readonly l1CacheService: L1CacheService,
    private readonly l2CacheService: L2CacheService,
  ) {
    this.config = {
      strategy: CacheStrategy.HYBRID,
      invalidationDelay: 100, // 100ms
      l1Config: {
        maxSize: 1000,
        defaultTtl: 300000, // 5 minutes
        maxSizeBytes: 50 * 1024 * 1024, // 50MB
      },
      l2Config: {
        keyPrefix: 'luminarytrade:',
        defaultTtl: 3600000, // 1 hour
        maxRetries: 3,
      },
    };
  }

  async onModuleInit() {
    this.logger.log('Initializing Multi-Level Cache Service...');
    
    // Subscribe to L2 cache events for cross-instance invalidation
    this.setupCrossInstanceInvalidation();
    
    // Warm up cache if configured
    if (this.config.warmUpKeys) {
      await this.warmUp();
    }
    
    this.logger.log(`Multi-Level Cache Service initialized with strategy: ${this.config.strategy}`);
  }

  /**
   * Get value from cache based on strategy
   */
  async get<T = any>(key: string): Promise<CacheResult<T>> {
    const startTime = Date.now();
    this.stats.combined.totalRequests++;
    
    try {
      switch (this.config.strategy) {
        case CacheStrategy.LOCAL:
          return await this.getFromLocal<T>(key, startTime);
          
        case CacheStrategy.SHARED:
          return await this.getFromShared<T>(key, startTime);
          
        case CacheStrategy.HYBRID:
          return await this.getFromHybrid<T>(key, startTime);
          
        default:
          return await this.getFromHybrid<T>(key, startTime);
      }
    } catch (error) {
      const responseTime = Date.now() - startTime;
      this.logger.error(`Cache get error for key ${key}:`, error);
      
      return {
        value: null,
        source: 'MISS',
        responseTime,
      };
    }
  }

  /**
   * Set value in cache based on strategy
   */
  async set<T = any>(key: string, value: T, ttl?: number): Promise<void> {
    const startTime = Date.now();
    
    try {
      switch (this.config.strategy) {
        case CacheStrategy.LOCAL:
          await this.setToLocal<T>(key, value, ttl);
          break;
          
        case CacheStrategy.SHARED:
          await this.setToShared<T>(key, value, ttl);
          break;
          
        case CacheStrategy.HYBRID:
          await this.setToHybrid<T>(key, value, ttl);
          break;
          
        default:
          await this.setToHybrid<T>(key, value, ttl);
      }
      
      const responseTime = Date.now() - startTime;
      this.logger.debug(`Cache SET: ${key} (${responseTime}ms)`);
    } catch (error) {
      this.logger.error(`Cache set error for key ${key}:`, error);
    }
  }

  /**
   * Delete from cache based on strategy
   */
  async delete(key: string): Promise<boolean> {
    const startTime = Date.now();
    
    try {
      let result = false;
      
      switch (this.config.strategy) {
        case CacheStrategy.LOCAL:
          result = await this.deleteFromLocal(key);
          break;
          
        case CacheStrategy.SHARED:
          result = await this.deleteFromShared(key);
          break;
          
        case CacheStrategy.HYBRID:
          result = await this.deleteFromHybrid(key);
          break;
          
        default:
          result = await this.deleteFromHybrid(key);
      }
      
      const responseTime = Date.now() - startTime;
      this.logger.debug(`Cache DELETE: ${key} (${responseTime}ms) - Success: ${result}`);
      
      return result;
    } catch (error) {
      this.logger.error(`Cache delete error for key ${key}:`, error);
      return false;
    }
  }

  /**
   * Clear cache based on strategy
   */
  async clear(): Promise<void> {
    try {
      switch (this.config.strategy) {
        case CacheStrategy.LOCAL:
          await this.l1CacheService.clear();
          break;
          
        case CacheStrategy.SHARED:
          await this.l2CacheService.clear();
          break;
          
        case CacheStrategy.HYBRID:
          await this.l1CacheService.clear();
          await this.l2CacheService.clear();
          break;
      }
      
      this.logger.debug('Cache cleared');
    } catch (error) {
      this.logger.error('Cache clear error:', error);
    }
  }

  /**
   * Get from local cache only
   */
  private async getFromLocal<T>(key: string, startTime: number): Promise<CacheResult<T>> {
    const value = this.l1CacheService.get<T>(key);
    const responseTime = Date.now() - startTime;
    
    if (value !== null) {
      this.stats.l1.hits++;
      this.stats.combined.hits++;
      return { value, source: 'L1', responseTime };
    } else {
      this.stats.l1.misses++;
      this.stats.combined.misses++;
      return { value: null, source: 'MISS', responseTime };
    }
  }

  /**
   * Get from shared cache only
   */
  private async getFromShared<T>(key: string, startTime: number): Promise<CacheResult<T>> {
    const value = await this.l2CacheService.get(key);
    const responseTime = Date.now() - startTime;
    
    if (value !== null) {
      this.stats.l2.hits++;
      this.stats.combined.hits++;
      return { value, source: 'L2', responseTime };
    } else {
      this.stats.l2.misses++;
      this.stats.combined.misses++;
      return { value: null, source: 'MISS', responseTime };
    }
  }

  /**
   * Get from hybrid cache (L1 first, then L2)
   */
  private async getFromHybrid<T>(key: string, startTime: number): Promise<CacheResult<T>> {
    // Try L1 first
    const l1Value = this.l1CacheService.get<T>(key);
    if (l1Value !== null) {
      this.stats.l1.hits++;
      this.stats.combined.hits++;
      return { value: l1Value, source: 'L1', responseTime: Date.now() - startTime };
    }
    
    // Try L2
    const l2Value = await this.l2CacheService.get(key);
    const responseTime = Date.now() - startTime;
    
    if (l2Value !== null) {
      // Store in L1 for future fast access
      this.l1CacheService.set(key, l2Value);
      this.stats.l2.hits++;
      this.stats.combined.hits++;
      return { value: l2Value, source: 'L2', responseTime };
    }
    
    // Miss in both
    this.stats.l1.misses++;
    this.stats.l2.misses++;
    this.stats.combined.misses += 2; // Both L1 and L2 missed
    return { value: null, source: 'MISS', responseTime };
  }

  /**
   * Set to local cache only
   */
  private async setToLocal<T>(key: string, value: T, ttl?: number): Promise<void> {
    this.l1CacheService.set(key, value, ttl);
    this.stats.l1.sets++;
  }

  /**
   * Set to shared cache only
   */
  private async setToShared<T>(key: string, value: T, ttl?: number): Promise<void> {
    await this.l2CacheService.set(key, JSON.stringify(value), ttl);
    this.stats.l2.sets++;
  }

  /**
   * Set to hybrid cache (both L1 and L2)
   */
  private async setToHybrid<T>(key: string, value: T, ttl?: number): Promise<void> {
    // Set in L2 for consistency across instances
    await this.l2CacheService.set(key, JSON.stringify(value), ttl);
    this.stats.l2.sets++;
    
    // Also set in L1 for fast local access
    this.l1CacheService.set(key, value, ttl);
    this.stats.l1.sets++;
  }

  /**
   * Delete from local cache only
   */
  private async deleteFromLocal(key: string): Promise<boolean> {
    const result = this.l1CacheService.delete(key);
    if (result) {
      this.stats.l1.deletes++;
    }
    return result;
  }

  /**
   * Delete from shared cache only
   */
  private async deleteFromShared(key: string): Promise<boolean> {
    const result = await this.l2CacheService.delete(key);
    if (result) {
      this.stats.l2.deletes++;
    }
    return result;
  }

  /**
   * Delete from hybrid cache (both L1 and L2)
   */
  private async deleteFromHybrid(key: string): Promise<boolean> {
    // Delete from L2
    const l2Result = await this.l2CacheService.delete(key);
    
    // Delete from L1
    const l1Result = this.l1CacheService.delete(key);
    
    if (l2Result || l1Result) {
      this.stats.l2.deletes++;
    }
    if (l1Result) {
      this.stats.l1.deletes++;
    }
    
    return l2Result || l1Result;
  }

  /**
   * Setup cross-instance invalidation
   */
  private setupCrossInstanceInvalidation(): void {
    // Subscribe to Redis pub/sub for invalidation events
    // This is a simplified implementation - in production you'd use Redis pub/sub
    
    // For now, we'll simulate invalidation events
    setInterval(() => {
      this.simulateInvalidationEvent();
    }, 30000); // Check every 30 seconds
  }

  /**
   * Simulate invalidation event from another instance
   */
  private simulateInvalidationEvent(): void {
    // Simulate receiving an invalidation for a key
    const invalidKeys = ['hot-key-1', 'hot-key-2'];
    
    for (const key of invalidKeys) {
      // Invalidate from L1
      this.l1CacheService.delete(key);
      
      // Update cross-level invalidation stats
      this.stats.crossLevelInvalidations++;
    }
    
    this.logger.debug(`Invalidated ${invalidKeys.length} keys from L1 due to cross-instance updates`);
    this.emit('invalidation', { keys: invalidKeys });
  }

  /**
   * Warm up cache with hot keys
   */
  private async warmUp(): Promise<void> {
    if (!this.config.warmUpKeys || this.config.warmUpKeys.length === 0) {
      return;
    }

    this.logger.log(`Warming up cache with ${this.config.warmUpKeys.length} hot keys`);
    const startTime = Date.now();
    
    // Warm up L1
    await this.l1CacheService.warmUp(this.config.warmUpKeys);
    
    // Warm up L2 if strategy includes it
    if (this.config.strategy === CacheStrategy.SHARED || this.config.strategy === CacheStrategy.HYBRID) {
      for (const item of this.config.warmUpKeys) {
        await this.l2CacheService.set(item.key, JSON.stringify(item.value), item.ttl);
      }
    }
    
    const duration = Date.now() - startTime;
    this.logger.log(`Cache warm-up completed in ${duration}ms`);
  }

  /**
   * Get comprehensive cache statistics
   */
  getStats(): CacheStats {
    // Update hit rates
    this.updateHitRates();
    
    return {
      l1: this.l1CacheService.getStats(),
      l2: this.l2CacheService.getStats(),
      combined: { ...this.stats.combined },
      crossLevelInvalidations: this.stats.crossLevelInvalidations,
    };
  }

  /**
   * Reset statistics
   */
  resetStats(): void {
    this.stats = {
      l1: {
        hits: 0,
        misses: 0,
        sets: 0,
        deletes: 0,
        evictions: 0,
        currentSize: 0,
        currentSizeBytes: 0,
        hitRate: 0,
        averageAccessCount: 0,
      },
      l2: {
        hits: 0,
        misses: 0,
        sets: 0,
        deletes: 0,
        errors: 0,
        hitRate: 0,
        averageResponseTime: 0,
      },
      combined: {
        hits: 0,
        misses: 0,
        hitRate: 0,
        totalRequests: 0,
        l1HitRate: 0,
        l2HitRate: 0,
      },
      crossLevelInvalidations: 0,
    };
    
    this.l1CacheService.resetStats();
    this.l2CacheService.resetStats();
  }

  /**
   * Update hit rates
   */
  private updateHitRates(): void {
    const l1Total = this.stats.l1.hits + this.stats.l1.misses;
    this.stats.l1.hitRate = l1Total > 0 ? (this.stats.l1.hits / l1Total) * 100 : 0;
    
    const l2Total = this.stats.l2.hits + this.stats.l2.misses;
    this.stats.l2.hitRate = l2Total > 0 ? (this.stats.l2.hits / l2Total) * 100 : 0;
    
    const combinedTotal = this.stats.combined.hits + this.stats.combined.misses;
    this.stats.combined.hitRate = combinedTotal > 0 ? (this.stats.combined.hits / combinedTotal) * 100 : 0;
    
    this.stats.combined.l1HitRate = combinedTotal > 0 ? (this.stats.l1.hits / combinedTotal) * 100 : 0;
    this.stats.combined.l2HitRate = combinedTotal > 0 ? (this.stats.l2.hits / combinedTotal) * 100 : 0;
  }

  /**
   * Configure cache settings
   */
  configure(config: Partial<MultiLevelCacheConfig>): void {
    this.config = { ...this.config, ...config };
    
    // Update L1 config
    if (config.l1Config) {
      this.l1CacheService.configure(config.l1Config);
    }
    
    // Update L2 config
    if (config.l2Config) {
      this.l2CacheService.configure(config.l2Config);
    }
    
    this.logger.log(`Multi-Level Cache configuration updated: ${JSON.stringify(this.config)}`);
  }

  /**
   * Check cache health
   */
  async isHealthy(): Promise<boolean> {
    const l1Healthy = this.l1CacheService.size() >= 0; // Basic health check
    const l2Healthy = await this.l2CacheService.isHealthy();
    
    return l1Healthy && l2Healthy;
  }

  async onModuleDestroy() {
    this.removeAllListeners();
    this.logger.log('Multi-Level Cache Service destroyed');
  }
}
