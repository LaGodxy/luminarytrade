import { Injectable, Inject, Logger, OnModuleInit } from '@nestjs/common';
import { CACHE_MANAGER, Cache } from '@nestjs/cache-manager';
import { CacheMetricsService } from './cache-metrics.service';
import { L1CacheService } from './l1-cache.service';
import { L2CacheService } from './l2-cache.service';
import { MultiLevelCacheService, CacheStrategy } from './multi-level-cache.service';

export interface CacheOptions {
  ttl?: number; // Time to live in seconds
  namespace?: string; // Cache key namespace
  level?: 'L1' | 'L2' | 'HYBRID'; // Cache level to use
  strategy?: CacheStrategy; // Cache strategy (LOCAL, SHARED, HYBRID)
}

@Injectable()
export class CacheManager implements OnModuleInit {
  private readonly logger = new Logger(CacheManager.name);
  private multiLevelCacheEnabled = false;

  constructor(
    @Inject(CACHE_MANAGER) private readonly cacheManager: Cache,
    private readonly metricsService: CacheMetricsService,
    @Inject(L1CacheService) private readonly l1CacheService: L1CacheService,
    @Inject(L2CacheService) private readonly l2CacheService: L2CacheService,
    @Inject(MultiLevelCacheService) private readonly multiLevelCacheService: MultiLevelCacheService,
  ) {}

  async onModuleInit() {
    // Check if multi-level cache services are available
    this.multiLevelCacheEnabled = !!(this.l1CacheService && this.l2CacheService && this.multiLevelCacheService);
    
    if (this.multiLevelCacheEnabled) {
      this.logger.log('Multi-level cache system enabled');
    } else {
      this.logger.log('Using standard cache manager');
    }
  }

  /**
   * Generate a cache key with optional namespace
   */
  generateKey(key: string, namespace?: string): string {
    return namespace ? `${namespace}:${key}` : key;
  }

  /**
   * Get value from cache (with multi-level support)
   */
  async get<T>(key: string, options?: CacheOptions): Promise<T | undefined> {
    const fullKey = this.generateKey(key, options?.namespace);
    
    // Use multi-level cache if enabled and requested
    if (this.multiLevelCacheEnabled && (options?.level || options?.strategy)) {
      return await this.getFromMultiLevel<T>(fullKey, options);
    }
    
    // Fallback to standard cache
    return await this.getFromStandard<T>(fullKey);
  }

  /**
   * Set value in cache (with multi-level support)
   */
  async set<T>(key: string, value: T, options?: CacheOptions): Promise<void> {
    const fullKey = this.generateKey(key, options?.namespace);
    
    // Use multi-level cache if enabled and requested
    if (this.multiLevelCacheEnabled && (options?.level || options?.strategy)) {
      await this.setToMultiLevel(fullKey, value, options);
      return;
    }
    
    // Fallback to standard cache
    await this.setToStandard(fullKey, value, options);
  }

  /**
   * Delete value from cache (with multi-level support)
   */
  async del(key: string, options?: CacheOptions): Promise<void> {
    const fullKey = this.generateKey(key, options?.namespace);
    
    // Use multi-level cache if enabled and requested
    if (this.multiLevelCacheEnabled && (options?.level || options?.strategy)) {
      await this.deleteFromMultiLevel(fullKey, options);
      return;
    }
    
    // Fallback to standard cache
    await this.deleteFromStandard(fullKey);
  }

  /**
   * Delete all keys matching a pattern (with multi-level support)
   */
  async delPattern(pattern: string, options?: CacheOptions): Promise<void> {
    // Use multi-level cache if enabled and requested
    if (this.multiLevelCacheEnabled && (options?.level || options?.strategy)) {
      await this.deletePatternFromMultiLevel(pattern, options);
      return;
    }
    
    // Fallback to standard cache
    await this.deletePatternFromStandard(pattern);
  }

  /**
   * Clear all cache (with multi-level support)
   */
  async reset(options?: CacheOptions): Promise<void> {
    // Use multi-level cache if enabled and requested
    if (this.multiLevelCacheEnabled && (options?.level || options?.strategy)) {
      await this.multiLevelCacheService.clear();
      this.logger.log('Multi-level cache cleared');
      return;
    }
    
    // Fallback to standard cache
    await this.cacheManager.reset();
    this.logger.log('Standard cache cleared');
  }

  /**
   * Get or set pattern - fetch from cache or execute function and cache result (with multi-level support)
   */
  async getOrSet<T>(
    key: string,
    factory: () => Promise<T>,
    options?: CacheOptions,
  ): Promise<T> {
    const cached = await this.get<T>(key, options);
    
    if (cached !== undefined && cached !== null) {
      return cached;
    }

    const value = await factory();
    await this.set(key, value, options);
    
    return value;
  }

  /**
   * Get from multi-level cache
   */
  private async getFromMultiLevel<T>(key: string, options?: CacheOptions): Promise<T | undefined> {
    try {
      const result = await this.multiLevelCacheService.get<T>(key);
      
      if (result.value !== null) {
        this.metricsService.recordHit(key);
        this.logger.debug(`Multi-level Cache HIT: ${key} from ${result.source}`);
        return result.value;
      } else {
        this.metricsService.recordMiss(key);
        this.logger.debug(`Multi-level Cache MISS: ${key}`);
        return undefined;
      }
    } catch (error) {
      this.logger.error(`Multi-level cache get error for key ${key}:`, error);
      this.metricsService.recordMiss(key);
      return undefined;
    }
  }

  /**
   * Set to multi-level cache
   */
  private async setToMultiLevel<T>(key: string, value: T, options?: CacheOptions): Promise<void> {
    try {
      const ttl = options?.ttl ? options.ttl * 1000 : undefined; // Convert to milliseconds
      
      // Configure strategy if provided
      if (options?.strategy) {
        this.multiLevelCacheService.configure({ strategy: options.strategy });
      }
      
      await this.multiLevelCacheService.set(key, value, ttl);
      this.logger.debug(`Multi-level Cache SET: ${key} (TTL: ${options?.ttl || 'default'}s)`);
    } catch (error) {
      this.logger.error(`Multi-level cache set error for key ${key}:`, error);
    }
  }

  /**
   * Delete from multi-level cache
   */
  private async deleteFromMultiLevel(key: string, options?: CacheOptions): Promise<void> {
    try {
      await this.multiLevelCacheService.delete(key);
      this.logger.debug(`Multi-level Cache DEL: ${key}`);
    } catch (error) {
      this.logger.error(`Multi-level cache delete error for key ${key}:`, error);
    }
  }

  /**
   * Delete pattern from multi-level cache
   */
  private async deletePatternFromMultiLevel(pattern: string, options?: CacheOptions): Promise<void> {
    try {
      // Get keys matching pattern
      const keys = await this.multiLevelCacheService.getKeys?.(pattern) || [];
      
      // Delete each key
      for (const key of keys) {
        await this.multiLevelCacheService.delete(key);
      }
      
      this.logger.debug(`Multi-level Cache DEL pattern: ${pattern} (${keys.length} keys)`);
    } catch (error) {
      this.logger.error(`Multi-level cache delete pattern error for ${pattern}:`, error);
    }
  }

  /**
   * Get from standard cache
   */
  private async getFromStandard<T>(key: string): Promise<T | undefined> {
    try {
      const value = await this.cacheManager.get<T>(key);
      
      if (value !== undefined && value !== null) {
        this.metricsService.recordHit(key);
        this.logger.debug(`Standard Cache HIT: ${key}`);
      } else {
        this.metricsService.recordMiss(key);
        this.logger.debug(`Standard Cache MISS: ${key}`);
      }
      
      return value;
    } catch (error) {
      this.logger.error(`Standard cache get error for key ${key}:`, error);
      this.metricsService.recordMiss(key);
      return undefined;
    }
  }

  /**
   * Set to standard cache
   */
  private async setToStandard<T>(key: string, value: T, options?: CacheOptions): Promise<void> {
    try {
      const ttl = options?.ttl ? options.ttl * 1000 : undefined; // Convert to milliseconds
      await this.cacheManager.set(key, value, ttl);
      this.logger.debug(`Standard Cache SET: ${key} (TTL: ${options?.ttl || 'default'}s)`);
    } catch (error) {
      this.logger.error(`Standard cache set error for key ${key}:`, error);
    }
  }

  /**
   * Delete from standard cache
   */
  private async deleteFromStandard(key: string): Promise<void> {
    try {
      await this.cacheManager.del(key);
      this.logger.debug(`Standard Cache DEL: ${key}`);
    } catch (error) {
      this.logger.error(`Standard cache delete error for key ${key}:`, error);
    }
  }

  /**
   * Delete pattern from standard cache
   */
  private async deletePatternFromStandard(pattern: string): Promise<void> {
    try {
      const store = this.cacheManager.store;
      
      // For memory cache, we need to get all keys and filter
      if (typeof store.keys === 'function') {
        const keys = await store.keys();
        const matchingKeys = keys.filter((key: string) => 
          this.matchPattern(key, pattern)
        );
        
        await Promise.all(
          matchingKeys.map((key: string) => this.cacheManager.del(key))
        );
        
        this.logger.debug(`Standard Cache DEL pattern: ${pattern} (${matchingKeys.length} keys)`);
      }
    } catch (error) {
      this.logger.error(`Standard cache delete pattern error for ${pattern}:`, error);
    }
  }

  /**
   * Simple pattern matching for cache keys
   */
  private matchPattern(key: string, pattern: string): boolean {
    const regexPattern = pattern
      .replace(/\*/g, '.*')
      .replace(/\?/g, '.');
    
    return new RegExp(`^${regexPattern}$`).test(key);
  }

  /**
   * Warm cache with data (with multi-level support)
   */
  async warm<T>(key: string, factory: () => Promise<T>, options?: CacheOptions): Promise<void> {
    try {
      const value = await factory();
      
      // Use multi-level cache if enabled and requested
      if (this.multiLevelCacheEnabled && (options?.level || options?.strategy)) {
        const ttl = options?.ttl ? options?.ttl * 1000 : undefined;
        await this.multiLevelCacheService.set(key, value, ttl);
        this.logger.log(`Multi-level cache warmed: ${key}`);
        return;
      }
      
      // Fallback to standard cache
      await this.setToStandard(key, value, options);
      this.logger.log(`Standard cache warmed: ${key}`);
    } catch (error) {
      this.logger.error(`Cache warming error for ${key}:`, error);
    }
  }

  /**
   * Get cache statistics (combining all levels)
   */
  async getStats() {
    if (this.multiLevelCacheEnabled) {
      const multiLevelStats = this.multiLevelCacheService.getStats();
      const standardMetrics = this.metricsService.getMetrics();
      
      return {
        multiLevel: multiLevelStats,
        standard: standardMetrics,
        enabled: true,
      };
    }
    
    return {
      standard: this.metricsService.getMetrics(),
      enabled: false,
    };
  }

  /**
   * Configure cache strategy
   */
  configureStrategy(strategy: CacheStrategy, config?: any): void {
    if (this.multiLevelCacheEnabled) {
      this.multiLevelCacheService.configure({ 
        strategy,
        l1Config: config?.l1,
        l2Config: config?.l2,
      });
      this.logger.log(`Cache strategy configured: ${strategy}`);
    }
  }

  /**
   * Check cache health
   */
  async isHealthy(): Promise<boolean> {
    if (this.multiLevelCacheEnabled) {
      return await this.multiLevelCacheService.isHealthy();
    }
    
    // Basic health check for standard cache
    try {
      await this.cacheManager.get('health-check');
      return true;
    } catch {
      return false;
    }
  }
}
