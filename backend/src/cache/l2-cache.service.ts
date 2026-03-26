import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

export interface L2CacheConfig {
  keyPrefix: string;
  defaultTtl: number;
  maxRetries: number;
  retryDelay: number;
  connectTimeout: number;
  lazyConnect?: boolean;
}

export interface CacheStats {
  hits: number;
  misses: number;
  sets: number;
  deletes: number;
  errors: number;
  hitRate: number;
  averageResponseTime: number;
  lastError?: string;
  lastErrorTime?: Date;
}

// Mock Redis client interface
interface RedisClient {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, mode?: string, duration?: number): Promise<string>;
  del(key: string): Promise<number>;
  exists(key: string): Promise<number>;
  keys(pattern?: string): Promise<string[]>;
  mget(keys: string[]): Promise<(string | null)[]>;
  mset(items: Array<{ key: string; value: string }>): Promise<string>;
  ping(): Promise<string>;
  info(section?: string): Promise<string>;
}

@Injectable()
export class L2CacheService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(L2CacheService.name);
  private redisClient: RedisClient | null = null;
  private config: L2CacheConfig;
  private stats: CacheStats = {
    hits: 0,
    misses: 0,
    sets: 0,
    deletes: 0,
    errors: 0,
    hitRate: 0,
    averageResponseTime: 0,
  };
  private isConnected = false;

  constructor(private readonly configService: ConfigService) {
    this.config = {
      keyPrefix: 'luminarytrade:',
      defaultTtl: 3600000, // 1 hour
      maxRetries: 3,
      retryDelay: 1000,
      connectTimeout: 10000,
      lazyConnect: true,
    };
  }

  async onModuleInit() {
    this.logger.log('Initializing L2 Cache Service (Redis)...');
    await this.connectToRedis();
    this.logger.log('L2 Cache Service initialized');
  }

  /**
   * Connect to Redis
   */
  private async connectToRedis(): Promise<void> {
    try {
      // In a real implementation, you would use a Redis client like ioredis
      this.redisClient = await this.createRedisClient();
      await this.redisClient.ping();
      this.isConnected = true;
      this.logger.log('Connected to Redis successfully');
    } catch (error) {
      this.logger.error('Failed to connect to Redis:', error);
      this.stats.errors++;
      this.stats.lastError = error instanceof Error ? error.message : String(error);
      this.stats.lastErrorTime = new Date();
      
      // Retry connection
      setTimeout(() => this.connectToRedis(), this.config.retryDelay);
    }
  }

  /**
   * Create Redis client (mock implementation)
   */
  private async createRedisClient(): Promise<RedisClient> {
    const redisHost = this.configService.get<string>('REDIS_HOST', 'localhost');
    const redisPort = this.configService.get<string>('REDIS_PORT', '6379');
    const redisPassword = this.configService.get<string>('REDIS_PASSWORD');

    this.logger.log(`Connecting to Redis at ${redisHost}:${redisPort}`);

    // Mock Redis client - in real implementation use ioredis or redis
    return {
      get: async (key: string) => {
        const startTime = Date.now();
        try {
          // Simulate Redis GET operation
          const value = await this.simulateRedisOperation('GET', key);
          const responseTime = Date.now() - startTime;
          this.updateResponseTime(responseTime);
          
          if (value !== null) {
            this.stats.hits++;
          } else {
            this.stats.misses++;
          }
          this.updateHitRate();
          
          return value;
        } catch (error) {
          this.stats.errors++;
          this.stats.lastError = error instanceof Error ? error.message : String(error);
          this.stats.lastErrorTime = new Date();
          throw error;
        }
      },

      set: async (key: string, value: string, mode = 'EX', duration?: number) => {
        const startTime = Date.now();
        try {
          const ttl = duration || this.config.defaultTtl;
          await this.simulateRedisOperation('SET', key, value, mode, ttl);
          const responseTime = Date.now() - startTime;
          this.updateResponseTime(responseTime);
          
          this.stats.sets++;
          return 'OK';
        } catch (error) {
          this.stats.errors++;
          this.stats.lastError = error instanceof Error ? error.message : String(error);
          this.stats.lastErrorTime = new Date();
          throw error;
        }
      },

      del: async (key: string) => {
        const startTime = Date.now();
        try {
          await this.simulateRedisOperation('DEL', key);
          const responseTime = Date.now() - startTime;
          this.updateResponseTime(responseTime);
          
          this.stats.deletes++;
          return 1;
        } catch (error) {
          this.stats.errors++;
          this.stats.lastError = error instanceof Error ? error.message : String(error);
          this.stats.lastErrorTime = new Date();
          throw error;
        }
      },

      exists: async (key: string) => {
        const startTime = Date.now();
        try {
          const exists = await this.simulateRedisOperation('EXISTS', key);
          const responseTime = Date.now() - startTime;
          this.updateResponseTime(responseTime);
          
          return exists;
        } catch (error) {
          this.stats.errors++;
          this.stats.lastError = error instanceof Error ? error.message : String(error);
          this.stats.lastErrorTime = new Date();
          throw error;
        }
      },

      keys: async (pattern?: string) => {
        const startTime = Date.now();
        try {
          const keys = await this.simulateRedisOperation('KEYS', pattern);
          const responseTime = Date.now() - startTime;
          this.updateResponseTime(responseTime);
          
          return keys;
        } catch (error) {
          this.stats.errors++;
          this.stats.lastError = error instanceof Error ? error.message : String(error);
          this.stats.lastErrorTime = new Date();
          throw error;
        }
      },

      mget: async (keys: string[]) => {
        const startTime = Date.now();
        try {
          const values = await this.simulateRedisOperation('MGET', ...keys);
          const responseTime = Date.now() - startTime;
          this.updateResponseTime(responseTime);
          
          // Update stats for each key
          for (const value of values) {
            if (value !== null) {
              this.stats.hits++;
            } else {
              this.stats.misses++;
            }
          }
          this.updateHitRate();
          
          return values;
        } catch (error) {
          this.stats.errors++;
          this.stats.lastError = error instanceof Error ? error.message : String(error);
          this.stats.lastErrorTime = new Date();
          throw error;
        }
      },

      mset: async (items: Array<{ key: string; value: string }>) => {
        const startTime = Date.now();
        try {
          await this.simulateRedisOperation('MSET', ...items.flatMap(item => [item.key, item.value]));
          const responseTime = Date.now() - startTime;
          this.updateResponseTime(responseTime);
          
          this.stats.sets += items.length;
          return 'OK';
        } catch (error) {
          this.stats.errors++;
          this.stats.lastError = error instanceof Error ? error.message : String(error);
          this.stats.lastErrorTime = new Date();
          throw error;
        }
      },

      ping: async () => {
        const startTime = Date.now();
        try {
          const result = await this.simulateRedisOperation('PING');
          const responseTime = Date.now() - startTime;
          this.updateResponseTime(responseTime);
          
          return result;
        } catch (error) {
          this.stats.errors++;
          this.stats.lastError = error instanceof Error ? error.message : String(error);
          this.stats.lastErrorTime = new Date();
          throw error;
        }
      },

      info: async (section?: string) => {
        const startTime = Date.now();
        try {
          const info = await this.simulateRedisOperation('INFO', section);
          const responseTime = Date.now() - startTime;
          this.updateResponseTime(responseTime);
          
          return info;
        } catch (error) {
          this.stats.errors++;
          this.stats.lastError = error instanceof Error ? error.message : String(error);
          this.stats.lastErrorTime = new Date();
          throw error;
        }
      },
    };
  }

  /**
   * Simulate Redis operations (mock implementation)
   */
  private async simulateRedisOperation(operation: string, ...args: any[]): Promise<any> {
    // This is a mock implementation
    // In a real implementation, this would make actual Redis calls
    
    await new Promise(resolve => setTimeout(resolve, Math.random() * 10 + 5));

    switch (operation) {
      case 'GET':
        return Math.random() > 0.3 ? `value:${args[0]}` : null;
      case 'SET':
        return 'OK';
      case 'DEL':
        return 1;
      case 'EXISTS':
        return Math.random() > 0.5 ? 1 : 0;
      case 'KEYS':
        return ['key1', 'key2', 'key3'].filter(key => 
          !args[0] || key.includes(args[0])
        );
      case 'MGET':
        return args.map(() => Math.random() > 0.3 ? `value:${Math.random()}` : null);
      case 'MSET':
        return 'OK';
      case 'PING':
        return 'PONG';
      case 'INFO':
        return 'redis_version:6.2.0';
      default:
        return null;
    }
  }

  /**
   * Get value from Redis
   */
  async get(key: string): Promise<string | null> {
    if (!this.isConnected || !this.redisClient) {
      this.stats.misses++;
      this.updateHitRate();
      return null;
    }

    const fullKey = `${this.config.keyPrefix}${key}`;
    return this.redisClient.get(fullKey);
  }

  /**
   * Set value in Redis
   */
  async set(key: string, value: string, ttl?: number): Promise<void> {
    if (!this.isConnected || !this.redisClient) {
      this.stats.errors++;
      this.stats.lastError = 'Redis not connected';
      this.stats.lastErrorTime = new Date();
      return;
    }

    const fullKey = `${this.config.keyPrefix}${key}`;
    await this.redisClient.set(fullKey, value, 'EX', ttl);
  }

  /**
   * Delete value from Redis
   */
  async delete(key: string): Promise<boolean> {
    if (!this.isConnected || !this.redisClient) {
      this.stats.errors++;
      this.stats.lastError = 'Redis not connected';
      this.stats.lastErrorTime = new Date();
      return false;
    }

    const fullKey = `${this.config.keyPrefix}${key}`;
    const result = await this.redisClient.del(fullKey);
    return result > 0;
  }

  /**
   * Check if key exists
   */
  async exists(key: string): Promise<boolean> {
    if (!this.isConnected || !this.redisClient) {
      return false;
    }

    const fullKey = `${this.config.keyPrefix}${key}`;
    const result = await this.redisClient.exists(fullKey);
    return result > 0;
  }

  /**
   * Get multiple keys
   */
  async mget(keys: string[]): Promise<(string | null)[]> {
    if (!this.isConnected || !this.redisClient) {
      return keys.map(() => null);
    }

    const fullKeys = keys.map(key => `${this.config.keyPrefix}${key}`);
    return this.redisClient.mget(fullKeys);
  }

  /**
   * Set multiple keys
   */
  async mset(items: Array<{ key: string; value: string }>): Promise<void> {
    if (!this.isConnected || !this.redisClient) {
      this.stats.errors++;
      this.stats.lastError = 'Redis not connected';
      this.stats.lastErrorTime = new Date();
      return;
    }

    const fullItems = items.map(item => ({
      key: `${this.config.keyPrefix}${item.key}`,
      value: item.value,
    }));

    await this.redisClient.mset(fullItems);
  }

  /**
   * Get keys matching pattern
   */
  async keys(pattern?: string): Promise<string[]> {
    if (!this.isConnected || !this.redisClient) {
      return [];
    }

    const fullPattern = pattern ? `${this.config.keyPrefix}${pattern}` : `${this.config.keyPrefix}*`;
    const keys = await this.redisClient.keys(fullPattern);
    return keys.map(key => key.replace(this.config.keyPrefix, ''));
  }

  /**
   * Clear all keys with prefix
   */
  async clear(): Promise<void> {
    if (!this.isConnected || !this.redisClient) {
      return;
    }

    const keys = await this.redisClient.keys(`${this.config.keyPrefix}*`);
    if (keys.length > 0) {
      await this.redisClient.del(...keys);
      this.logger.log(`Cleared ${keys.length} keys from Redis`);
    }
  }

  /**
   * Get cache statistics
   */
  getStats(): CacheStats {
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
      errors: 0,
      hitRate: 0,
      averageResponseTime: 0,
    };
  }

  /**
   * Check Redis health
   */
  async isHealthy(): Promise<boolean> {
    if (!this.isConnected || !this.redisClient) {
      return false;
    }

    try {
      const pong = await this.redisClient.ping();
      return pong === 'PONG';
    } catch (error) {
      this.logger.error('Redis health check failed:', error);
      return false;
    }
  }

  /**
   * Get Redis info
   */
  async getInfo(): Promise<Record<string, any>> {
    if (!this.isConnected || !this.redisClient) {
      return {};
    }

    try {
      const info = await this.redisClient.info();
      return this.parseRedisInfo(info);
    } catch (error) {
      this.logger.error('Failed to get Redis info:', error);
      return {};
    }
  }

  /**
   * Parse Redis info response
   */
  private parseRedisInfo(info: string): Record<string, any> {
    const parsed: Record<string, any> = {};
    const lines = info.split('\r\n');
    
    for (const line of lines) {
      if (line.includes(':')) {
        const [key, value] = line.split(':');
        parsed[key.trim()] = value.trim();
      }
    }
    
    return parsed;
  }

  /**
   * Update response time for statistics
   */
  private updateResponseTime(responseTime: number): void {
    const total = this.stats.hits + this.stats.misses;
    if (total > 0) {
      this.stats.averageResponseTime = 
        (this.stats.averageResponseTime * (total - 1) + responseTime) / total;
    }
  }

  /**
   * Update hit rate
   */
  private updateHitRate(): void {
    const total = this.stats.hits + this.stats.misses;
    this.stats.hitRate = total > 0 ? (this.stats.hits / total) * 100 : 0;
  }

  /**
   * Configure cache settings
   */
  configure(config: Partial<L2CacheConfig>): void {
    this.config = { ...this.config, ...config };
    this.logger.log(`L2 Cache configuration updated: ${JSON.stringify(this.config)}`);
  }

  async onModuleDestroy() {
    this.isConnected = false;
    this.redisClient = null;
    this.logger.log('L2 Cache Service destroyed');
  }
}
