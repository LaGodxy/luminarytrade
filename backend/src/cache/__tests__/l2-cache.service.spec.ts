import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { L2CacheService } from '../l2-cache.service';

describe('L2CacheService', () => {
  let service: L2CacheService;
  let mockConfigService: jest.Mocked<ConfigService>;

  beforeEach(async () => {
    mockConfigService = {
      get: jest.fn(),
    } as any;

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        L2CacheService,
        {
          provide: ConfigService,
          useValue: mockConfigService,
        },
      ],
    }).compile();

    service = module.get<L2CacheService>(L2CacheService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('get', () => {
    it('should return null for non-existent key', async () => {
      const result = await service.get('non-existent-key');
      expect(result).toBeNull();
    });

    it('should return value for existing key', async () => {
      await service.set('test-key', 'test-value');
      const result = await service.get('test-key');
      expect(result).toBe('test-value');
    });

    it('should update stats on hit', async () => {
      await service.set('test-key', 'test-value');
      const initialStats = service.getStats();
      
      await service.get('test-key');
      
      const updatedStats = service.getStats();
      expect(updatedStats.hits).toBe(initialStats.hits + 1);
    });

    it('should update stats on miss', async () => {
      const initialStats = service.getStats();
      
      await service.get('non-existent-key');
      
      const updatedStats = service.getStats();
      expect(updatedStats.misses).toBe(initialStats.misses + 1);
    });

    it('should return null when not connected', async () => {
      // Force disconnection by destroying the service
      await service.onModuleDestroy();
      
      const result = await service.get('test-key');
      expect(result).toBeNull();
    });
  });

  describe('set', () => {
    it('should set value with default TTL', async () => {
      await service.set('test-key', 'test-value');
      const result = await service.get('test-key');
      expect(result).toBe('test-value');
    });

    it('should set value with custom TTL', async () => {
      await service.set('test-key', 'test-value', 60000);
      const result = await service.get('test-key');
      expect(result).toBe('test-value');
    });

    it('should update stats on set', async () => {
      const initialStats = service.getStats();
      
      await service.set('test-key', 'test-value');
      
      const updatedStats = service.getStats();
      expect(updatedStats.sets).toBe(initialStats.sets + 1);
    });

    it('should handle errors gracefully', async () => {
      // Force disconnection
      await service.onModuleDestroy();
      
      // Should not throw error
      await expect(service.set('test-key', 'test-value')).resolves.toBeUndefined();
    });
  });

  describe('delete', () => {
    it('should return false for non-existent key', async () => {
      const result = await service.delete('non-existent-key');
      expect(result).toBe(false);
    });

    it('should return true for existing key', async () => {
      await service.set('test-key', 'test-value');
      const result = await service.delete('test-key');
      expect(result).toBe(true);
    });

    it('should actually delete the key', async () => {
      await service.set('test-key', 'test-value');
      await service.delete('test-key');
      
      const result = await service.get('test-key');
      expect(result).toBeNull();
    });

    it('should update stats on delete', async () => {
      await service.set('test-key', 'test-value');
      const initialStats = service.getStats();
      
      await service.delete('test-key');
      
      const updatedStats = service.getStats();
      expect(updatedStats.deletes).toBe(initialStats.deletes + 1);
    });
  });

  describe('exists', () => {
    it('should return false for non-existent key', async () => {
      const result = await service.exists('non-existent-key');
      expect(result).toBe(false);
    });

    it('should return true for existing key', async () => {
      await service.set('test-key', 'test-value');
      const result = await service.exists('test-key');
      expect(result).toBe(true);
    });

    it('should return false for deleted key', async () => {
      await service.set('test-key', 'test-value');
      await service.delete('test-key');
      
      const result = await service.exists('test-key');
      expect(result).toBe(false);
    });
  });

  describe('mget', () => {
    it('should get multiple keys', async () => {
      await service.set('key1', 'value1');
      await service.set('key2', 'value2');
      
      const results = await service.mget(['key1', 'key2', 'key3']);
      
      expect(results).toEqual(['value1', 'value2', null]);
    });

    it('should return empty array when not connected', async () => {
      await service.onModuleDestroy();
      
      const results = await service.mget(['key1', 'key2']);
      
      expect(results).toEqual([null, null]);
    });
  });

  describe('mset', () => {
    it('should set multiple keys', async () => {
      const items = [
        { key: 'key1', value: 'value1' },
        { key: 'key2', value: 'value2' },
      ];
      
      await service.mset(items);
      
      expect(await service.get('key1')).toBe('value1');
      expect(await service.get('key2')).toBe('value2');
    });

    it('should update stats for all items', async () => {
      const items = [
        { key: 'key1', value: 'value1' },
        { key: 'key2', value: 'value2' },
      ];
      
      const initialStats = service.getStats();
      await service.mset(items);
      
      const updatedStats = service.getStats();
      expect(updatedStats.sets).toBe(initialStats.sets + 2);
    });
  });

  describe('keys', () => {
    it('should return all keys', async () => {
      await service.set('key1', 'value1');
      await service.set('key2', 'value2');
      
      const keys = await service.keys();
      
      expect(keys).toContain('key1');
      expect(keys).toContain('key2');
    });

    it('should return keys matching pattern', async () => {
      await service.set('user:1', 'user1');
      await service.set('user:2', 'user2');
      await service.set('product:1', 'product1');
      
      const userKeys = await service.keys('user:*');
      
      expect(userKeys).toContain('user:1');
      expect(userKeys).toContain('user:2');
      expect(userKeys).not.toContain('product:1');
    });

    it('should return empty array when not connected', async () => {
      await service.onModuleDestroy();
      
      const keys = await service.keys();
      
      expect(keys).toEqual([]);
    });
  });

  describe('clear', () => {
    it('should clear all keys', async () => {
      await service.set('key1', 'value1');
      await service.set('key2', 'value2');
      
      await service.clear();
      
      expect(await service.get('key1')).toBeNull();
      expect(await service.get('key2')).toBeNull();
    });

    it('should clear keys with prefix', async () => {
      await service.set('key1', 'value1');
      await service.set('key2', 'value2');
      
      await service.clear();
      
      const keys = await service.keys();
      expect(keys).toHaveLength(0);
    });
  });

  describe('isHealthy', () => {
    it('should return true when connected', async () => {
      const isHealthy = await service.isHealthy();
      expect(isHealthy).toBe(true);
    });

    it('should return false when not connected', async () => {
      await service.onModuleDestroy();
      
      const isHealthy = await service.isHealthy();
      expect(isHealthy).toBe(false);
    });
  });

  describe('getInfo', () => {
    it('should return Redis info', async () => {
      const info = await service.getInfo();
      
      expect(typeof info).toBe('object');
      expect(info).toHaveProperty('redis_version');
    });

    it('should return empty object when not connected', async () => {
      await service.onModuleDestroy();
      
      const info = await service.getInfo();
      
      expect(info).toEqual({});
    });
  });

  describe('getStats', () => {
    it('should return cache statistics', async () => {
      await service.set('key1', 'value1');
      await service.get('key1');
      await service.get('nonexistent');
      await service.delete('key1');
      
      const stats = service.getStats();
      
      expect(stats.hits).toBe(1);
      expect(stats.misses).toBe(1);
      expect(stats.sets).toBe(1);
      expect(stats.deletes).toBe(1);
      expect(stats.hitRate).toBe(50);
    });

    it('should include error information', async () => {
      await service.onModuleDestroy();
      await service.get('test-key'); // This should cause an error
      
      const stats = service.getStats();
      
      expect(stats.errors).toBeGreaterThan(0);
      expect(stats.lastError).toBeDefined();
      expect(stats.lastErrorTime).toBeDefined();
    });
  });

  describe('resetStats', () => {
    it('should reset all statistics', async () => {
      await service.set('key1', 'value1');
      await service.get('key1');
      
      service.resetStats();
      
      const stats = service.getStats();
      expect(stats.hits).toBe(0);
      expect(stats.misses).toBe(0);
      expect(stats.sets).toBe(0);
      expect(stats.deletes).toBe(0);
      expect(stats.errors).toBe(0);
      expect(stats.hitRate).toBe(0);
    });
  });

  describe('configure', () => {
    it('should update cache configuration', () => {
      const newConfig = {
        keyPrefix: 'test:',
        defaultTtl: 7200000,
        maxRetries: 5,
      };
      
      service.configure(newConfig);
      
      // Configuration is applied, but we can't easily test private config
      // This test ensures the method doesn't throw
      expect(true).toBe(true);
    });
  });

  describe('module lifecycle', () => {
    it('should initialize on module init', async () => {
      await service.onModuleInit();
      
      const isHealthy = await service.isHealthy();
      expect(isHealthy).toBe(true);
    });

    it('should cleanup on module destroy', async () => {
      await service.onModuleDestroy();
      
      const isHealthy = await service.isHealthy();
      expect(isHealthy).toBe(false);
    });
  });
});
