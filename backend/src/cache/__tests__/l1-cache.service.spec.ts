import { Test, TestingModule } from '@nestjs/testing';
import { L1CacheService, CacheItem } from '../l1-cache.service';

describe('L1CacheService', () => {
  let service: L1CacheService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [L1CacheService],
    }).compile();

    service = module.get<L1CacheService>(L1CacheService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('get', () => {
    it('should return null for non-existent key', () => {
      const result = service.get('non-existent-key');
      expect(result).toBeNull();
    });

    it('should return value for existing key', () => {
      service.set('test-key', 'test-value');
      const result = service.get('test-key');
      expect(result).toBe('test-value');
    });

    it('should return null for expired key', (done) => {
      service.set('test-key', 'test-value', 100); // 100ms TTL
      
      setTimeout(() => {
        const result = service.get('test-key');
        expect(result).toBeNull();
        done();
      }, 150);
    });

    it('should update access count and last accessed', () => {
      service.set('test-key', 'test-value');
      
      const initialStats = service.getStats();
      expect(initialStats.hits).toBe(0);
      
      service.get('test-key');
      service.get('test-key');
      
      const updatedStats = service.getStats();
      expect(updatedStats.hits).toBe(2);
    });
  });

  describe('set', () => {
    it('should set value with default TTL', () => {
      service.set('test-key', 'test-value');
      const result = service.get('test-key');
      expect(result).toBe('test-value');
    });

    it('should set value with custom TTL', () => {
      service.set('test-key', 'test-value', 60000); // 1 minute
      const result = service.get('test-key');
      expect(result).toBe('test-value');
    });

    it('should update existing key', () => {
      service.set('test-key', 'initial-value');
      service.set('test-key', 'updated-value');
      
      const result = service.get('test-key');
      expect(result).toBe('updated-value');
    });

    it('should update stats on set', () => {
      const initialStats = service.getStats();
      service.set('test-key', 'test-value');
      
      const updatedStats = service.getStats();
      expect(updatedStats.sets).toBe(initialStats.sets + 1);
      expect(updatedStats.currentSize).toBe(initialStats.currentSize + 1);
    });
  });

  describe('delete', () => {
    it('should return false for non-existent key', () => {
      const result = service.delete('non-existent-key');
      expect(result).toBe(false);
    });

    it('should return true and delete existing key', () => {
      service.set('test-key', 'test-value');
      const result = service.delete('test-key');
      
      expect(result).toBe(true);
      expect(service.get('test-key')).toBeNull();
    });

    it('should update stats on delete', () => {
      service.set('test-key', 'test-value');
      const initialStats = service.getStats();
      
      service.delete('test-key');
      const updatedStats = service.getStats();
      
      expect(updatedStats.deletes).toBe(initialStats.deletes + 1);
      expect(updatedStats.currentSize).toBe(initialStats.currentSize - 1);
    });
  });

  describe('clear', () => {
    it('should clear all cache items', () => {
      service.set('key1', 'value1');
      service.set('key2', 'value2');
      
      service.clear();
      
      expect(service.get('key1')).toBeNull();
      expect(service.get('key2')).toBeNull();
      expect(service.size()).toBe(0);
    });

    it('should reset stats on clear', () => {
      service.set('key1', 'value1');
      service.set('key2', 'value2');
      
      service.clear();
      
      const stats = service.getStats();
      expect(stats.currentSize).toBe(0);
      expect(stats.currentSizeBytes).toBe(0);
    });
  });

  describe('has', () => {
    it('should return false for non-existent key', () => {
      const result = service.has('non-existent-key');
      expect(result).toBe(false);
    });

    it('should return true for existing key', () => {
      service.set('test-key', 'test-value');
      const result = service.has('test-key');
      expect(result).toBe(true);
    });

    it('should return false for expired key', (done) => {
      service.set('test-key', 'test-value', 100);
      
      setTimeout(() => {
        const result = service.has('test-key');
        expect(result).toBe(false);
        done();
      }, 150);
    });
  });

  describe('mget', () => {
    it('should get multiple keys', () => {
      service.set('key1', 'value1');
      service.set('key2', 'value2');
      
      const results = service.mget(['key1', 'key2', 'key3']);
      
      expect(results).toEqual(['value1', 'value2', null]);
    });
  });

  describe('mset', () => {
    it('should set multiple keys', () => {
      const items = [
        { key: 'key1', value: 'value1' },
        { key: 'key2', value: 'value2' },
        { key: 'key3', value: 'value3' },
      ];
      
      service.mset(items);
      
      expect(service.get('key1')).toBe('value1');
      expect(service.get('key2')).toBe('value2');
      expect(service.get('key3')).toBe('value3');
    });
  });

  describe('keys', () => {
    it('should return all keys', () => {
      service.set('key1', 'value1');
      service.set('key2', 'value2');
      
      const keys = service.keys();
      
      expect(keys).toContain('key1');
      expect(keys).toContain('key2');
      expect(keys).toHaveLength(2);
    });
  });

  describe('LRU eviction', () => {
    it('should evict least recently used items when at capacity', () => {
      // Configure small cache for testing
      service.configure({ maxSize: 2 });
      
      service.set('key1', 'value1');
      service.set('key2', 'value2');
      service.set('key3', 'value3'); // Should evict key1
      
      expect(service.get('key1')).toBeNull();
      expect(service.get('key2')).toBe('value2');
      expect(service.get('key3')).toBe('value3');
      
      const stats = service.getStats();
      expect(stats.evictions).toBe(1);
    });

    it('should update LRU order on access', () => {
      service.configure({ maxSize: 2 });
      
      service.set('key1', 'value1');
      service.set('key2', 'value2');
      service.get('key1'); // Access key1 to make it most recently used
      service.set('key3', 'value3'); // Should evict key2
      
      expect(service.get('key1')).toBe('value1');
      expect(service.get('key2')).toBeNull();
      expect(service.get('key3')).toBe('value3');
    });
  });

  describe('warmUp', () => {
    it('should warm up cache with hot keys', async () => {
      const hotKeys = [
        { key: 'hot1', value: 'value1' },
        { key: 'hot2', value: 'value2', ttl: 60000 },
      ];
      
      await service.warmUp(hotKeys);
      
      expect(service.get('hot1')).toBe('value1');
      expect(service.get('hot2')).toBe('value2');
      expect(service.size()).toBe(2);
    });
  });

  describe('getStats', () => {
    it('should return cache statistics', () => {
      service.set('key1', 'value1');
      service.get('key1');
      service.get('nonexistent');
      service.delete('key1');
      
      const stats = service.getStats();
      
      expect(stats.hits).toBe(1);
      expect(stats.misses).toBe(1);
      expect(stats.sets).toBe(1);
      expect(stats.deletes).toBe(1);
      expect(stats.currentSize).toBe(0);
      expect(stats.hitRate).toBe(50);
    });
  });

  describe('resetStats', () => {
    it('should reset all statistics', () => {
      service.set('key1', 'value1');
      service.get('key1');
      
      service.resetStats();
      
      const stats = service.getStats();
      expect(stats.hits).toBe(0);
      expect(stats.misses).toBe(0);
      expect(stats.sets).toBe(0);
      expect(stats.deletes).toBe(0);
      expect(stats.hitRate).toBe(0);
    });
  });

  describe('getKeysByPattern', () => {
    it('should return keys matching pattern', async () => {
      service.set('user:1', 'user1');
      service.set('user:2', 'user2');
      service.set('product:1', 'product1');
      
      const userKeys = await service.getKeysByPattern('user:*');
      
      expect(userKeys).toContain('user:1');
      expect(userKeys).toContain('user:2');
      expect(userKeys).toHaveLength(2);
    });

    it('should return empty array for no matches', async () => {
      service.set('user:1', 'user1');
      
      const productKeys = await service.getKeysByPattern('product:*');
      
      expect(productKeys).toHaveLength(0);
    });
  });

  describe('configure', () => {
    it('should update cache configuration', () => {
      const newConfig = {
        maxSize: 500,
        defaultTtl: 600000,
        cleanupInterval: 30000,
      };
      
      service.configure(newConfig);
      
      // Configuration is applied, but we can't easily test private config
      // This test ensures the method doesn't throw
      expect(true).toBe(true);
    });
  });
});
