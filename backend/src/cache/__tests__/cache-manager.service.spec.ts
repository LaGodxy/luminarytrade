import { Test, TestingModule } from '@nestjs/testing';
import { CACHE_MANAGER } from '@nestjs/cache-manager';
import { CacheManager } from '../cache-manager.service';
import { CacheMetricsService } from '../cache-metrics.service';
import { L1CacheService } from '../l1-cache.service';
import { L2CacheService } from '../l2-cache.service';
import { MultiLevelCacheService, CacheStrategy } from '../multi-level-cache.service';

describe('CacheManager', () => {
  let service: CacheManager;
  let mockCacheManager: jest.Mocked<any>;
  let mockMetricsService: jest.Mocked<CacheMetricsService>;
  let mockL1CacheService: jest.Mocked<L1CacheService>;
  let mockL2CacheService: jest.Mocked<L2CacheService>;
  let mockMultiLevelCacheService: jest.Mocked<MultiLevelCacheService>;

  beforeEach(async () => {
    mockCacheManager = {
      get: jest.fn(),
      set: jest.fn(),
      del: jest.fn(),
      reset: jest.fn(),
      store: { keys: jest.fn() },
    };

    mockMetricsService = {
      recordHit: jest.fn(),
      recordMiss: jest.fn(),
      getMetrics: jest.fn().mockReturnValue({ hits: 0, misses: 0 }),
      reset: jest.fn(),
    };

    mockL1CacheService = {
      get: jest.fn(),
      set: jest.fn(),
      delete: jest.fn(),
      clear: jest.fn(),
      size: jest.fn().mockReturnValue(0),
      keys: jest.fn().mockReturnValue([]),
      getStats: jest.fn().mockReturnValue({ hitRate: 0 }),
    } as any;

    mockL2CacheService = {
      get: jest.fn(),
      set: jest.fn(),
      delete: jest.fn(),
      clear: jest.fn(),
      keys: jest.fn().mockResolvedValue([]),
      isHealthy: jest.fn().mockResolvedValue(true),
      getStats: jest.fn().mockReturnValue({ hitRate: 0 }),
    } as any;

    mockMultiLevelCacheService = {
      get: jest.fn(),
      set: jest.fn(),
      delete: jest.fn(),
      clear: jest.fn(),
      getKeys: jest.fn().mockResolvedValue([]),
      isHealthy: jest.fn().mockResolvedValue(true),
      getStats: jest.fn().mockReturnValue({}),
      configure: jest.fn(),
    } as any;

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        CacheManager,
        {
          provide: CACHE_MANAGER,
          useValue: mockCacheManager,
        },
        {
          provide: CacheMetricsService,
          useValue: mockMetricsService,
        },
        {
          provide: L1CacheService,
          useValue: mockL1CacheService,
        },
        {
          provide: L2CacheService,
          useValue: mockL2CacheService,
        },
        {
          provide: MultiLevelCacheService,
          useValue: mockMultiLevelCacheService,
        },
      ],
    }).compile();

    service = module.get<CacheManager>(CacheManager);
    await service.onModuleInit();
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('generateKey', () => {
    it('should return key without namespace', () => {
      const result = service.generateKey('test-key');
      expect(result).toBe('test-key');
    });

    it('should return key with namespace', () => {
      const result = service.generateKey('test-key', 'user');
      expect(result).toBe('user:test-key');
    });
  });

  describe('get', () => {
    it('should use standard cache for normal requests', async () => {
      mockCacheManager.get.mockResolvedValue('test-value');
      
      const result = await service.get('test-key');
      
      expect(result).toBe('test-value');
      expect(mockCacheManager.get).toHaveBeenCalledWith('test-key');
      expect(mockMetricsService.recordHit).toHaveBeenCalledWith('test-key');
    });

    it('should use multi-level cache when level is specified', async () => {
      mockMultiLevelCacheService.get.mockResolvedValue({ value: 'multi-value', source: 'L1' });
      
      const result = await service.get('test-key', { level: 'L1' });
      
      expect(result).toBe('multi-value');
      expect(mockMultiLevelCacheService.get).toHaveBeenCalledWith('test-key');
      expect(mockMetricsService.recordHit).toHaveBeenCalledWith('test-key');
    });

    it('should use multi-level cache when strategy is specified', async () => {
      mockMultiLevelCacheService.get.mockResolvedValue({ value: 'multi-value', source: 'L2' });
      
      const result = await service.get('test-key', { strategy: CacheStrategy.SHARED });
      
      expect(result).toBe('multi-value');
      expect(mockMultiLevelCacheService.get).toHaveBeenCalledWith('test-key');
    });

    it('should handle cache errors gracefully', async () => {
      mockCacheManager.get.mockRejectedValue(new Error('Cache error'));
      
      const result = await service.get('test-key');
      
      expect(result).toBeUndefined();
      expect(mockMetricsService.recordMiss).toHaveBeenCalledWith('test-key');
    });
  });

  describe('set', () => {
    it('should use standard cache for normal requests', async () => {
      await service.set('test-key', 'test-value');
      
      expect(mockCacheManager.set).toHaveBeenCalledWith('test-key', 'test-value', undefined);
    });

    it('should use custom TTL', async () => {
      await service.set('test-key', 'test-value', { ttl: 300 });
      
      expect(mockCacheManager.set).toHaveBeenCalledWith('test-key', 'test-value', 300000);
    });

    it('should use multi-level cache when level is specified', async () => {
      await service.set('test-key', 'test-value', { level: 'L1' });
      
      expect(mockMultiLevelCacheService.set).toHaveBeenCalledWith('test-key', 'test-value', undefined);
    });

    it('should use multi-level cache when strategy is specified', async () => {
      await service.set('test-key', 'test-value', { strategy: CacheStrategy.HYBRID });
      
      expect(mockMultiLevelCacheService.set).toHaveBeenCalledWith('test-key', 'test-value', undefined);
    });

    it('should configure strategy when provided', async () => {
      await service.set('test-key', 'test-value', { strategy: CacheStrategy.LOCAL });
      
      expect(mockMultiLevelCacheService.configure).toHaveBeenCalledWith({ strategy: CacheStrategy.LOCAL });
    });
  });

  describe('del', () => {
    it('should use standard cache for normal requests', async () => {
      await service.del('test-key');
      
      expect(mockCacheManager.del).toHaveBeenCalledWith('test-key');
    });

    it('should use multi-level cache when level is specified', async () => {
      await service.del('test-key', { level: 'L2' });
      
      expect(mockMultiLevelCacheService.delete).toHaveBeenCalledWith('test-key');
    });

    it('should use multi-level cache when strategy is specified', async () => {
      await service.del('test-key', { strategy: CacheStrategy.SHARED });
      
      expect(mockMultiLevelCacheService.delete).toHaveBeenCalledWith('test-key');
    });
  });

  describe('delPattern', () => {
    it('should use standard cache for normal requests', async () => {
      mockCacheManager.store.keys.mockResolvedValue(['user:1', 'user:2', 'product:1']);
      
      await service.delPattern('user:*');
      
      expect(mockCacheManager.store.keys).toHaveBeenCalled();
      expect(mockCacheManager.del).toHaveBeenCalledWith('user:1');
      expect(mockCacheManager.del).toHaveBeenCalledWith('user:2');
      expect(mockCacheManager.del).not.toHaveBeenCalledWith('product:1');
    });

    it('should use multi-level cache when level is specified', async () => {
      mockMultiLevelCacheService.getKeys = jest.fn().mockResolvedValue(['user:1', 'user:2']);
      
      await service.delPattern('user:*', { level: 'L1' });
      
      expect(mockMultiLevelCacheService.getKeys).toHaveBeenCalledWith('user:*');
      expect(mockMultiLevelCacheService.delete).toHaveBeenCalledWith('user:1');
      expect(mockMultiLevelCacheService.delete).toHaveBeenCalledWith('user:2');
    });
  });

  describe('reset', () => {
    it('should use standard cache for normal requests', async () => {
      await service.reset();
      
      expect(mockCacheManager.reset).toHaveBeenCalled();
    });

    it('should use multi-level cache when level is specified', async () => {
      await service.reset({ level: 'L2' });
      
      expect(mockMultiLevelCacheService.clear).toHaveBeenCalled();
    });

    it('should use multi-level cache when strategy is specified', async () => {
      await service.reset({ strategy: CacheStrategy.HYBRID });
      
      expect(mockMultiLevelCacheService.clear).toHaveBeenCalled();
    });
  });

  describe('getOrSet', () => {
    it('should return cached value if exists', async () => {
      mockCacheManager.get.mockResolvedValue('cached-value');
      
      const factory = jest.fn().mockResolvedValue('new-value');
      const result = await service.getOrSet('test-key', factory);
      
      expect(result).toBe('cached-value');
      expect(factory).not.toHaveBeenCalled();
    });

    it('should set and return new value if not cached', async () => {
      mockCacheManager.get.mockResolvedValue(undefined);
      mockCacheManager.set.mockResolvedValue(undefined);
      
      const factory = jest.fn().mockResolvedValue('new-value');
      const result = await service.getOrSet('test-key', factory);
      
      expect(result).toBe('new-value');
      expect(factory).toHaveBeenCalled();
      expect(mockCacheManager.set).toHaveBeenCalledWith('test-key', 'new-value', undefined);
    });

    it('should use multi-level cache when specified', async () => {
      mockMultiLevelCacheService.get.mockResolvedValue({ value: null, source: 'MISS' });
      mockMultiLevelCacheService.set.mockResolvedValue(undefined);
      
      const factory = jest.fn().mockResolvedValue('new-value');
      const result = await service.getOrSet('test-key', factory, { level: 'L1' });
      
      expect(result).toBe('new-value');
      expect(factory).toHaveBeenCalled();
      expect(mockMultiLevelCacheService.set).toHaveBeenCalledWith('test-key', 'new-value', undefined);
    });
  });

  describe('warm', () => {
    it('should warm standard cache', async () => {
      const factory = jest.fn().mockResolvedValue('warm-value');
      
      await service.warm('test-key', factory);
      
      expect(factory).toHaveBeenCalled();
      expect(mockCacheManager.set).toHaveBeenCalledWith('test-key', 'warm-value', undefined);
    });

    it('should warm multi-level cache when specified', async () => {
      const factory = jest.fn().mockResolvedValue('warm-value');
      
      await service.warm('test-key', factory, { level: 'L2' });
      
      expect(factory).toHaveBeenCalled();
      expect(mockMultiLevelCacheService.set).toHaveBeenCalledWith('test-key', 'warm-value', undefined);
    });
  });

  describe('getStats', () => {
    it('should return multi-level stats when enabled', async () => {
      mockMultiLevelCacheService.getStats.mockReturnValue({
        l1: { hitRate: 80 },
        l2: { hitRate: 60 },
        combined: { hitRate: 70 },
      });
      
      const stats = await service.getStats();
      
      expect(stats.enabled).toBe(true);
      expect(stats.multiLevel).toBeDefined();
      expect(stats.standard).toBeDefined();
    });

    it('should return standard stats when multi-level disabled', async () => {
      // Simulate disabled multi-level cache
      jest.spyOn(service as any, 'multiLevelCacheEnabled', 'get').mockReturnValue(false);
      
      const stats = await service.getStats();
      
      expect(stats.enabled).toBe(false);
      expect(stats.standard).toBeDefined();
    });
  });

  describe('configureStrategy', () => {
    it('should configure multi-level cache strategy', () => {
      const config = { l1: { maxSize: 500 }, l2: { maxRetries: 5 } };
      
      service.configureStrategy(CacheStrategy.LOCAL, config);
      
      expect(mockMultiLevelCacheService.configure).toHaveBeenCalledWith({
        strategy: CacheStrategy.LOCAL,
        l1Config: config.l1,
        l2Config: config.l2,
      });
    });
  });

  describe('isHealthy', () => {
    it('should return multi-level cache health when enabled', async () => {
      mockMultiLevelCacheService.isHealthy.mockResolvedValue(true);
      
      const isHealthy = await service.isHealthy();
      
      expect(isHealthy).toBe(true);
      expect(mockMultiLevelCacheService.isHealthy).toHaveBeenCalled();
    });

    it('should return standard cache health when multi-level disabled', async () => {
      jest.spyOn(service as any, 'multiLevelCacheEnabled', 'get').mockReturnValue(false);
      mockCacheManager.get.mockResolvedValue('test');
      
      const isHealthy = await service.isHealthy();
      
      expect(isHealthy).toBe(true);
      expect(mockCacheManager.get).toHaveBeenCalledWith('health-check');
    });

    it('should return false when standard cache fails', async () => {
      jest.spyOn(service as any, 'multiLevelCacheEnabled', 'get').mockReturnValue(false);
      mockCacheManager.get.mockRejectedValue(new Error('Cache error'));
      
      const isHealthy = await service.isHealthy();
      
      expect(isHealthy).toBe(false);
    });
  });

  describe('matchPattern', () => {
    it('should match simple patterns', () => {
      expect((service as any).matchPattern('user:123', 'user:*')).toBe(true);
      expect((service as any).matchPattern('product:456', 'user:*')).toBe(false);
    });

    it('should match wildcard patterns', () => {
      expect((service as any).matchPattern('user:123:profile', 'user:*:profile')).toBe(true);
      expect((service as any).matchPattern('user:123:settings', 'user:*:profile')).toBe(false);
    });

    it('should match single character patterns', () => {
      expect((service as any).matchPattern('user1', 'user?')).toBe(true);
      expect((service as any).matchPattern('user12', 'user?')).toBe(false);
    });
  });
});
