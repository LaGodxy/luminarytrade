import { Test, TestingModule } from '@nestjs/testing';
import { MultiLevelCacheService, CacheStrategy } from '../multi-level-cache.service';
import { L1CacheService } from '../l1-cache.service';
import { L2CacheService } from '../l2-cache.service';

describe('MultiLevelCacheService', () => {
  let service: MultiLevelCacheService;
  let mockL1CacheService: jest.Mocked<L1CacheService>;
  let mockL2CacheService: jest.Mocked<L2CacheService>;

  beforeEach(async () => {
    mockL1CacheService = {
      get: jest.fn(),
      set: jest.fn(),
      delete: jest.fn(),
      clear: jest.fn(),
      size: jest.fn().mockReturnValue(0),
      keys: jest.fn().mockReturnValue([]),
      getStats: jest.fn().mockReturnValue({
        hits: 0, misses: 0, sets: 0, deletes: 0,
        evictions: 0, currentSize: 0, currentSizeBytes: 0,
        hitRate: 0, averageAccessCount: 0,
      }),
      resetStats: jest.fn(),
      configure: jest.fn(),
    } as any;

    mockL2CacheService = {
      get: jest.fn(),
      set: jest.fn(),
      delete: jest.fn(),
      clear: jest.fn(),
      keys: jest.fn().mockResolvedValue([]),
      isHealthy: jest.fn().mockResolvedValue(true),
      getStats: jest.fn().mockReturnValue({
        hits: 0, misses: 0, sets: 0, deletes: 0,
        errors: 0, hitRate: 0, averageResponseTime: 0,
      }),
      resetStats: jest.fn(),
      configure: jest.fn(),
    } as any;

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MultiLevelCacheService,
        {
          provide: L1CacheService,
          useValue: mockL1CacheService,
        },
        {
          provide: L2CacheService,
          useValue: mockL2CacheService,
        },
      ],
    }).compile();

    service = module.get<MultiLevelCacheService>(MultiLevelCacheService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('get', () => {
    it('should get from L1 in HYBRID strategy', async () => {
      mockL1CacheService.get.mockReturnValue('local-value');
      mockL2CacheService.get.mockResolvedValue('redis-value');

      const result = await service.get('test-key');

      expect(result.value).toBe('local-value');
      expect(result.source).toBe('L1');
      expect(mockL1CacheService.get).toHaveBeenCalledWith('test-key');
      expect(mockL2CacheService.get).not.toHaveBeenCalled();
    });

    it('should get from L2 when L1 misses', async () => {
      mockL1CacheService.get.mockReturnValue(null);
      mockL2CacheService.get.mockResolvedValue('redis-value');

      const result = await service.get('test-key');

      expect(result.value).toBe('redis-value');
      expect(result.source).toBe('L2');
      expect(mockL1CacheService.get).toHaveBeenCalledWith('test-key');
      expect(mockL2CacheService.get).toHaveBeenCalledWith('test-key');
    });

    it('should return MISS when both miss', async () => {
      mockL1CacheService.get.mockReturnValue(null);
      mockL2CacheService.get.mockResolvedValue(null);

      const result = await service.get('test-key');

      expect(result.value).toBeNull();
      expect(result.source).toBe('MISS');
    });

    it('should use LOCAL strategy', async () => {
      service.configure({ strategy: CacheStrategy.LOCAL });
      mockL1CacheService.get.mockReturnValue('local-value');

      const result = await service.get('test-key');

      expect(result.value).toBe('local-value');
      expect(result.source).toBe('L1');
    });

    it('should use SHARED strategy', async () => {
      service.configure({ strategy: CacheStrategy.SHARED });
      mockL2CacheService.get.mockResolvedValue('redis-value');

      const result = await service.get('test-key');

      expect(result.value).toBe('redis-value');
      expect(result.source).toBe('L2');
    });
  });

  describe('set', () => {
    it('should set to both levels in HYBRID strategy', async () => {
      await service.set('test-key', 'test-value');

      expect(mockL1CacheService.set).toHaveBeenCalledWith('test-key', 'test-value', undefined);
      expect(mockL2CacheService.set).toHaveBeenCalledWith('test-key', JSON.stringify('test-value'), undefined);
    });

    it('should use LOCAL strategy', async () => {
      service.configure({ strategy: CacheStrategy.LOCAL });
      await service.set('test-key', 'test-value');

      expect(mockL1CacheService.set).toHaveBeenCalledWith('test-key', 'test-value', undefined);
      expect(mockL2CacheService.set).not.toHaveBeenCalled();
    });

    it('should use SHARED strategy', async () => {
      service.configure({ strategy: CacheStrategy.SHARED });
      await service.set('test-key', 'test-value');

      expect(mockL1CacheService.set).not.toHaveBeenCalled();
      expect(mockL2CacheService.set).toHaveBeenCalledWith('test-key', JSON.stringify('test-value'), undefined);
    });
  });

  describe('delete', () => {
    it('should delete from both levels in HYBRID strategy', async () => {
      mockL1CacheService.delete.mockReturnValue(true);
      mockL2CacheService.delete.mockResolvedValue(true);

      const result = await service.delete('test-key');

      expect(result).toBe(true);
      expect(mockL1CacheService.delete).toHaveBeenCalledWith('test-key');
      expect(mockL2CacheService.delete).toHaveBeenCalledWith('test-key');
    });
  });

  describe('clear', () => {
    it('should clear both levels in HYBRID strategy', async () => {
      await service.clear();

      expect(mockL1CacheService.clear).toHaveBeenCalled();
      expect(mockL2CacheService.clear).toHaveBeenCalled();
    });
  });

  describe('getStats', () => {
    it('should return combined statistics', () => {
      mockL1CacheService.getStats.mockReturnValue({
        hits: 10, misses: 5, hitRate: 66.7,
      });
      mockL2CacheService.getStats.mockReturnValue({
        hits: 8, misses: 7, hitRate: 53.3,
      });

      const stats = service.getStats();

      expect(stats.l1.hitRate).toBe(66.7);
      expect(stats.l2.hitRate).toBe(53.3);
      expect(stats.combined.hits).toBeGreaterThan(0);
    });
  });

  describe('isHealthy', () => {
    it('should return true when both levels are healthy', async () => {
      mockL1CacheService.size.mockReturnValue(5);
      mockL2CacheService.isHealthy.mockResolvedValue(true);

      const isHealthy = await service.isHealthy();

      expect(isHealthy).toBe(true);
    });

    it('should return false when L2 is unhealthy', async () => {
      mockL1CacheService.size.mockReturnValue(5);
      mockL2CacheService.isHealthy.mockResolvedValue(false);

      const isHealthy = await service.isHealthy();

      expect(isHealthy).toBe(false);
    });
  });
});
