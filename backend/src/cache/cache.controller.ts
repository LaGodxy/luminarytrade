import { Controller, Get, Delete, Param, HttpCode, HttpStatus, Post, Body, Query, Res } from '@nestjs/common';
import { Response } from 'express';
import { CacheManager } from './cache-manager.service';
import { CacheMetricsService } from './cache-metrics.service';
import { CacheInvalidator } from './cache-invalidator.service';
import { MultiLevelCacheService, CacheStrategy } from './multi-level-cache.service';
import { L1CacheService } from './l1-cache.service';
import { L2CacheService } from './l2-cache.service';
import { CacheStatsService } from './cache-stats.service';

@Controller('cache')
export class CacheController {
  constructor(
    private readonly cacheManager: CacheManager,
    private readonly metricsService: CacheMetricsService,
    private readonly cacheInvalidator: CacheInvalidator,
    private readonly multiLevelCacheService: MultiLevelCacheService,
    private readonly l1CacheService: L1CacheService,
    private readonly l2CacheService: L2CacheService,
    private readonly cacheStatsService: CacheStatsService,
  ) {}

  /**
   * Get cache metrics (legacy endpoint)
   */
  @Get('metrics')
  getMetrics() {
    return this.metricsService.getMetrics();
  }

  /**
   * Get comprehensive cache statistics with multi-level support
   */
  @Get('stats')
  @HttpCode(HttpStatus.OK)
  async getStats(@Res() res: Response) {
    try {
      const analytics = this.cacheStatsService.getAnalytics();
      const performance = this.cacheStatsService.getPerformanceMetrics();
      
      res.status(HttpStatus.OK).json({
        status: 'success',
        timestamp: new Date().toISOString(),
        analytics,
        performance,
        health: {
          isHealthy: await this.multiLevelCacheService.isHealthy(),
          l1Healthy: this.l1CacheService.size() >= 0,
          l2Healthy: await this.l2CacheService.isHealthy(),
        },
      });
    } catch (error) {
      res.status(HttpStatus.INTERNAL_SERVER_ERROR).json({
        status: 'error',
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Get detailed cache statistics
   */
  @Get('detailed')
  @HttpCode(HttpStatus.OK)
  async getDetailedStats(@Res() res: Response) {
    try {
      const detailedStats = this.cacheStatsService.getDetailedStats();
      
      res.status(HttpStatus.OK).json({
        status: 'success',
        timestamp: new Date().toISOString(),
        stats: detailedStats,
      });
    } catch (error) {
      res.status(HttpStatus.INTERNAL_SERVER_ERROR).json({
        status: 'error',
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Get L1 cache statistics
   */
  @Get('l1/stats')
  @HttpCode(HttpStatus.OK)
  async getL1Stats(@Res() res: Response) {
    try {
      const stats = this.l1CacheService.getStats();
      
      res.status(HttpStatus.OK).json({
        status: 'success',
        timestamp: new Date().toISOString(),
        level: 'L1',
        cache: 'Local Memory',
        stats,
      });
    } catch (error) {
      res.status(HttpStatus.INTERNAL_SERVER_ERROR).json({
        status: 'error',
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Get L2 cache statistics
   */
  @Get('l2/stats')
  @HttpCode(HttpStatus.OK)
  async getL2Stats(@Res() res: Response) {
    try {
      const stats = this.l2CacheService.getStats();
      const info = await this.l2CacheService.getInfo();
      
      res.status(HttpStatus.OK).json({
        status: 'success',
        timestamp: new Date().toISOString(),
        level: 'L2',
        cache: 'Redis',
        stats,
        info,
      });
    } catch (error) {
      res.status(HttpStatus.INTERNAL_SERVER_ERROR).json({
        status: 'error',
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Get cache performance metrics
   */
  @Get('performance')
  @HttpCode(HttpStatus.OK)
  async getPerformance(@Res() res: Response) {
    try {
      const performance = this.cacheStatsService.getPerformanceMetrics();
      
      res.status(HttpStatus.OK).json({
        status: 'success',
        timestamp: new Date().toISOString(),
        performance,
      });
    } catch (error) {
      res.status(HttpStatus.INTERNAL_SERVER_ERROR).json({
        status: 'error',
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Get cache health status
   */
  @Get('health')
  @HttpCode(HttpStatus.OK)
  async getHealth(@Res() res: Response) {
    try {
      const isHealthy = await this.multiLevelCacheService.isHealthy();
      const l1Stats = this.l1CacheService.getStats();
      const l2Stats = this.l2CacheService.getStats();
      const performance = this.cacheStatsService.getPerformanceMetrics();
      
      const statusCode = isHealthy ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
      
      res.status(statusCode).json({
        status: isHealthy ? 'healthy' : 'unhealthy',
        timestamp: new Date().toISOString(),
        overall: {
          isHealthy,
          hitRate: l1Stats.hitRate + l2Stats.hitRate,
        },
        l1: {
          healthy: l1Stats.hits >= 0,
          hitRate: l1Stats.hitRate,
          currentSize: l1Stats.currentSize,
          evictions: l1Stats.evictions,
        },
        l2: {
          healthy: await this.l2CacheService.isHealthy(),
          hitRate: l2Stats.hitRate,
          errors: l2Stats.errors,
          lastError: l2Stats.lastError,
        },
        issues: performance.health.issues,
        recommendations: performance.health.recommendations,
      });
    } catch (error) {
      res.status(HttpStatus.INTERNAL_SERVER_ERROR).json({
        status: 'error',
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Get cache keys with multi-level support
   */
  @Get('keys')
  @HttpCode(HttpStatus.OK)
  async getKeys(
    @Query('pattern') pattern?: string,
    @Query('level') level?: 'L1' | 'L2' | 'ALL',
    @Query('limit') limit?: number,
  ) {
    try {
      let keys: string[] = [];
      
      switch (level) {
        case 'L1':
          keys = this.l1CacheService.keys();
          if (pattern) {
            const regex = new RegExp(pattern.replace(/\*/g, '.*'));
            keys = keys.filter(key => regex.test(key));
          }
          break;
          
        case 'L2':
          keys = await this.l2CacheService.keys(pattern);
          break;
          
        case 'ALL':
        default:
          const l1Keys = this.l1CacheService.keys();
          const l2Keys = await this.l2CacheService.keys(pattern);
          keys = [...new Set([...l1Keys, ...l2Keys])];
          break;
      }
      
      if (limit && limit > 0) {
        keys = keys.slice(0, limit);
      }
      
      return {
        status: 'success',
        timestamp: new Date().toISOString(),
        level: level || 'ALL',
        pattern: pattern || '*',
        totalKeys: keys.length,
        keys,
      };
    } catch (error) {
      return {
        status: 'error',
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  /**
   * Get cache value with multi-level support
   */
  @Get('value/:key')
  @HttpCode(HttpStatus.OK)
  async getValue(
    @Res() res: Response,
    @Query('level') level?: 'L1' | 'L2' | 'ALL',
  ) {
    try {
      const key = res.req.params?.key;
      
      if (!key) {
        return res.status(HttpStatus.BAD_REQUEST).json({
          status: 'error',
          error: 'Key is required',
        });
      }
      
      let value: any = null;
      let source: string = 'MISS';
      
      switch (level) {
        case 'L1':
          value = this.l1CacheService.get(key);
          source = value !== null ? 'L1' : 'MISS';
          break;
          
        case 'L2':
          const l2Value = await this.l2CacheService.get(key);
          value = l2Value ? JSON.parse(l2Value) : null;
          source = l2Value !== null ? 'L2' : 'MISS';
          break;
          
        case 'ALL':
        default:
          const result = await this.multiLevelCacheService.get(key);
          value = result.value;
          source = result.source;
          break;
      }
      
      if (value === null) {
        return res.status(HttpStatus.NOT_FOUND).json({
          status: 'not_found',
          timestamp: new Date().toISOString(),
          key,
          source,
        });
      }
      
      res.status(HttpStatus.OK).json({
        status: 'success',
        timestamp: new Date().toISOString(),
        key,
        value,
        source,
      });
    } catch (error) {
      res.status(HttpStatus.INTERNAL_SERVER_ERROR).json({
        status: 'error',
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Set cache value with multi-level support
   */
  @Post('value/:key')
  @HttpCode(HttpStatus.OK)
  async setValue(
    @Res() res: Response,
    @Body() body: { value: any; ttl?: number; level?: 'L1' | 'L2' | 'ALL' },
  ) {
    try {
      const key = res.req.params?.key;
      
      if (!key) {
        return res.status(HttpStatus.BAD_REQUEST).json({
          status: 'error',
          error: 'Key is required',
        });
      }
      
      const { value, ttl, level = 'ALL' } = body;
      
      switch (level) {
        case 'L1':
          this.l1CacheService.set(key, value, ttl);
          break;
          
        case 'L2':
          await this.l2CacheService.set(key, JSON.stringify(value), ttl);
          break;
          
        case 'ALL':
        default:
          await this.multiLevelCacheService.set(key, value, ttl);
          break;
      }
      
      res.status(HttpStatus.OK).json({
        status: 'success',
        timestamp: new Date().toISOString(),
        key,
        level,
        ttl: ttl || 'default',
      });
    } catch (error) {
      res.status(HttpStatus.INTERNAL_SERVER_ERROR).json({
        status: 'error',
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Delete cache key with multi-level support
   */
  @Delete('value/:key')
  @HttpCode(HttpStatus.OK)
  async deleteValue(
    @Res() res: Response,
    @Query('level') level?: 'L1' | 'L2' | 'ALL',
  ) {
    try {
      const key = res.req.params?.key;
      
      if (!key) {
        return res.status(HttpStatus.BAD_REQUEST).json({
          status: 'error',
          error: 'Key is required',
        });
      }
      
      let success = false;
      
      switch (level) {
        case 'L1':
          success = this.l1CacheService.delete(key);
          break;
          
        case 'L2':
          success = await this.l2CacheService.delete(key);
          break;
          
        case 'ALL':
        default:
          success = await this.multiLevelCacheService.delete(key);
          break;
      }
      
      res.status(HttpStatus.OK).json({
        status: 'success',
        timestamp: new Date().toISOString(),
        key,
        level: level || 'ALL',
        deleted: success,
      });
    } catch (error) {
      res.status(HttpStatus.INTERNAL_SERVER_ERROR).json({
        status: 'error',
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Clear all cache with multi-level support
   */
  @Delete('clear')
  @HttpCode(HttpStatus.OK)
  async clearCache(
    @Query('level') level?: 'L1' | 'L2' | 'ALL',
  ) {
    try {
      switch (level) {
        case 'L1':
          this.l1CacheService.clear();
          break;
          
        case 'L2':
          await this.l2CacheService.clear();
          break;
          
        case 'ALL':
        default:
          await this.multiLevelCacheService.clear();
          break;
      }
      
      return {
        status: 'success',
        timestamp: new Date().toISOString(),
        level: level || 'ALL',
        cleared: true,
      };
    } catch (error) {
      return {
        status: 'error',
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  /**
   * Configure cache strategy
   */
  @Post('configure')
  @HttpCode(HttpStatus.OK)
  async configureCache(@Body() body: {
    strategy?: CacheStrategy;
    l1Config?: any;
    l2Config?: any;
  }) {
    try {
      if (body.strategy || body.l1Config || body.l2Config) {
        this.multiLevelCacheService.configure(body);
      }
      
      return {
        status: 'success',
        timestamp: new Date().toISOString(),
        configuration: body,
        applied: true,
      };
    } catch (error) {
      return {
        status: 'error',
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  /**
   * Get metrics for a specific key (legacy endpoint)
   */
  @Get('metrics/:key')
  getKeyMetrics(@Param('key') key: string) {
    return this.metricsService.getKeyMetrics(key);
  }

  /**
   * Clear all cache (legacy endpoint)
   */
  @Delete('clear')
  @HttpCode(HttpStatus.NO_CONTENT)
  async clearCache() {
    await this.cacheManager.reset();
  }

  /**
   * Invalidate cache by pattern (legacy endpoint)
   */
  @Delete('pattern/:pattern')
  @HttpCode(HttpStatus.NO_CONTENT)
  async invalidatePattern(@Param('pattern') pattern: string) {
    await this.cacheInvalidator.invalidatePattern(pattern);
  }

  /**
   * Invalidate cache by key (legacy endpoint)
   */
  @Delete('key/:key')
  @HttpCode(HttpStatus.NO_CONTENT)
  async invalidateKey(@Param('key') key: string) {
    await this.cacheInvalidator.invalidateKey(key);
  }

  /**
   * Reset cache metrics (legacy endpoint)
   */
  @Delete('metrics')
  @HttpCode(HttpStatus.NO_CONTENT)
  resetMetrics() {
    this.metricsService.reset();
  }
}
