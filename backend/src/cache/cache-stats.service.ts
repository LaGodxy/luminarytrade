import { Injectable, Logger } from '@nestjs/common';
import { MultiLevelCacheService, CacheStats } from './multi-level-cache.service';

export interface DetailedCacheStats {
  timestamp: Date;
  l1: {
    hits: number;
    misses: number;
    hitRate: number;
    currentSize: number;
    currentSizeBytes: number;
    evictions: number;
    averageAccessCount: number;
  };
  l2: {
    hits: number;
    misses: number;
    hitRate: number;
    errors: number;
    averageResponseTime: number;
    lastError?: string;
    lastErrorTime?: Date;
  };
  combined: {
    hits: number;
    misses: number;
    hitRate: number;
    totalRequests: number;
    l1HitRate: number;
    l2HitRate: number;
  };
  crossLevelInvalidations: number;
  performance: {
    l1ResponseTime: number;
    l2ResponseTime: number;
    overallResponseTime: number;
  };
  trends: {
    last24Hours: Array<{
      timestamp: Date;
      hitRate: number;
      totalRequests: number;
    }>;
    lastHour: Array<{
      timestamp: Date;
      hitRate: number;
      totalRequests: number;
    }>;
  };
}

@Injectable()
export class CacheStatsService {
  private readonly logger = new Logger(CacheStatsService.name);
  private historicalData: Array<{
    timestamp: Date;
    stats: CacheStats;
  }> = [];
  private maxHistorySize = 1000;

  constructor(private readonly multiLevelCacheService: MultiLevelCacheService) {
    this.startStatsCollection();
  }

  /**
   * Get detailed cache statistics
   */
  getDetailedStats(): DetailedCacheStats {
    const currentStats = this.multiLevelCacheService.getStats();
    const now = new Date();

    return {
      timestamp: now,
      l1: {
        hits: currentStats.l1.hits,
        misses: currentStats.l1.misses,
        hitRate: currentStats.l1.hitRate,
        currentSize: currentStats.l1.currentSize,
        currentSizeBytes: currentStats.l1.currentSizeBytes,
        evictions: currentStats.l1.evictions,
        averageAccessCount: currentStats.l1.averageAccessCount,
      },
      l2: {
        hits: currentStats.l2.hits,
        misses: currentStats.l2.misses,
        hitRate: currentStats.l2.hitRate,
        errors: currentStats.l2.errors,
        averageResponseTime: currentStats.l2.averageResponseTime,
        lastError: currentStats.l2.lastError,
        lastErrorTime: currentStats.l2.lastErrorTime,
      },
      combined: {
        hits: currentStats.combined.hits,
        misses: currentStats.combined.misses,
        hitRate: currentStats.combined.hitRate,
        totalRequests: currentStats.combined.totalRequests,
        l1HitRate: currentStats.combined.l1HitRate,
        l2HitRate: currentStats.combined.l2HitRate,
      },
      crossLevelInvalidations: currentStats.crossLevelInvalidations,
      performance: {
        l1ResponseTime: this.estimateL1ResponseTime(currentStats),
        l2ResponseTime: currentStats.l2.averageResponseTime,
        overallResponseTime: this.calculateOverallResponseTime(currentStats),
      },
      trends: {
        last24Hours: this.get24HourTrends(),
        lastHour: this.getHourTrends(),
      },
    };
  }

  /**
   * Get cache performance metrics
   */
  getPerformanceMetrics() {
    const stats = this.multiLevelCacheService.getStats();
    
    return {
      efficiency: {
        l1Efficiency: stats.l1.hitRate,
        l2Efficiency: stats.l2.hitRate,
        overallEfficiency: stats.combined.hitRate,
      },
      throughput: {
        requestsPerSecond: this.calculateRequestsPerSecond(),
        setsPerSecond: this.calculateSetsPerSecond(),
        deletesPerSecond: this.calculateDeletesPerSecond(),
      },
      memory: {
        l1MemoryUsage: stats.l1.currentSizeBytes,
        l1MemoryUsageFormatted: this.formatBytes(stats.l1.currentSizeBytes),
        l1MemoryEfficiency: this.calculateMemoryEfficiency(stats.l1),
      },
      health: {
        isHealthy: this.isCacheHealthy(stats),
        issues: this.identifyIssues(stats),
        recommendations: this.getRecommendations(stats),
      },
    };
  }

  /**
   * Get cache analytics for dashboard
   */
  getAnalytics() {
    const detailedStats = this.getDetailedStats();
    
    return {
      overview: {
        totalRequests: detailedStats.combined.totalRequests,
        overallHitRate: detailedStats.combined.hitRate,
        crossLevelInvalidations: detailedStats.crossLevelInvalidations,
        lastUpdated: detailedStats.timestamp,
      },
      levels: {
        l1: {
          name: 'Local Memory Cache',
          hitRate: detailedStats.l1.hitRate,
          currentSize: detailedStats.l1.currentSize,
          memoryUsage: detailedStats.l1.currentSizeBytes,
          evictions: detailedStats.l1.evictions,
        },
        l2: {
          name: 'Redis Cache',
          hitRate: detailedStats.l2.hitRate,
          errors: detailedStats.l2.errors,
          averageResponseTime: detailedStats.l2.averageResponseTime,
          lastError: detailedStats.l2.lastError,
        },
      },
      performance: {
        l1ResponseTime: detailedStats.performance.l1ResponseTime,
        l2ResponseTime: detailedStats.performance.l2ResponseTime,
        overallResponseTime: detailedStats.performance.overallResponseTime,
      },
      trends: detailedStats.trends,
    };
  }

  /**
   * Start collecting statistics
   */
  private startStatsCollection(): void {
    // Collect stats every minute
    setInterval(() => {
      this.collectStats();
    }, 60000);

    this.logger.log('Cache statistics collection started');
  }

  /**
   * Collect current statistics
   */
  private collectStats(): void {
    const stats = this.multiLevelCacheService.getStats();
    
    this.historicalData.push({
      timestamp: new Date(),
      stats,
    });

    // Limit history size
    if (this.historicalData.length > this.maxHistorySize) {
      this.historicalData = this.historicalData.slice(-this.maxHistorySize);
    }
  }

  /**
   * Get 24-hour trends
   */
  private get24HourTrends() {
    const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
    
    return this.historicalData
      .filter(data => data.timestamp > twentyFourHoursAgo)
      .map(data => ({
        timestamp: data.timestamp,
        hitRate: data.stats.combined.hitRate,
        totalRequests: data.stats.combined.totalRequests,
      }));
  }

  /**
   * Get hour trends
   */
  private getHourTrends() {
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
    
    return this.historicalData
      .filter(data => data.timestamp > oneHourAgo)
      .map(data => ({
        timestamp: data.timestamp,
        hitRate: data.stats.combined.hitRate,
        totalRequests: data.stats.combined.totalRequests,
      }));
  }

  /**
   * Estimate L1 response time
   */
  private estimateL1ResponseTime(stats: CacheStats): number {
    // L1 is typically 1-5ms
    return Math.max(1, Math.min(5, stats.l1.averageAccessCount * 0.1));
  }

  /**
   * Calculate overall response time
   */
  private calculateOverallResponseTime(stats: CacheStats): number {
    const l1ResponseTime = this.estimateL1ResponseTime(stats);
    const l2ResponseTime = stats.l2.averageResponseTime;
    
    // Weighted average based on hit rates
    const l1Weight = stats.combined.l1HitRate / 100;
    const l2Weight = stats.combined.l2HitRate / 100;
    
    return l1ResponseTime * l1Weight + l2ResponseTime * l2Weight;
  }

  /**
   * Calculate requests per second
   */
  private calculateRequestsPerSecond(): number {
    if (this.historicalData.length < 2) {
      return 0;
    }

    const recent = this.historicalData.slice(-10); // Last 10 data points
    const oldest = recent[0];
    const newest = recent[recent.length - 1];
    
    const timeDiff = (newest.timestamp.getTime() - oldest.timestamp.getTime()) / 1000;
    const requestDiff = newest.stats.combined.totalRequests - oldest.stats.combined.totalRequests;
    
    return timeDiff > 0 ? requestDiff / timeDiff : 0;
  }

  /**
   * Calculate sets per second
   */
  private calculateSetsPerSecond(): number {
    if (this.historicalData.length < 2) {
      return 0;
    }

    const recent = this.historicalData.slice(-10);
    const oldest = recent[0];
    const newest = recent[recent.length - 1];
    
    const timeDiff = (newest.timestamp.getTime() - oldest.timestamp.getTime()) / 1000;
    const setDiff = newest.stats.l1.sets + newest.stats.l2.sets - 
                   (oldest.stats.l1.sets + oldest.stats.l2.sets);
    
    return timeDiff > 0 ? setDiff / timeDiff : 0;
  }

  /**
   * Calculate deletes per second
   */
  private calculateDeletesPerSecond(): number {
    if (this.historicalData.length < 2) {
      return 0;
    }

    const recent = this.historicalData.slice(-10);
    const oldest = recent[0];
    const newest = recent[recent.length - 1];
    
    const timeDiff = (newest.timestamp.getTime() - oldest.timestamp.getTime()) / 1000;
    const deleteDiff = newest.stats.l1.deletes + newest.stats.l2.deletes - 
                     (oldest.stats.l1.deletes + oldest.stats.l2.deletes);
    
    return timeDiff > 0 ? deleteDiff / timeDiff : 0;
  }

  /**
   * Calculate memory efficiency
   */
  private calculateMemoryEfficiency(l1Stats: any): number {
    if (l1Stats.currentSize === 0) {
      return 100;
    }

    // Efficiency based on hit rate vs memory usage
    return l1Stats.hitRate * (1 - (l1Stats.currentSizeBytes / 100 * 1024 * 1024));
  }

  /**
   * Check if cache is healthy
   */
  private isCacheHealthy(stats: CacheStats): boolean {
    const issues = this.identifyIssues(stats);
    return issues.length === 0;
  }

  /**
   * Identify cache issues
   */
  private identifyIssues(stats: CacheStats): string[] {
    const issues: string[] = [];

    if (stats.combined.hitRate < 50) {
      issues.push('Low overall hit rate (< 50%)');
    }

    if (stats.l1.hitRate < 30) {
      issues.push('Low L1 hit rate (< 30%)');
    }

    if (stats.l2.errors > stats.l2.hits * 0.01) {
      issues.push('High L2 error rate (> 1%)');
    }

    if (stats.l2.averageResponseTime > 100) {
      issues.push('High L2 response time (> 100ms)');
    }

    if (stats.l1.evictions > stats.l1.hits * 0.1) {
      issues.push('High L1 eviction rate (> 10% of hits)');
    }

    return issues;
  }

  /**
   * Get recommendations
   */
  private getRecommendations(stats: CacheStats): string[] {
    const recommendations: string[] = [];

    if (stats.combined.hitRate < 50) {
      recommendations.push('Consider increasing cache TTL or cache size');
    }

    if (stats.l1.hitRate < 30) {
      recommendations.push('Consider increasing L1 cache size or warming up hot keys');
    }

    if (stats.l2.averageResponseTime > 100) {
      recommendations.push('Check Redis performance and network latency');
    }

    if (stats.l1.evictions > stats.l1.hits * 0.1) {
      recommendations.push('Consider increasing L1 cache size');
    }

    if (stats.crossLevelInvalidations > stats.combined.totalRequests * 0.05) {
      recommendations.push('High cross-instance invalidation rate - review cache strategy');
    }

    return recommendations;
  }

  /**
   * Format bytes to human readable format
   */
  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  /**
   * Reset statistics
   */
  resetStats(): void {
    this.historicalData = [];
    this.multiLevelCacheService.resetStats();
    this.logger.log('Cache statistics reset');
  }

  /**
   * Export statistics to JSON
   */
  exportStats(): string {
    return JSON.stringify({
      timestamp: new Date(),
      detailedStats: this.getDetailedStats(),
      performanceMetrics: this.getPerformanceMetrics(),
      analytics: this.getAnalytics(),
    }, null, 2);
  }
}
