# Flipkart Search and Extract - Resource Analysis

## Container Resource Requirements (Without Volumes)

### Memory Usage
- **Base Python 3.10-slim**: ~50-80 MB
- **Chrome Browser**: ~200-400 MB (headless mode)
- **ChromeDriver**: ~50-100 MB
- **Python Dependencies**: ~100-150 MB
- **Application Data**: ~50-100 MB (in-memory processing)
- **Backup Data**: ~10-50 MB (temporary backup storage)
- **Total Estimated Memory**: **460-880 MB**

### CPU Usage
- **Idle State**: 5-15% CPU
- **Active Scraping**: 20-40% CPU (during page loads and processing)
- **Peak Usage**: 50-70% CPU (during Chrome startup and complex page processing)
- **Backup Creation**: 10-20% CPU (every 100 rows)

### Disk Space (Container Image)
- **Base Image**: ~150 MB
- **Chrome Installation**: ~200 MB
- **Python Dependencies**: ~100 MB
- **Application Files**: ~5 MB
- **Total Image Size**: ~455 MB

### Network Usage
- **Proxy Requests**: ~1-5 KB per request
- **Page Downloads**: ~500 KB - 2 MB per page
- **Estimated per 100 rows**: ~50-200 MB (depending on page complexity)

## Resource Optimization Recommendations

### Memory Limits
```yaml
# Recommended Docker Compose configuration
services:
  flipkart-scraper:
    deploy:
      resources:
        limits:
          memory: 1.5G
          cpus: '1.0'
        reservations:
          memory: 800M
          cpus: '0.5'
```

### Container Configuration
```dockerfile
# Add to Dockerfile for memory optimization
ENV MALLOC_ARENA_MAX=2
ENV MALLOC_MMAP_THRESHOLD_=131072
ENV MALLOC_TRIM_THRESHOLD_=131072
ENV MALLOC_TOP_PAD_=131072
ENV MALLOC_MMAP_MAX_=65536
```

## Performance Characteristics

### Processing Speed
- **Average per row**: 10-30 seconds (including delays)
- **100 rows**: ~17-50 minutes
- **1000 rows**: ~3-8 hours
- **4510 rows (full dataset)**: ~12-35 hours

### Memory Growth
- **Linear growth**: ~1-2 KB per processed row
- **Peak memory**: Occurs during Chrome session changes (every 5 rows)
- **Memory cleanup**: Automatic after session changes
- **Backup overhead**: ~10-50 MB during backup creation

### Storage Requirements (Without Volumes)
- **Progress file**: ~1-10 MB (grows with processed rows)
- **Screenshots**: ~100-500 KB each (only on errors)
- **Backup files**: ~1-50 MB each (every 100 rows, auto-cleanup)
- **Temporary files**: ~1-5 MB (progress and temp data)
- **Total storage**: ~50-200 MB for full dataset

## Key Improvements Made

### 1. Atomic Data Structure
- **Before**: Grouped data by model_id, product_name, colour, ram_rom
- **After**: Each CSV row becomes a separate JSON entry
- **Impact**: Better data integrity, easier processing
- **Memory**: Slightly higher (~5-10% increase) due to individual entries

### 2. Backup Management
- **Frequency**: Every 100 rows processed
- **Cleanup**: Previous backup automatically deleted
- **Storage**: Only one backup file at a time
- **Error Backups**: Created on any critical error
- **Memory Impact**: ~10-50 MB during backup creation

### 3. Enhanced Error Handling
- **Retry Limit**: Reduced to 2 attempts (MAX_RETRIES = 2)
- **Critical Error Detection**: Stops on network/connection errors
- **Backup on Error**: Automatic backup creation on failures
- **Session Management**: Chrome session renewal every 5 rows

### 4. Resource Management
- **Session Renewal**: Every 5 rows (memory cleanup)
- **Proxy Rotation**: On network errors
- **Backup Cleanup**: Automatic deletion of previous backups
- **Progress Tracking**: Enhanced with backup count

## Container Deployment Considerations

### Without Volume Mounts
- **Data Loss Risk**: All data lost on container restart
- **Progress Loss**: Scraping restarts from beginning
- **Backup Strategy**: Backups stored in container (lost on restart)
- **Resource Efficiency**: Lower overhead, faster startup
- **Storage Management**: Automatic cleanup prevents disk bloat

### Recommended Approach
1. **Use volume mounts** for data persistence
2. **Set memory limits** to prevent OOM kills
3. **Monitor resource usage** during initial runs
4. **Implement health checks** for container monitoring
5. **Monitor backup creation** for storage management

## Monitoring Commands

### Check Resource Usage
```bash
# Monitor container resources
docker stats flipkart-scraper

# Check memory usage inside container
docker exec flipkart-scraper ps aux --sort=-%mem

# Monitor disk usage
docker exec flipkart-scraper df -h

# Check backup directory
docker exec flipkart-scraper ls -la backups/
```

### Performance Monitoring
```bash
# Check processing speed
docker logs flipkart-scraper | grep "Processing"

# Monitor error rates
docker logs flipkart-scraper | grep "Error"

# Check backup creation
docker logs flipkart-scraper | grep "Backup created"

# Monitor session renewals
docker logs flipkart-scraper | grep "Renewing Chrome session"
```

## Scaling Considerations

### Single Container Limits
- **Maximum rows per day**: ~2000-3000 (with 8-12 hour runtime)
- **Memory pressure**: Increases with session count, managed by renewals
- **Network limits**: Proxy rotation helps avoid rate limits
- **Storage limits**: Automatic backup cleanup prevents disk bloat

### Multi-Container Deployment
- **Parallel processing**: Split CSV into chunks
- **Resource sharing**: Each container needs ~1GB RAM
- **Load balancing**: Distribute by model_id ranges
- **Backup coordination**: Each container manages its own backups

## Error Recovery

### Automatic Recovery
- **Session changes**: Every 5 rows (memory cleanup)
- **Proxy rotation**: On connection errors
- **Backup creation**: Every 100 rows + on errors
- **Retry mechanism**: Max 2 attempts per error
- **Critical error handling**: Stops and creates final backup

### Manual Recovery
- **Progress resumption**: Automatic from last completed row
- **Backup restoration**: Use latest backup file
- **Error investigation**: Debug screenshots on failures
- **Resource monitoring**: Track memory and CPU usage

## Resource Comparison: Before vs After

| Aspect | Before | After | Impact |
|--------|--------|-------|---------|
| Memory Usage | 450-830 MB | 460-880 MB | +10-50 MB |
| Storage | 50-200 MB | 50-200 MB | Same (auto-cleanup) |
| Error Handling | Basic retry | Enhanced with backups | Better reliability |
| Data Structure | Grouped | Atomic | Better integrity |
| Backup Strategy | None | Every 100 rows | Data safety |
| Session Management | Every 5 rows | Every 5 rows | Same |
| Retry Limit | 3 attempts | 2 attempts | Faster failure detection |

## Recommendations for Container Deployment

### Minimum Requirements
- **Memory**: 1GB (with 800MB reservation)
- **CPU**: 0.5 cores (with 0.25 reservation)
- **Storage**: 500MB (for full dataset + backups)
- **Network**: Stable internet connection

### Optimal Configuration
- **Memory**: 1.5GB (with 800MB reservation)
- **CPU**: 1.0 core (with 0.5 reservation)
- **Storage**: 1GB (with volume mount)
- **Network**: High-speed connection with proxy support

### Production Deployment
- **Use volume mounts** for data persistence
- **Monitor resource usage** continuously
- **Set up alerts** for memory/CPU spikes
- **Implement health checks** for container monitoring
- **Schedule regular backups** to external storage
- **Use load balancing** for multiple containers

