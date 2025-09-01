# Amazon Mobile Scraper - Resource Analysis

## Container Resource Requirements (Without Volumes)

### Memory Usage
- **Base Python 3.10-slim**: ~50-80 MB
- **Chrome Browser**: ~200-400 MB (headless mode)
- **ChromeDriver**: ~50-100 MB
- **Python Dependencies**: ~100-150 MB
- **Application Data**: ~50-100 MB (in-memory processing)
- **Total Estimated Memory**: **450-830 MB**

### CPU Usage
- **Idle State**: 5-15% CPU
- **Active Scraping**: 20-40% CPU (during page loads and processing)
- **Peak Usage**: 50-70% CPU (during Chrome startup and complex page processing)

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
  amazon-scraper:
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
- **4218 rows (full dataset)**: ~12-35 hours

### Memory Growth
- **Linear growth**: ~1-2 KB per processed row
- **Peak memory**: Occurs during Chrome session changes
- **Memory cleanup**: Automatic after session changes

### Storage Requirements (Without Volumes)
- **Progress file**: ~1-10 MB (grows with processed rows)
- **Screenshots**: ~100-500 KB each (only on errors)
- **Backup files**: ~1-50 MB each (every 100 rows)
- **Total storage**: ~50-200 MB for full dataset

## Container Deployment Considerations

### Without Volume Mounts
- **Data Loss Risk**: All data lost on container restart
- **Progress Loss**: Scraping restarts from beginning
- **Backup Strategy**: Backups stored in container (lost on restart)
- **Resource Efficiency**: Lower overhead, faster startup

### Recommended Approach
1. **Use volume mounts** for data persistence
2. **Set memory limits** to prevent OOM kills
3. **Monitor resource usage** during initial runs
4. **Implement health checks** for container monitoring

## Monitoring Commands

### Check Resource Usage
```bash
# Monitor container resources
docker stats amazon-mobile-scraper

# Check memory usage inside container
docker exec amazon-mobile-scraper ps aux --sort=-%mem

# Monitor disk usage
docker exec amazon-mobile-scraper df -h
```

### Performance Monitoring
```bash
# Check processing speed
docker logs amazon-mobile-scraper | grep "Processing"

# Monitor error rates
docker logs amazon-mobile-scraper | grep "Error"

# Check backup creation
docker logs amazon-mobile-scraper | grep "Backup created"
```

## Scaling Considerations

### Single Container Limits
- **Maximum rows per day**: ~2000-3000 (with 8-12 hour runtime)
- **Memory pressure**: Increases with session count
- **Network limits**: Proxy rotation helps avoid rate limits

### Multi-Container Deployment
- **Parallel processing**: Split CSV into chunks
- **Resource sharing**: Each container needs ~1GB RAM
- **Load balancing**: Distribute by model_id ranges

## Error Recovery

### Automatic Recovery
- **Session changes**: Every 5 rows (memory cleanup)
- **Proxy rotation**: On connection errors
- **Backup creation**: Every 100 rows + on errors
- **Retry mechanism**: Max 2 attempts per error

### Manual Recovery
- **Container restart**: Resumes from last progress
- **Backup restoration**: From latest backup file
- **Error investigation**: Screenshots saved for debugging
