# Amazon Mobile Scraper Docker Setup

This directory contains the Docker setup for running the Amazon mobile phone scraper on a server.

## Files Included

- `Dockerfile.amazon` - Docker configuration for the Amazon scraper
- `docker-compose.amazon.yml` - Docker Compose configuration for easy deployment
- `requirements.txt` - Python dependencies
- `enhanced_amazon_mobile_scraper.py` - The main scraper script
- `expanded_permutations.csv` - Input CSV file with product permutations

## Prerequisites

- Docker installed on your server
- Docker Compose installed (optional, for easier management)

## Quick Start

### Option 1: Using Docker Compose (Recommended)

1. **Build and run the container:**
   ```bash
   docker-compose -f docker-compose.amazon.yml up --build
   ```

2. **Run in background:**
   ```bash
   docker-compose -f docker-compose.amazon.yml up -d --build
   ```

3. **View logs:**
   ```bash
   docker-compose -f docker-compose.amazon.yml logs -f
   ```

4. **Stop the container:**
   ```bash
   docker-compose -f docker-compose.amazon.yml down
   ```

### Option 2: Using Docker directly

1. **Build the image:**
   ```bash
   docker build -f Dockerfile.amazon -t amazon-scraper .
   ```

2. **Run the container:**
   ```bash
   docker run -v $(pwd):/app amazon-scraper
   ```

3. **Run in background:**
   ```bash
   docker run -d -v $(pwd):/app --name amazon-scraper amazon-scraper
   ```

## Configuration

### Environment Variables

The scraper uses the following environment variables:
- `PYTHONUNBUFFERED=1` - Ensures Python output is not buffered
- `PYTHONPATH=/app` - Sets the Python path

### Volumes

The Docker setup mounts the current directory to `/app` in the container, which means:
- Input files (`expanded_permutations.csv`) are accessible
- Output files (results, screenshots, progress) are saved to your local directory
- Progress is preserved between runs

## Output Files

The scraper generates the following files:
- `amazon_mobile_results.json` - Main results in JSON format
- `amazon_mobile_results.csv` - Results in CSV format
- `scraping_progress.json` - Progress tracking for resuming interrupted runs
- `screenshots/` - Debug screenshots (if issues occur)

## Monitoring

### View Real-time Logs
```bash
docker-compose -f docker-compose.amazon.yml logs -f
```

### Check Container Status
```bash
docker ps
```

### Access Container Shell (for debugging)
```bash
docker exec -it amazon-mobile-scraper /bin/bash
```

## Troubleshooting

### Common Issues

1. **Chrome/ChromeDriver Issues:**
   - The Dockerfile includes all necessary Chrome dependencies
   - If you encounter Chrome issues, check the logs for specific error messages

2. **Proxy Issues:**
   - The scraper uses proxy rotation for anti-detection
   - If proxy fails, it will fall back to direct connection

3. **Memory Issues:**
   - The scraper can be memory-intensive
   - Consider increasing Docker memory limits if needed

### Restarting Interrupted Scraping

The scraper automatically saves progress and can resume from where it left off:
1. Simply restart the container
2. The scraper will load `scraping_progress.json` and continue

### Running Without Headless Mode

To run with a visible browser (for debugging):
```bash
docker run -v $(pwd):/app amazon-scraper python3 enhanced_amazon_mobile_scraper.py
```

## Security Notes

- The scraper includes proxy rotation for anti-detection
- It uses undetected-chromedriver to avoid bot detection
- Consider running on a server with good network connectivity
- Monitor for any rate limiting or IP blocking

## Performance Tips

- The scraper processes 5 rows before changing Chrome sessions
- It includes random delays between requests
- Proxy rotation helps avoid IP-based rate limiting
- Consider running during off-peak hours for better performance
