FROM memcached:latest
# Optional: Set environment variables for Memcached configuration
ENV MEMCACHED_PORT 11211
ENV MAXCONN 1024
# Expose the Memcached port
EXPOSE $MEMCACHED_PORT
# Run Memcached with custom arguments (optional)
CMD ["memcached", "-m", "128"]
