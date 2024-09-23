# # 使用 redis:alpine 作为基础镜像
# FROM redis:alpine

# # 更新 apk 包索引并安装 bash
# RUN apk update && apk add --no-cache bash && mkdir -p /usr/local/var/db/redis/
# # 复制 Redis 配置文件到容器中（可选，根据需要）
# COPY redis.conf /usr/local/etc/redis/redis.conf

# # 设置容器启动时的默认命令
# CMD ["redis-server"]

# 使用 Ubuntu 作为基础镜像
FROM ubuntu:latest

# 更新包索引并安装 Redis
RUN apt-get update && \
    apt-get install -y redis-server netcat-openbsd  && \
    mkdir -p /usr/local/var/db/redis/ && \
    mkdir -p /usr/local/var/db/redis-cluster/
    
