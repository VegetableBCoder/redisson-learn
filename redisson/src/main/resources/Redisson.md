# 概述

## Redisson是干嘛的,有哪些同类的东西,为什么选择redisson?

* redisson是redis的java api
* redis常用的api还有jedis,lettuce,以及基于前两者封装的redistemplate
* jedis是同步阻塞的api,而 lettuce,redisson支持异步api 
* lettuce和jedis是基础的api封装,而Redisson封装了很多实用的功能,如分布式锁,本地缓存,hkey过期机制等
* redisson通过实现jdk中的接口提供易于理解和学习的api

## Redisson 操作redis api

### List

### ZSET

### 发布订阅

* AsyncSemaphore.java

## 分布式锁

### 非公平锁

### 公平锁

### 读写锁

