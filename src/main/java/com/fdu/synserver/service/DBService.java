package com.fdu.synserver.service;

import java.util.List;
import java.util.Queue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class DBService {
    @Autowired
    private JdbcTemplate jdbcTemplate;
    // 用于缓存SQL语句的线程安全队列
    private final Queue<String> sqlCache = new ConcurrentLinkedQueue<>();
    private final int BATCH_SIZE = 10000;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    // 执行查询来获取所有的表名
    public List<String> getAllTableNames() {
        String sql = "SHOW TABLES";
        return jdbcTemplate.queryForList(sql, String.class);
    }


    // 初始化调度程序，每隔5秒执行一次批量操作
    public DBService() {
        scheduler.scheduleAtFixedRate(this::executeBatch, 5, 5, TimeUnit.SECONDS);
    }

    // 添加SQL语句到缓存中
    public void addSQL(String sql) {
        sqlCache.add(sql);
        // 如果缓存达到批量大小，立即执行批处理
        if (sqlCache.size() >= BATCH_SIZE) {
            executeBatch();
        }
    }

    // 批量执行缓存的SQL语句
    private void executeBatch() {
        if (!sqlCache.isEmpty()) {
            List<String> batch;
            synchronized (this) {
                batch = List.copyOf(sqlCache);
                sqlCache.clear();
            }
            try {
                jdbcTemplate.batchUpdate(batch.toArray(new String[0]));
                System.out.println("Batch executed with " + batch.size() + " statements.");
            } catch (Exception e) {
                System.err.println("Error executing batch: " + e.getMessage());
                // 这里可以添加错误处理逻辑，例如重试或记录到失败队列中
            }
        }
    }
}
