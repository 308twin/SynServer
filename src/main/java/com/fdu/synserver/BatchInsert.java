package com.fdu.synserver;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
@Component
public class BatchInsert {
    
    //@PostConstruct
    public void insertTest(){
        String url = "jdbc:mysql://localhost:3306/block_chain";
        String user = "root";
        String password = "199795";
        String sql = "INSERT INTO test_table (`key`, `update_time_on_chain`) VALUES (?, ?)";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);  // 开启事务

            // 批量插入 1000 条数据
            for (int key = 1; key <= 10000; key++) {
                pstmt.setInt(1, key);
                pstmt.setTimestamp(2, new java.sql.Timestamp(System.currentTimeMillis()));
                pstmt.addBatch();  // 添加到批处理中

                if (key % 1000 == 0) {  // 每 100 条执行一次
                    pstmt.executeBatch();
                    conn.commit();
                    System.out.println("已插入 " + key + " 条数据");
                }
            }

            // 执行并提交剩余数据
            pstmt.executeBatch();
            conn.commit();

            System.out.println("插入完成");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
