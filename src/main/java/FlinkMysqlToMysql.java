import libs.ResetValueFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

public class FlinkMysqlToMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // flink程序在开发环境已经运行成功的情况下，部署到独立的flink集群（start-cluster）中，可能遇到不能正常运行的情况。
//        env.setRestartStrategy(RestartStrategies.noRestart()); // 不重启
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, // 一个时间段内的最大失败次数
//                Time.of(5, TimeUnit.MINUTES), // 衡量失败次数的是时间段
//                Time.of(3, TimeUnit.SECONDS) // 间隔
//        ));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name", "mysql2mysql-test");


        tableEnv.createTemporarySystemFunction("ResetValue", ResetValueFunction.class);

        // 从数据表a读取数据
        tableEnv.executeSql(
                "CREATE TABLE `logtest1`("
                        + "  `id` INT NOT NULL,"
                        + "  `uid` INT NOT NULL,"
                        + "  `operate_type` INT NOT NULL,"
                        + "  `opeate_detail` STRING NOT NULL,"
                        + "  `create_time` TIMESTAMP NOT NULL,"
                        + "  PRIMARY KEY (`id`) NOT ENFORCED "
                        + ") WITH ("
                        + " 'connector' = 'mysql-cdc', "
                        + " 'hostname' = '127.0.0.1',"
                        + " 'port' = '3306', "
                        + " 'username' = 'root', "
                        + "  'password' = '123456', "
                        + " 'database-name' = 'mytest', "
                        + " 'table-name' = 'logtest1' "
                        + ")"
        );

        // 将数据写入数据表b
        tableEnv.executeSql(
                "CREATE TABLE `logtest`("
                        + "  `id` INT NOT NULL, "
                        + "  `uid` INT NOT NULL, "
                        + "  `operate_type` INT NOT NULL, "
                        + "  `opeate_detail` STRING NOT NULL,"
                        + "  `create_time` TIMESTAMP NOT NULL,"
                        + "  PRIMARY KEY (`id`) NOT ENFORCED "
                        + ") WITH ("
                        // 这里添加你的数据库连接参数
                        + "  'connector' = 'jdbc', "
                        + "  'url' = 'jdbc:mysql://127.0.0.1:3306/test?characterEncoding=utf8&useSSL=true&serverTimezone=Asia/Shanghai', "
                        + "  'driver' = 'com.mysql.cj.jdbc.Driver', "
                        + "  'username' = 'root', "
                        + "  'password' = '123456', "
                        + "  'table-name' = 'logtest' "
                        + ")"
        );


//        StatementSet statementSet = tableEnv.createStatementSet();
//        statementSet.addInsertSql(
//                "insert into logtest select id, uid, operate_type,  ResetValue(opeate_detail), create_time from logtest1 where operate_type=1"
//        );
//        statementSet.execute();

        // 对数据进行处理并写入数据表b
        tableEnv.executeSql(
                "insert into logtest select id, uid, operate_type,  ResetValue(opeate_detail), create_time from logtest1 where operate_type=1"
        );
    }
}
