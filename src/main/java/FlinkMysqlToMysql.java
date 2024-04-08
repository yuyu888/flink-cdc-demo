import libs.ResetValueFunction;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkMysqlToMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        // 对数据进行处理并写入数据表b
        tableEnv.executeSql(
                "insert into logtest select id, uid, operate_type,  ResetValue(opeate_detail), create_time from logtest1 where operate_type=1"
        );
    }
}
