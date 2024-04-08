package libs;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

public class ResetValueFunction extends ScalarFunction {
    // 接受任意类型输入，返回 String 型输出
    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
        return String.valueOf(o)+"yyyyyy";
    }
}
