package org.apache.flink.connector.elasticsearch.table;


import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.elasticsearch.source.ESSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class EsourceTest {
    public static void main(String[] args) {

        String hosts = "http://192.168.173.46:9200";
        String username = "elastic";
        String password = "elastic";
        String index = "my_index";
        String documentType = "_doc";
        Integer fetchSize = 2;

        // 创建 Elasticsearch 连接源的 DeserializationSchema
        DeserializationSchema<RowData> deserializer = null;

        deserializer = new YourElasticsearchDeserializationSchema();


        // 创建 Elasticsearch 连接源
        ESSourceFunction sourceFunction = new ESSourceFunction(
                hosts, username, password, index, documentType, fetchSize, deserializer
        );

        // 添加 Elasticsearch 连接源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> sourceStream = env.addSource(sourceFunction);

        // 打印结果
        sourceStream.print();

        // 执行作业
        try {
            env.execute("Elasticsearch Source Test");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 自定义的 Elasticsearch DeserializationSchema，根据具体数据结构实现反序列化逻辑
    private static class YourElasticsearchDeserializationSchema implements DeserializationSchema<RowData> {

        @Override
        public RowData deserialize(byte[] message) throws IOException {
            // 解析字节数组，生成 RowData 对象
            // 根据具体的数据结构和序列化方式进行解析
            // 返回解析后的 RowData 对象
            // 将字节数组转换为字符串
            String json1 = "{ \"name\": \"name\", \"id\": \"id\",\"age\": \"age\"}";
            message = json1.getBytes(StandardCharsets.UTF_8);

            String json = new String(message, StandardCharsets.UTF_8);

            // 解析 JSON 数据
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(json);

            // 从 JSON 中提取字段值
            String name = rootNode.get("name").asText();
            String id = rootNode.get("id").asText();
            String age = rootNode.get("age").asText();

            // 创建 RowData 对象
            // 定义字段类型
            DataType[] fieldTypes = {
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING()
            };

            // 创建 RowType
            RowType rowType = (RowType) DataTypes.ROW(fieldTypes).getLogicalType();

            // 创建 GenericRowData 对象
            GenericRowData rowData = new GenericRowData(rowType.getFieldCount());


            rowData.setField(0, name);
            rowData.setField(1, id);
            rowData.setField(2, age);


            return rowData;
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            // 判断是否为流的结束
            // 根据具体的数据结构和业务逻辑进行判断
            return false; // 假设流会结束
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            // 定义字段类型和顺序
            TypeInformation<?>[] fieldTypes = {
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
            };

            // 定义字段名称
            String[] fieldNames = {"id", "name", "age"};

            return new RowDataTypeInfo(fieldTypes, fieldNames);
        }
    }
}
