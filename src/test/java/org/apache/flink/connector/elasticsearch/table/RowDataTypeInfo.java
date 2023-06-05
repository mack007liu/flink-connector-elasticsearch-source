package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.ArrayList;
import java.util.List;

public class RowDataTypeInfo extends TypeInformation<RowData> {
    private final RowType rowType;

    public RowDataTypeInfo(TypeInformation<?>[] fieldTypes, String[] fieldNames) {
        List<RowType.RowField> fields = new ArrayList<>();
        for (int i = 0; i < fieldTypes.length; i++) {
            TypeInformation<?> fieldType = fieldTypes[i];
            LogicalType logicalType = TypeConversions.fromLegacyInfoToDataType(fieldType).getLogicalType();
            fields.add(new RowType.RowField(fieldNames[i], logicalType));
        }
        this.rowType = new RowType(fields);
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return rowType.getFieldCount();
    }

    @Override
    public int getTotalFields() {
        return rowType.getFieldCount();
    }

    @Override
    public Class<RowData> getTypeClass() {
        return RowData.class;
    }

    @Override
    public boolean isKeyType() {
        return true;
    }



    @Override
    public boolean isSortKeyType() {
        return true;
    }

    @Override
    public TypeSerializer<RowData> createSerializer(ExecutionConfig executionConfig) {
        return null;
    }



    @Override
    public String toString() {
        return rowType.asSummaryString();
    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean canEqual(Object o) {
        return false;
    }
}
