package com.cognitube.consumer.typehandler;

import com.cognitube.consumer.enums.BaseEnum;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-backend
 * @description Generic enum hanlder for MyBatis enum mapping
 * @date 2024/5/23 20:32:39
 */
public class GenericEnumTypeHandler<E extends Enum<E> & BaseEnum> extends BaseTypeHandler<E> {
    private final Class<E> type;

    public GenericEnumTypeHandler(Class<E> type) {
        if (type == null) throw new IllegalArgumentException("Type argument cannot be null");
        this.type = type;
    }

    @Override
    public void setNonNullParameter(PreparedStatement ps, int i, E parameter, JdbcType jdbcType) throws SQLException {
        ps.setInt(i, parameter.getValue());
    }

    @Override
    public E getNullableResult(ResultSet rs, String columnName) throws SQLException {
        int value = rs.getInt(columnName);
        return getEnum(value);
    }

    @Override
    public E getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
        int value = rs.getInt(columnIndex);
        return getEnum(value);
    }

    @Override
    public E getNullableResult(java.sql.CallableStatement cs, int columnIndex) throws SQLException {
        int value = cs.getInt(columnIndex);
        return getEnum(value);
    }

    private E getEnum(int value) {
        for (E enumConstant : type.getEnumConstants()) {
            if (enumConstant.getValue() == value) {
                return enumConstant;
            }
        }
        throw new IllegalArgumentException("Unknown enum value: " + value);
    }
}
