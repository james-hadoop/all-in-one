package com.james.calcite.demo;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.thedal.optiq.javabean.utils.JavaBeanInspector;

public class JavaBeanTable<E> extends AbstractQueryableTable implements TranslatableTable {

    static final Logger logger = LoggerFactory.getLogger(JavaBeanTable.class);
    private List<E> javaBeanList;

    /**
     * Constructor
     *
     * @param javaBeanList
     *            A JavaBean List
     */
    public JavaBeanTable(List<E> javaBeanList) {
        super(Object[].class);
        this.javaBeanList = javaBeanList;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        List<String> names = new ArrayList<String>();
        List<RelDataType> types = new ArrayList<RelDataType>();
        if ((javaBeanList != null) && (javaBeanList.size() > 0)) {
            Class sample = javaBeanList.get(0).getClass();
            Method[] methods = sample.getMethods();
            for (Method method : methods) {
                if (JavaBeanInspector.checkMethodEligiblity(method)) {
                    String name = method.getName().substring(3);
                    Class type = method.getReturnType();
                    names.add(name);
                    types.add(typeFactory.createJavaType(type));
                    logger.info("Added field name: " + name + " of type: " + type.getSimpleName());
                }
            }
        }
        return typeFactory.createStructType(Pair.zip(names, types));
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        logger.info("Got query request for: " + tableName);
        return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
            public Enumerator<T> enumerator() {
                // noinspection unchecked
                try {
                    JavaBeanEnumerator enumerator = new JavaBeanEnumerator(javaBeanList);
                    return (Enumerator<T>) enumerator;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
        return new JavaRules.EnumerableTableAccessRel(context.getCluster(),
                context.getCluster().traitSetOf(EnumerableConvention.INSTANCE), relOptTable, (Class) getElementType());
    }
}