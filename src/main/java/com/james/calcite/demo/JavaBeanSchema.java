package com.james.calcite.demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/**
* JavaBeanSchema is a type of Optiq Schema that contains a list of tables. A
* table is a List of JavaBean Objects of the same type.
*
* @author Abishek Baskaran
*/
public class JavaBeanSchema extends AbstractSchema {

  static final Logger logger = LoggerFactory.getLogger(JavaBeanSchema.class);
  private String schemaName;
  private Map<String, List> javaBeanListMap = new HashMap<String, List>();

  /**
* Constructor
*
* @param schemaName
* The schema name which is like database name.
*/
  public JavaBeanSchema(String schemaName) {
    super();
    this.schemaName = schemaName;
  }

  /**
* Adds a table to the schema.
*
* @param tableName
* The name of the table, has to be unique else will overwrite.
* @param javaBeanList
* A List of JavaBeans of same type that's to be seen as table.
*/
  public <E> void addAsTable(String tableName, List<E> javaBeanList) {
    javaBeanListMap.put(tableName, javaBeanList);
    logger.info("Added table: " + tableName + " to Schema: " + schemaName);
  }

  /**
* @return The name of the schema
*/
  public String getName() {
    return schemaName;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (String tableName : javaBeanListMap.keySet()) {
      Table javaBeanTable = new JavaBeanTable(javaBeanListMap.get(tableName));
      builder.put(tableName, javaBeanTable);
      logger.debug("Initialized JavaBeanTable for: " + tableName);
    }
    return builder.build();
  }
}