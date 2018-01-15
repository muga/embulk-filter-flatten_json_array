package org.embulk.filter.flatten_json_array;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.JsonPathException;
import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.ReadContext;
import org.embulk.config.Config;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfigException;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.ArrayList;
import java.util.List;

public class FlattenJsonArrayFilterPlugin
        implements FilterPlugin {

    public interface PluginTask
            extends Task {
        @Config("json_column")
        public String getJsonColumn();

        @Config("flattened_json_path")
        public String getFlattenedJsonPath(); // json path
    }

    @Override
    public void transaction(final ConfigSource config,
                            final Schema inputSchema,
                            final FilterPlugin.Control control) {
        final PluginTask task = config.loadConfig(PluginTask.class);
        validatePluginTask(task, inputSchema);
        control.run(task.dump(), inputSchema);
    }

    private void validatePluginTask(final PluginTask task, final Schema inputSchema) {
        final Column column = inputSchema.lookupColumn(task.getJsonColumn());
        if (!column.getType().equals(Types.JSON) && !column.getType().equals(Types.STRING)) {
            throw new SchemaConfigException("A column specified as json_column: must be 'json' type.");
        }

        final String jsonPath = task.getFlattenedJsonPath();
        try {
            createJsonPath(jsonPath);
        } catch (JsonPathException e) {
            throw new ConfigException("Invalid flattened_json_path: string: " + jsonPath);
        }
    }

    private JsonPath createJsonPath(final String jsonPath) {
        return JsonPath.compile(jsonPath, new Predicate[0]);
    }

    @Override
    public PageOutput open(final TaskSource taskSource,
                           final Schema inputSchema,
                           final Schema outputSchema,
                           final PageOutput pageOutput) {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final Column flattening = inputSchema.lookupColumn(task.getJsonColumn());
        final JsonPath jsonPath = createJsonPath(task.getFlattenedJsonPath());
        final ColumnFlattener columnFlattener = new ColumnFlattener(flattening, jsonPath);
        return new FilteredPageOutput(columnFlattener, inputSchema, outputSchema, pageOutput);
    }

    private static class ColumnFlattener {
        private final Column flattening;
        private final JsonPath jsonPath;

        private ColumnFlattener(final Column flattening, final JsonPath jsonPath) {
            this.flattening = flattening;
            this.jsonPath = jsonPath;
        }

        private int getColumnIndex() {
            return this.flattening.getIndex();
        }

        private Object extractJsonObject(final ReadContext from) {
            return from.read(jsonPath);
        }
    }

    private static class FilteredPageOutput
            implements PageOutput {

        private final ColumnFlattener columnFlattener;
        private final Schema inputSchema;
        private final Schema outputSchema;

        private final PageReader pageReader;
        private final PageBuilder pageBuilder;
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final JsonParser jsonParser = new JsonParser();

        private FilteredPageOutput(final ColumnFlattener columnFlattener,
                                   final Schema inputSchema,
                                   final Schema outputSchema,
                                   final PageOutput pageOutput) {
            this.columnFlattener = columnFlattener;
            this.inputSchema = inputSchema;
            this.outputSchema = outputSchema;
            this.pageReader = new PageReader(inputSchema);
            this.pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, pageOutput);
        }

        private List<Value> flatJsonArray() {
            if (pageReader.isNull(columnFlattener.getColumnIndex())) {
                return Lists.<Value>newArrayList((Value)null);
            }

            final Value original = pageReader.getJson(columnFlattener.getColumnIndex());
            final String jsonString = original.toJson();
            if (Strings.isNullOrEmpty(jsonString)) {
                return Lists.<Value>newArrayList(original);
            }

            // TODO need to optimize
            final List<Value> flattened = new ArrayList<>();
            final DocumentContext json = JsonPath.parse(jsonString);
            final Object value = columnFlattener.extractJsonObject(json);
            if (!(value instanceof List)) {
                return Lists.<Value>newArrayList(original);
            }

            ((List)value).forEach(elementObject -> {
                Value element;
                try {
                    final String elementString = objectMapper.writeValueAsString(elementObject);
                    element = jsonParser.parse(elementString);
                } catch (JsonProcessingException e) {
                    element = ValueFactory.newNil();
                }
                flattened.add(element);
            });
            return flattened;
        }

        @Override
        public void add(final Page page) {
            pageReader.setPage(page);
            while (pageReader.nextRecord()) {
                final List<Value> values = flatJsonArray();
                values.forEach(value -> {
                    inputSchema.visitColumns(new ColumnVisitor() {
                        @Override
                        public void booleanColumn(Column column) {
                            final int i = column.getIndex();
                            if (pageReader.isNull(i)) {
                                pageBuilder.setNull(i);
                            } else {
                                pageBuilder.setBoolean(i, pageReader.getBoolean(i));
                            }
                        }

                        @Override
                        public void longColumn(Column column) {
                            final int i = column.getIndex();
                            if (pageReader.isNull(i)) {
                                pageBuilder.setNull(i);
                            } else {
                                pageBuilder.setLong(i, pageReader.getLong(i));
                            }
                        }

                        @Override
                        public void doubleColumn(Column column) {
                            final int i = column.getIndex();
                            if (pageReader.isNull(i)) {
                                pageBuilder.setNull(i);
                            } else {
                                pageBuilder.setDouble(i, pageReader.getDouble(i));
                            }
                        }

                        @Override
                        public void stringColumn(Column column) {
                            final int i = column.getIndex();
                            if (pageReader.isNull(i)) {
                                pageBuilder.setNull(i);
                            } else {
                                pageBuilder.setString(i, pageReader.getString(i));
                            }
                        }

                        @Override
                        public void timestampColumn(Column column) {
                            final int i = column.getIndex();
                            if (pageReader.isNull(i)) {
                                pageBuilder.setNull(i);
                            } else {
                                pageBuilder.setTimestamp(i, pageReader.getTimestamp(i));
                            }
                        }

                        @Override
                        public void jsonColumn(Column column) {
                            final int i = column.getIndex();
                            if (i != columnFlattener.flattening.getIndex()) {
                                if (pageReader.isNull(i)) {
                                    pageBuilder.setNull(i);
                                } else {
                                    pageBuilder.setJson(i, pageReader.getJson(i));
                                }
                            } else {
                                if (value == null) {
                                    pageBuilder.setNull(column);
                                } else {
                                    pageBuilder.setJson(i, value);
                                }
                            }
                        }
                    });

                    pageBuilder.addRecord();
                });
            }
        }

        @Override
        public void finish() {
            pageBuilder.finish();
        }

        @Override
        public void close() {
            pageReader.close();
            pageBuilder.close();
        }
    }
}
