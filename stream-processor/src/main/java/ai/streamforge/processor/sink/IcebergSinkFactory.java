package ai.streamforge.processor.sink;

import ai.streamforge.processor.model.UserEventCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds and attaches an Apache Iceberg sink to a {@link UserEventCount} stream.
 *
 * <p>Supported catalog types: {@code hadoop} (default), {@code hive}, {@code rest}.
 * For MinIO / S3-compatible storage set the {@code ICEBERG_S3_*} environment variables
 * so the Hadoop S3A filesystem is configured automatically.
 */
public class IcebergSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkFactory.class);

    static final Schema TABLE_SCHEMA = new Schema(
            Types.NestedField.required(1, "user_id",         Types.StringType.get()),
            Types.NestedField.required(2, "event_count",     Types.LongType.get()),
            Types.NestedField.required(3, "window_start_ms", Types.LongType.get()),
            Types.NestedField.required(4, "window_end_ms",   Types.LongType.get())
    );

    /**
     * Converts {@code stream} to Iceberg {@link RowData} and appends it to the
     * specified Iceberg table, creating the namespace and table if absent.
     */
    public static void attach(
            DataStream<UserEventCount> stream,
            String catalogType,
            String warehouse,
            String database,
            String tableName,
            String s3Endpoint,
            String s3AccessKey,
            String s3SecretKey) {

        Configuration hadoopConf = buildHadoopConf(s3Endpoint, s3AccessKey, s3SecretKey);
        CatalogLoader catalogLoader = buildCatalogLoader(catalogType, warehouse, hadoopConf);
        TableIdentifier tableId = TableIdentifier.of(database, tableName);

        ensureTable(catalogLoader, tableId, database);

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        DataStream<RowData> rowData = stream
                .map((MapFunction<UserEventCount, RowData>) e -> {
                    GenericRowData row = new GenericRowData(4);
                    row.setField(0, StringData.fromString(e.userId));
                    row.setField(1, e.count);
                    row.setField(2, e.windowStartMs);
                    row.setField(3, e.windowEndMs);
                    return row;
                })
                .returns(TypeInformation.of(RowData.class))
                .name("Map to Iceberg RowData");

        FlinkSink.forRowData(rowData)
                .tableLoader(tableLoader)
                .append()
                .name("Iceberg Sink: " + database + "." + tableName);

        LOG.info("Iceberg sink attached: catalog={}, warehouse={}, table={}.{}",
                catalogType, warehouse, database, tableName);
    }

    private static CatalogLoader buildCatalogLoader(
            String catalogType, String warehouse, Configuration hadoopConf) {
        Map<String, String> props = new HashMap<>();
        props.put("warehouse", warehouse);
        return switch (catalogType.toLowerCase()) {
            case "hadoop" -> CatalogLoader.hadoop("streamforge", hadoopConf, props);
            case "hive"   -> CatalogLoader.hive("streamforge", hadoopConf, props);
            case "rest"   -> {
                props.put("uri", warehouse);
                yield CatalogLoader.rest("streamforge", hadoopConf, props);
            }
            default -> throw new IllegalArgumentException(
                    "Unsupported Iceberg catalog type: " + catalogType
                    + ". Supported: hadoop, hive, rest");
        };
    }

    private static void ensureTable(
            CatalogLoader catalogLoader, TableIdentifier tableId, String database) {
        Catalog catalog = catalogLoader.loadCatalog();
        Namespace ns = Namespace.of(database);
        if (!catalog.namespaceExists(ns)) {
            catalog.createNamespace(ns);
        }
        if (!catalog.tableExists(tableId)) {
            catalog.createTable(tableId, TABLE_SCHEMA, PartitionSpec.unpartitioned());
            LOG.info("Created Iceberg table {}", tableId);
        }
    }

    private static Configuration buildHadoopConf(
            String s3Endpoint, String s3AccessKey, String s3SecretKey) {
        Configuration conf = new Configuration();
        if (s3Endpoint != null && !s3Endpoint.isBlank()) {
            conf.set("fs.s3a.endpoint",          s3Endpoint);
            conf.set("fs.s3a.access.key",        s3AccessKey != null ? s3AccessKey : "");
            conf.set("fs.s3a.secret.key",        s3SecretKey != null ? s3SecretKey : "");
            conf.set("fs.s3a.path.style.access", "true");
            conf.set("fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem");
        }
        return conf;
    }
}
