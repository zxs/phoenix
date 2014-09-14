package org.apache.phoenix.flume.serializer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.flume.DefaultKeyGenerator;
import org.apache.phoenix.flume.FlumeConstants;
import org.apache.phoenix.flume.KeyGenerator;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;

import static org.apache.phoenix.util.PhoenixRuntime.UPSERT_BATCH_SIZE_ATTRIB;

/**
 * Created by shenlets on 8/3/14.
 */
public class AvroEventSerializer implements EventSerializer {
    private static final Logger logger = LoggerFactory.getLogger(AvroEventSerializer.class);
    private Connection connection;
    private String jdbcUrl;
    private Integer batchSize;

    private String schemaKey;
    private String tableKey;
    private String appendRowkey;
    private KeyGenerator keyGenerator;
    private boolean insertUpdateTime = false;
    @Override
    public void configure(Context context) {
        final String zookeeperQuorum = context.getString(FlumeConstants.CONFIG_ZK_QUORUM);
        final String ipJdbcURL = context.getString(FlumeConstants.CONFIG_JDBC_URL);
        this.batchSize = context.getInteger(FlumeConstants.CONFIG_BATCHSIZE, FlumeConstants.DEFAULT_BATCH_SIZE);
        this.schemaKey = context.getString(FlumeConstants.CONFIG_SCHEMA_KEY);
        this.tableKey = context.getString(FlumeConstants.CONFIG_TABLE_KEY);
        this.appendRowkey = context.getString(FlumeConstants.CONFIG_APPEND_ROWKEY);
        this.insertUpdateTime = context.getBoolean(FlumeConstants.CONFIG_INSERT_UPDATE_TIME, false);

        if (!Strings.isNullOrEmpty(zookeeperQuorum)) {
            this.jdbcUrl = QueryUtil.getUrl(zookeeperQuorum);
        }
        if (!Strings.isNullOrEmpty(ipJdbcURL)) {
            this.jdbcUrl = ipJdbcURL;
        }

        if(!Strings.isNullOrEmpty(appendRowkey)) {
            try {
                keyGenerator =  DefaultKeyGenerator.valueOf(appendRowkey.toUpperCase());
            } catch(IllegalArgumentException iae) {
                logger.error("An invalid key generator {} was specified in configuration file. Specify one of {}",appendRowkey,DefaultKeyGenerator.values());
                Throwables.propagate(iae);
            }
        }
        Preconditions.checkNotNull(this.schemaKey,"Schema Key on Event Headers cannot be empty, please specify in the configuration file");
        Preconditions.checkNotNull(this.tableKey,"Table Key on Event Headers cannot be empty, please specify in the configuration file");

        logger.debug(" the jdbcUrl configured is {}", jdbcUrl);
    }

    @Override
    public void configure(ComponentConfiguration conf) {
        // NO-OP
    }

    @Override
    public void initialize() throws SQLException {
        final Properties props = new Properties();
        props.setProperty(UPSERT_BATCH_SIZE_ATTRIB, String.valueOf(this.batchSize));
        ResultSet rs = null;
        try {
            this.connection = DriverManager.getConnection(this.jdbcUrl, props);
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            logger.error("error {} occurred during initializing connection ", e.getMessage());
            throw e;
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

    @Override
    public void upsertEvents(List<Event> events) throws SQLException {
        Preconditions.checkNotNull(events);
        Preconditions.checkNotNull(connection);

        boolean wasAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        String table = null;
        try {
            PreparedStatement pstat = null;
            for (Event event : events) {
                table = event.getHeaders().get(tableKey);
                String avscUrl = event.getHeaders().get(schemaKey);

                AvroInfo avroInfo = avroInfoMap.get(table);
                if (avroInfo != null && !avroInfo.getAvscUrl().equals(avscUrl)) {
                    avroInfo.handleSchemaChange(avscUrl);
                    avroInfo = null; // reset to null , for create new one
                }
                if (avroInfo == null) {
                    avroInfo = new AvroInfo(jdbcUrl, table, avscUrl, keyGenerator, insertUpdateTime);
                    avroInfoMap.put(table, avroInfo);
                }

                GenericRecord rec = readAvroData(avroInfo.getSchema(), event.getBody());
                pstat = avroInfo.getUpsertStatement(connection);

                int index = 1;
                ColumnInfo cInfo = null;
                int sqlType;
                String value;
                Object upsertValue = null, tmp = null;
                int  size = avroInfo.getColumnMetadata().size();
                for (int i = 0; i < (keyGenerator!=null?size-1:size); i++) {
                    cInfo = avroInfo.getColumnMetadata().get(i);
                    if (cInfo == null) {
                        continue;
                    }
                    sqlType = cInfo.getSqlType();
                    if(cInfo.getColumnName().equalsIgnoreCase(FlumeConstants.UPDATE_TIME)) {
                        upsertValue = Long.valueOf(new Date().getTime());
                    } else {
                        tmp = rec.get(cInfo.getColumnName());
                        value = tmp != null ? tmp.toString() : null;
                        upsertValue = PDataType.fromTypeId(sqlType).toObject(value);
                    }
                    logger.debug("set c:{}, p:{}, t:{}, uv:{}", new Object[]{cInfo.getColumnName(),index, sqlType,  upsertValue});
                    if (upsertValue != null) {
                        pstat.setObject(index++, upsertValue, sqlType);
                    } else {
                        pstat.setNull(index++, sqlType);
                    }
                }
                if(keyGenerator!=null) {
                    if(keyGenerator == DefaultKeyGenerator.EVENT_HEAD_KEY) {
                        pstat.setString(index, event.getHeaders().get(keyGenerator.generate()));
                    } else {
                        pstat.setString(index, keyGenerator.generate());
                    }
                }
                pstat.execute();
                //pstat.addBatch();
            }
            connection.commit();
        } catch (Exception ex) {
            logger.error("An error {} occurred during persisting the event [{}]", table+"<:"+ex.getMessage(), ex);
            throw new SQLException(ex.getMessage());
        } finally {
            if (wasAutoCommit) {
                connection.setAutoCommit(true);
            }
        }
    }

    private GenericRecord readAvroData(Schema schema, byte[] body) throws IOException {
        GenericDatumReader<GenericRecord> serveReader = new GenericDatumReader<GenericRecord>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(body, null /* reuse */);
        return serveReader.read(null /* reuse */, decoder);
    }

    @Override
    public void close() throws SQLException {

    }

    private Map<String, AvroInfo> avroInfoMap = new HashMap<String, AvroInfo>();

    private class AvroInfo {
        private String jdbcUrl;
        private String avscUrl;
        private String tblName;
        private String schemaName;
        private boolean insertUpdateTime = false;
        private KeyGenerator keyGenerator;

        private Schema schema;
        private List<ColumnInfo> columnMetadata = null;
        private PreparedStatement upsertPrepareStatement = null;

        public AvroInfo(String jdbcUrl, String tblName, String avscUrl, KeyGenerator keyGenerator, boolean insertUpdateTime) throws SQLException, IOException {
            this.jdbcUrl = jdbcUrl;
            this.tblName = tblName;
            this.avscUrl = avscUrl;
            this.keyGenerator = keyGenerator;
            this.insertUpdateTime = insertUpdateTime;
            int p = getSchema().getNamespace().lastIndexOf(".");
            schemaName = getSchema().getNamespace().substring(p+1);
            createPhoenixTable();
        }

        public List<ColumnInfo> getColumnMetadata() throws SQLException {
            if (columnMetadata == null) {
                columnMetadata = new ArrayList<ColumnInfo>();
                String rowkey = null;
                String cq = null;
                String cf = null;
                Integer dt = null;
                String columnName;
                Connection conn = DriverManager.getConnection(jdbcUrl);
                ResultSet rs = conn.getMetaData().getColumns("", StringUtil.escapeLike(schemaName), StringUtil.escapeLike(tblName), null);
                while (rs.next()) {
//                    cf = rs.getString(QueryUtil.COLUMN_FAMILY_POSITION);
                    cq = rs.getString(QueryUtil.COLUMN_NAME_POSITION);
                    dt = rs.getInt(QueryUtil.DATA_TYPE_POSITION);
//                    if (Strings.isNullOrEmpty(cf)) {
//                        rowkey = cq; // this is required only when row key is auto generated
//                        columnName = SchemaUtil.getColumnDisplayName(null, cq);
//                    } else {
//                        columnName = SchemaUtil.getColumnDisplayName(cf, cq);
//                    }
                    columnName = SchemaUtil.getColumnDisplayName(null, cq);
                    logger.info("schemaName={}, tblName={}, columnName={}, columnType={}",
                            new Object[]{schemaName, tblName, columnName, dt} );
                    columnMetadata.add(new ColumnInfo(columnName, dt));
                }
                rs.close();
                conn.close();
            }
            return columnMetadata;
        }


        public String getAvscUrl() {
            return avscUrl;
        }

        public Schema getSchema() throws IOException {
            if (schema == null) {
                schema = getSchema(avscUrl);
            }
            return schema;
        }

        public PreparedStatement getUpsertStatement(Connection conn) throws SQLException {
            if(upsertPrepareStatement==null) {
                String upsertSQL = QueryUtil.constructUpsertStatement("\"" +schemaName+"\".\""+ tblName + "\"", getColumnMetadata());
                logger.debug("upsertSQL=\n---\n {} \n---", upsertSQL);
                upsertPrepareStatement = conn.prepareStatement(upsertSQL);
            }
            return upsertPrepareStatement;
        }

        public void handleSchemaChange(String newAvscUrl) throws SQLException, IOException {
            //创建备份表，复制原数据 TODO: Snapshot or Rename
            //删除原始表，创建新表

            dropPhoenixTable();
        }

        private void createPhoenixTable() throws SQLException, IOException {
            Connection conn = DriverManager.getConnection(jdbcUrl);
            conn.setAutoCommit(true);
            List<Schema.Field> fs = getSchema().getFields();
            String pkStr = getSchema().getProp("phoenix.primaryKeys");
            if(pkStr==null && keyGenerator==null) {
                throw new IllegalArgumentException(" The Schema[" + getSchema().getName() + "] MUST BE HAS the property[phoenix.primaryKeys]" +
                        ", OR config the paramater <appendRowkey> ") ;
            }
            String tableOptions = getSchema().getProp("phoenix.tableOptions");
            // NORM. PK
            String[] pAy = null;
            List<String> pks = new ArrayList<String>();
            StringBuffer primaryKeys = new StringBuffer();
            if(pkStr!=null) {
                for (String p : Arrays.asList(pkStr.split(","))) {
                    pAy = p.trim().split(" ");
                    pks.add(pAy[0].trim());
                    primaryKeys.append(",\"").append(pAy[0].trim()).append("\"");
                    if (pAy.length > 1) {
                        primaryKeys.append(" ").append(pAy[1].trim().toUpperCase());
                    } else {
                        primaryKeys.append(" ASC");
                    }
                }
            }
            //
            StringBuffer sb = new StringBuffer("CREATE TABLE IF NOT EXISTS \"").append(schemaName).append("\".\"").append(tblName).append("\"(");
            Schema.Field f = null;
            boolean isPk = false;
            boolean hasUpdateTimeField = false;
            String qFn = null;
            for(int i=0,size=fs.size();i<size; i++) {
                f = fs.get(i);
                if(f.name().equalsIgnoreCase(FlumeConstants.UPDATE_TIME)) {
                    hasUpdateTimeField = true;
                }
                isPk = pks.contains(f.name());
                sb.append("\"").append(f.name()).append("\" ").append(avro2phoenix(f, isPk));
                if(isPk) {
                    sb.append(" NOT NULL");
                }
                if(i!=size-1) {
                    sb.append(",");
                }
            }
            if(!hasUpdateTimeField) {
                sb.append(",\"").append(FlumeConstants.UPDATE_TIME).append("\" BIGINT");
            }
            if(keyGenerator!=null) {
                String ark = null;
                if(keyGenerator == DefaultKeyGenerator.EVENT_HEAD_KEY) {
                    ark = keyGenerator.generate();
                } else {
                    ark = "_ark" + new Random().nextInt(1000);
                }
                primaryKeys.append(",\"").append(ark).append("\"");
                sb.append(",\"").append(ark).append("\"").append(" VARCHAR(");
                sb.append(keyGenerator.length()).append(") NOT NULL ");
            }
            sb.append(" CONSTRAINT \"").append(tblName).append("_pk\" PRIMARY KEY (")
                    .append(primaryKeys.substring(1)).append(")");
            sb.append(") ").append(tableOptions);
            logger.info("- To create phoenix table sql \n ---\n {} \n---", sb);

            conn.createStatement().execute(sb.toString());

            conn.close();
        }

        private void alterPhoenixTable(String newAvscUrl) throws SQLException, IOException {
            Connection conn = DriverManager.getConnection(jdbcUrl);
            getSchema(avscUrl);

        }

        private void dropPhoenixTable() throws SQLException, IOException {
            Connection conn = DriverManager.getConnection(jdbcUrl);
            StringBuffer sb = new StringBuffer("DROP TABLE IF EXISTS \"").append(schemaName).append("\".\"").append(tblName).append("\"");
            logger.info("- To drop phoenix old-table sql \n ---\n {} \n---", sb);

            conn.createStatement().execute(sb.toString());

            conn.close();
        }

        private Schema getSchema(String avscUrl) throws IOException {
            Configuration conf = new Configuration();
            FileSystem dfs = FileSystem.get(conf);
            Schema.Parser parser = new Schema.Parser();
            if (avscUrl.toLowerCase().startsWith("hdfs://")) {
                FSDataInputStream input = null;
                try {
                    input = dfs.open(new Path(avscUrl));
                    return parser.parse(input);
                } finally {
                    if (input != null) {
                        input.close();
                    }
                }
            }
            return null;
        }

        private String avro2phoenix(Schema.Field f, boolean isPk) {
            String res = null;
            int maxLen = 32;
            if(isPk) {
                try {
                    maxLen = Integer.parseInt(f.getProp("maxLength"));
                } catch(NumberFormatException e) {
                    logger.warn("Primary Column/Type: {} use default maxLength {}",
                            f.name()+"/"+f.schema().getType(), maxLen);
                }
            }
            switch(f.schema().getType()) {
                // NULL, RECORD, ENUM, ARRAY, MAP, UNION, FIXED,
                // STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN
                case NULL:
                case RECORD:
                case ENUM:
                case ARRAY:
                case MAP:
                case UNION:
                case FIXED:
                    logger.warn("AVRO Complex Type isn't supported by Phoenix");
                    break;
                case STRING:
                    if(isPk) {
                        res = "VARCHAR("+ maxLen+")";
                    } else {
                        res = "VARCHAR";
                    }
                    break;
                case BYTES:
                    if(isPk) {
                        res = "BINARY("+ maxLen+")";
                    } else {
                        res = "VARBINARY";
                    }
                    break;
                case INT:
                    res = "INTEGER";
                    break;
                case LONG:
                    res = "BIGINT";
                    break;
                default:
                    res = f.schema().getType().name();
                    break;
            }
            return res;
        }
    }


}
