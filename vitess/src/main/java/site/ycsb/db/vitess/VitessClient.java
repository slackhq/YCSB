package site.ycsb.db.vitess;

import com.google.common.primitives.UnsignedLong;
import io.vitess.client.Context;
import io.vitess.client.RpcClient;
import io.vitess.client.VTGateBlockingConnection;
import io.vitess.client.VTSession;
import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.Row;
import io.vitess.client.grpc.GrpcClientFactory;

import io.vitess.proto.Query;
import io.vitess.proto.Topodata;

import io.vitess.proto.Vtrpc;

import site.ycsb.*;

import java.util.*;

/**
 * Vitess client for YCSB framework.
 */
public class VitessClient extends DB {
  private Context ctx;
  private VTGateBlockingConnection vtgate;
  private RpcClient client;
  private VTSession session;
  private QueryCreator queryCreator;
  private String vtgateAddress;
  private String keyspace;
  private Topodata.TabletType readTabletType;
  private Topodata.TabletType writeTabletType;
  private String privateKeyField;
  private boolean serverAutoCommitEnabled;
  private boolean debugMode;
  private boolean createVindex;
  private boolean keyIsString;

  private static final Vtrpc.CallerID CALLER_ID =
      Vtrpc.CallerID.newBuilder()
          .setPrincipal("ycsb_principal")
          .setComponent("ycsb_component")
          .setSubcomponent("ycsb_subcomponent")
          .build();

  private static final String DEFAULT_CREATE_TABLE =
      "CREATE TABLE usertable(YCSB_KEY BIGINT UNSIGNED PRIMARY KEY, "
          + "field0 TEXT, field1 TEXT, field2 TEXT, field3 TEXT, field4 TEXT, "
          + "field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT"
          + ") Engine=InnoDB";
  private static final String DEFAULT_DROP_TABLE = "drop table if exists usertable";

  @Override
  public void init() throws DBException {
    vtgateAddress = getProperties().getProperty("hosts", "");
    String[] vtgateAddressSplit = vtgateAddress.split(":");
    keyspace = getProperties().getProperty("keyspace", "ycsb");
    String shardingColumnName = getProperties().getProperty(
        "vitess_sharding_column_name", "keyspace_id");
    keyIsString = Boolean.parseBoolean(getProperties().getProperty(
        "vitess_key_is_string", "true"));
    writeTabletType = Topodata.TabletType.MASTER;
    readTabletType = Topodata.TabletType.REPLICA;
    privateKeyField = getProperties().getProperty("vitess_primary_key_field", "YCSB_KEY");
    debugMode = getProperties().getProperty("debug") != null;
    serverAutoCommitEnabled = Boolean.parseBoolean(
        getProperties().getProperty("server_autocommit_enabled", "false"));

    ctx = Context.getDefault().withCallerId(CALLER_ID);

    queryCreator = new QueryCreator(shardingColumnName, keyIsString);

    // Configure Vitess Session
    session = new VTSession(keyspace, Query.ExecuteOptions.getDefaultInstance());
    session.setAutoCommit(serverAutoCommitEnabled);

    // Custom create and drop table statements
    String createTable = getProperties().getProperty("createTable", DEFAULT_CREATE_TABLE);
    String dropTable = getProperties().getProperty("dropTable", DEFAULT_DROP_TABLE);

    try {
      client = new GrpcClientFactory().create(ctx, vtgateAddress);
      vtgate = new VTGateBlockingConnection(client);
      vtgate.execute(ctx, "use " + keyspace, null, session);

      if (Boolean.parseBoolean(getProperties().getProperty("doCreateTable", "false"))) {
        String[] shards = getProperties().getProperty("shards", "0").split(",");

        if (!"skip".equalsIgnoreCase(createTable)) {
          try {
            vtgate.execute(ctx, "begin", null, session);
            if (debugMode) {
              System.out.println(dropTable);
            }
            vtgate.execute(ctx, dropTable, null, session);
            if (debugMode) {
              System.out.println(createTable);
            }
            vtgate.execute(ctx, createTable, null, session);
            vtgate.execute(ctx, "commit", null, session);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public Status delete(String table, String key) {
    QueryCreator.Query query =
        queryCreator.createDeleteQuery(keyspace, writeTabletType, table, privateKeyField, key);

    return applyMutation(query);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> result) {
    QueryCreator.Query query = queryCreator.createInsertQuery(keyspace,
        writeTabletType,
        table,
        privateKeyField,
        key,
        result);

    return applyMutation(query);
  }

  /**
   * @param query
   * @return
   */
  private Status applyMutation(QueryCreator.Query query) {
    try {
      if (serverAutoCommitEnabled) {
        vtgate.execute(ctx, query.getQuery(), query.getBindVars(), session);
      } else {
        vtgate.execute(ctx, "begin", null, session);
        vtgate.execute(ctx, query.getQuery(), query.getBindVars(), session);
        vtgate.execute(ctx, "commit", null, session);
      }
    } catch (Exception e) {
      e.printStackTrace();
      return new Status("ERROR", e.getMessage());
    }
    return Status.OK;
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    QueryCreator.Query query = queryCreator.createSelectQuery(keyspace,
        readTabletType,
        table,
        privateKeyField,
        key,
        fields);
    try {
      Cursor cursor = vtgate.execute(
          ctx, query.getQuery(), query.getBindVars(), session);

      List<Query.Field> cursorFields = cursor.getFields();
      Row row = cursor.next();
      if (row == null) {
        System.out.println("No rows returned for query: " + query.getBindVars().getOrDefault("YCSB_KEY", "null"));
        return new Status("ERROR", "No rows returned");
      }

      for (Query.Field field : cursorFields) {
        if (!keyIsString && field.getName().equals(privateKeyField)) {
          UnsignedLong value = row.getULong(field.getName());
          result.put(field.getName(), new NumericByteIterator(value.longValue()));
        } else {
          byte[] value = row.getBytes(field.getName());
          if (value == null) {
            value = new byte[] {};
          }
          result.put(field.getName(), new ByteArrayByteIterator(value));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
      return new Status("ERROR", e.getMessage());
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String key, int num, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    QueryCreator.Query query = queryCreator.createSelectScanQuery(keyspace,
        readTabletType,
        table,
        privateKeyField,
        key,
        fields,
        num);
    try {
      Cursor cursor = vtgate.execute(
          ctx, query.getQuery(), query.getBindVars(), session);
      Row row = cursor.next();
      while (row != null) {
        HashMap<String, ByteIterator> rowResult = new HashMap<>();
        List<Query.Field> cursorFields = cursor.getFields();
        for (int i = 0; i < cursorFields.size(); i++) {
          byte[] value = row.getBytes(i);
          if (value == null) {
            value = new byte[] {};
          }
          rowResult.put(cursorFields.get(i).getName(), new ByteArrayByteIterator(value));
        }
        result.add(rowResult);
        row = cursor.next();
      }
    } catch (Exception e) {
      e.printStackTrace();
      return new Status("ERROR", e.getMessage());
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> result) {
    QueryCreator.Query query = queryCreator.createUpdateQuery(keyspace,
        writeTabletType,
        table,
        privateKeyField,
        key,
        result);

    return applyMutation(query);
  }
}