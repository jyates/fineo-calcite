/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.avatica;

import org.apache.calcite.avatica.remote.MockJsonService;
import org.apache.calcite.avatica.remote.MockProtobufService;
import org.apache.calcite.avatica.remote.MockProtobufService.MockProtobufServiceFactory;
import org.apache.calcite.avatica.remote.Service;


import com.google.common.base.Function;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static java.util.Arrays.asList;

/**
 * RemoteDriver tests that use a Mock implementation of a Connection.
 */
@RunWith(Parameterized.class)
public class RemoteDriverMockTest {
  public static final String MJS = MockJsonService.Factory.class.getName();
  public static final String MPBS = MockProtobufServiceFactory.class.getName();

  @Parameters
  public static List<Object[]> parameters() {
    List<Object[]> parameters = new ArrayList<>();
    parameters.add(new Object[] {MJS});
    parameters.add(new Object[] {MPBS});
    return parameters;
  }

  private final Function<Properties, Connection> connectionFunctor;
  private final String factory;

  public RemoteDriverMockTest(final String factory) {
    this.factory = factory;
    this.connectionFunctor = new Function<Properties, Connection>() {
      @Override public Connection apply(Properties input) {
        try {
          return DriverManager.getConnection("jdbc:avatica:remote:factory=" + factory, input);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private Connection getMockConnection() {
    return getMockConnection(new Properties());
  }

  private Connection getMockConnection(Properties props) {
    try {
      return connectionFunctor.apply(props);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test public void testRegister() throws Exception {
    final Connection connection = getMockConnection();
    assertThat(connection.isClosed(), is(false));
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Test public void testSchemas() throws Exception {
    final Connection connection = getMockConnection();
    final ResultSet resultSet =
        connection.getMetaData().getSchemas(null, null);
    assertFalse(resultSet.next());
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertTrue(metaData.getColumnCount() >= 2);
    assertEquals("TABLE_SCHEM", metaData.getColumnName(1));
    assertEquals("TABLE_CATALOG", metaData.getColumnName(2));
    resultSet.close();
    connection.close();
  }

  @Test public void testTables() throws Exception {
    final Connection connection = getMockConnection();
    final ResultSet resultSet =
        connection.getMetaData().getTables(null, null, null, new String[0]);
    assertFalse(resultSet.next());
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertTrue(metaData.getColumnCount() >= 3);
    assertEquals("TABLE_CAT", metaData.getColumnName(1));
    assertEquals("TABLE_SCHEM", metaData.getColumnName(2));
    assertEquals("TABLE_NAME", metaData.getColumnName(3));
    resultSet.close();
    connection.close();
  }

  @Ignore
  @Test public void testNoFactory() throws Exception {
    final Connection connection =
        DriverManager.getConnection("jdbc:avatica:remote:");
    assertThat(connection.isClosed(), is(false));
    final ResultSet resultSet = connection.getMetaData().getSchemas();
    assertFalse(resultSet.next());
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(2, metaData.getColumnCount());
    assertEquals("TABLE_SCHEM", metaData.getColumnName(1));
    assertEquals("TABLE_CATALOG", metaData.getColumnName(2));
    resultSet.close();
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Ignore
  @Test public void testCatalogsMock() throws Exception {
    final Connection connection = getMockConnection();
    assertThat(connection.isClosed(), is(false));
    final ResultSet resultSet = connection.getMetaData().getSchemas();
    assertFalse(resultSet.next());
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(2, metaData.getColumnCount());
    assertEquals("TABLE_SCHEM", metaData.getColumnName(1));
    assertEquals("TABLE_CATALOG", metaData.getColumnName(2));
    resultSet.close();
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Ignore
  @Test public void testStatementExecuteQueryMock() throws Exception {
    checkStatementExecuteQuery(getMockConnection(), false);
  }

  @Ignore
  @Test public void testPrepareExecuteQueryMock() throws Exception {
    checkStatementExecuteQuery(getMockConnection(), true);
  }

  private List<MockProtobufService.BiFunction<String, Service.Request, Service.Response>>
  setupConnectionProperties(Properties props) {
    List<MockProtobufService.BiFunction<String, Service.Request, Service.Response>> map =
        new ArrayList<>();
    final Map<String, String> openProps = new HashMap<>();
    if (this.factory.equals(MPBS)) {
      String id = UUID.randomUUID().toString();
      props.put(MockProtobufService.TEST_RESPONSE_GROUP_PROPERTY_KEY, id);
      openProps.put(MockProtobufService.TEST_RESPONSE_GROUP_PROPERTY_KEY, id);
      MockProtobufService.PER_ID_MAP.put(id, map);
      // setup the initial open properties for the connection
      map.add(new EqualsWithId(new Service.OpenConnectionResponse()) {
        @Override protected Service.Request getRequest() throws Exception {
          return new Service.OpenConnectionRequest(id, openProps);
        }
      });
    } else if (this.factory.equals(MJS)) {
      return null;
    }
    return map;
  }

  /**
   * Base class for generating a matching request by providing a connection id.
   */
  public abstract class WithId
      implements MockProtobufService.BiFunction<String, Service.Request, Service.Response> {
    private final Service.Response response;
    protected String id;

    protected WithId(Service.Response response) {
      this.response = response;
    }

    @Override public Service.Response apply(String in, Service.Request in2) {
      this.id = in;
      try {
        if (this.withId(in2)) {
          return this.response;
        }
        return null;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    protected abstract Boolean withId(Service.Request in2) throws Exception;
  }

  /**
   * Base class for matching a Response to a Request and provides the connection id.
   */
  public abstract class EqualsWithId extends WithId {
    public EqualsWithId(Service.Response response) {
      super(response);
    }

    @Override protected Boolean withId(Service.Request in2) throws Exception {
      return in2.equals(getRequest());
    }

    protected abstract Service.Request getRequest() throws Exception;
  }

  /**
   * Match a Request to a Response if the incoming Request is of the same class.
   */
  public class IsA
      implements MockProtobufService.BiFunction<String, Service.Request, Service.Response> {
    private final Service.Response response;
    private final Class clazz;

    protected IsA(Class clazz, Service.Response response) {
      this.clazz = clazz;
      this.response = response;
    }


    @Override public Service.Response apply(String in, Service.Request in2) {
      return in2.getClass().equals(this.clazz) ? response : null;
    }
  }

  @Test
  public void testStatementExecuteWithChangingMetadata() throws Exception {
    if (this.factory == MJS) {
      return;
    }
    Properties props = new Properties();
    List<MockProtobufService.BiFunction<String, Service.Request, Service.Response>> list =
        setupConnectionProperties(props);
    final Connection connection = getMockConnection(props);
    final String sql = "select * from (\n"
                       + "  values (1, 'a'), (null, 'b'), (3, 'c')) as t (c1, c2)";

    // setup the request/responses we want per-test
    Service.RpcMetadataResponse metadata = new Service.RpcMetadataResponse();
    Service.ConnectionSyncResponse connResponse =
        new Service.ConnectionSyncResponse(null, metadata);
    list.add(new IsA(Service.ConnectionSyncRequest.class, connResponse));

    Service.CreateStatementResponse createResponse =
        new Service.CreateStatementResponse("", 1, null);
    list.add(new IsA(Service.CreateStatementRequest.class, createResponse));

    List<ColumnMetaData> columns =
        asList(MetaImpl.columnMetaData("C1", 1, Integer.class, true),
            MetaImpl.columnMetaData("C2", 0, String.class, true));
    Meta.CursorFactory cursorFactory = Meta.CursorFactory.create(Meta.Style.LIST, Object.class,
        asList("C1", "C2"));
    Meta.Signature sig = Meta.Signature.create(columns, "sql", null, cursorFactory,
        Meta.StatementType.SELECT);
    List<Object> rows = Collections.<Object>singletonList(asList(1, "2", 3));
    // not the last frame, we need to get another one
    Meta.Frame frame = Meta.Frame.create(0, false, rows, null);
    Service.ResultSetResponse result1 =
        new Service.ResultSetResponse("", 1, true, sig, frame, -1, null);
    List<Service.ResultSetResponse> results = new ArrayList<>();
    results.add(result1);
    Service.ExecuteResponse executeResponse = new Service.ExecuteResponse(results, false, null);
    list.add(new IsA(Service.PrepareAndExecuteRequest.class, executeResponse));

    // now we change the signature on next request
    columns =
        asList(MetaImpl.columnMetaData("C1", 1, Integer.class, true),
            MetaImpl.columnMetaData("C2", 0, String.class, true),
            MetaImpl.columnMetaData("C3", 0, Integer.class, true));
    cursorFactory = Meta.CursorFactory.create(Meta.Style.LIST, Object.class,
        asList("C1", "C2", "C3"));
    sig = Meta.Signature.create(columns, "sql", null, cursorFactory,
        Meta.StatementType.SELECT);
    rows = Collections.<Object>singletonList(asList(2, "2", 3));
    Meta.Frame frame2 = Meta.Frame.create(0, false, rows, sig);
    Service.FetchResponse fetch1 = new Service.FetchResponse(frame2, false, false, null);

    // one more group of rows, this time its the last
    rows = Collections.<Object>singletonList(asList(3, "2", 3));
    Meta.Frame frame3 = Meta.Frame.create(0, true, rows, null);
    Service.FetchResponse fetch2 = new Service.FetchResponse(frame3, false, false, null);

    // then we are going to have a fetch request
    list.add(new IsAListResponses(Service.FetchRequest.class, fetch1, fetch2));

    final Statement statement;
    final ResultSet resultSet;
    statement = connection.createStatement();
    resultSet = statement.executeQuery(sql);
    // initial metadata read
    assertColumns(resultSet, "C1", "C2");

    // get the first row (though we also load the second row/frame at the same time)
    assertTrue(resultSet.next());
    assertColumns(resultSet, "C1", "C2");
    assertEquals(1, resultSet.getInt(1));
    assertEquals("2", resultSet.getString(2));

    // get the second row, which comes "back" with a metadata change, but we shouldn't propagate
    // that until we read the next row
    assertTrue(resultSet.next());
    // so the metadata stays the same
    assertColumns(resultSet, "C1", "C2");
    // and we can read columns as we expect with the new schema
    assertEquals(2, resultSet.getInt(1));
    assertEquals("2", resultSet.getString(2));


    // get the third row. Its "done" and doesn't include a signature change. However, the
    // signature change at the end of the last frame will come into play now
    assertTrue(resultSet.next());
    // we get get a new column to read
    assertColumns(resultSet, "C1", "C2", "C3");
    assertEquals(3, resultSet.getInt(1));
    assertEquals("2", resultSet.getString(2));
    assertEquals(3, resultSet.getInt(3));

    list.add(new IsA(Service.CloseStatementRequest.class, new Service.CloseStatementResponse()));
    list.add(new IsA(Service.CloseConnectionRequest.class, new Service.CloseConnectionResponse()));

    // success!
    resultSet.close();
    statement.close();
    connection.close();
  }

  private static void assertColumns(ResultSet rs, String... columns) throws SQLException {
    ResultSetMetaData metaData = rs.getMetaData();
    assertEquals("Wrong number of columns!", columns.length, metaData.getColumnCount());
    for (int i = 0; i < columns.length; i++) {
      assertEquals(columns[i], metaData.getColumnName(i + 1));
    }
  }

  /**
   * Return each element in the list of Responses if the request class matches
   */
  private class IsAListResponses
      implements MockProtobufService.BiFunction<String, Service.Request, Service.Response> {

    private final Class<?> clazz;
    private final List<Service.Response> responses;

    private IsAListResponses(Class<?> clazz, Service.Response... responses) {
      this(clazz, newArrayList(responses));
    }

    private IsAListResponses(Class<?> clazz, List<Service.Response> responses) {
      this.clazz = clazz;
      this.responses = responses;
    }

    @Override public Service.Response apply(String in, Service.Request in2) {
      Service.Response next = null;
      if (in2.getClass().equals(this.clazz) && this.responses.size() > 0) {
        next = responses.remove(0);
      }
      return next;
    }
  }


  private void checkStatementExecuteQuery(Connection connection,
      boolean prepare) throws SQLException {
    final String sql = "select * from (\n"
        + "  values (1, 'a'), (null, 'b'), (3, 'c')) as t (c1, c2)";
    final Statement statement;
    final ResultSet resultSet;
    final ParameterMetaData parameterMetaData;
    if (prepare) {
      final PreparedStatement ps = connection.prepareStatement(sql);
      statement = ps;
      parameterMetaData = ps.getParameterMetaData();
      resultSet = ps.executeQuery();
    } else {
      statement = connection.createStatement();
      parameterMetaData = null;
      resultSet = statement.executeQuery(sql);
    }
    if (parameterMetaData != null) {
      assertThat(parameterMetaData.getParameterCount(), equalTo(0));
    }
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(2, metaData.getColumnCount());
    assertEquals("C1", metaData.getColumnName(1));
    assertEquals("C2", metaData.getColumnName(2));
    assertTrue(resultSet.next());
    assertTrue(resultSet.next());
    assertTrue(resultSet.next());
    assertFalse(resultSet.next());
    resultSet.close();
    statement.close();
    connection.close();
  }

  @Test public void testResultSetsFinagled() throws Exception {
    // These values specified in MockJsonService
    final String table = "my_table";
    final long value = 10;

    final Connection connection = getMockConnection();
    // Not an accurate ResultSet per JDBC, but close enough for testing.
    ResultSet results = connection.getMetaData().getColumns(null, null, table, null);
    assertTrue(results.next());
    assertEquals(table, results.getString(1));
    assertEquals(value, results.getLong(2));
  }

}

// End RemoteDriverMockTest.java
