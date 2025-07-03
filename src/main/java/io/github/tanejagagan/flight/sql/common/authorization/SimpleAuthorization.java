package io.github.tanejagagan.flight.sql.common.authorization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ExpressionFactory;
import io.dazzleduck.sql.commons.Transformations;
import io.github.tanejagagan.flight.sql.common.UnauthorizedException;
import io.github.tanejagagan.flight.sql.server.Main;
import io.github.tanejagagan.flight.sql.server.SqlAuthorizer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.*;

public class SimpleAuthorization implements SqlAuthorizer {
    private record AccessKey(String user, Transformations.CatalogSchemaTable catalogSchemaTable) { }
    private record AccessValue(JsonNode filter, List<String> columns) { }
    Map<AccessKey, AccessValue> accessMap = new HashMap<>();
    Set<String> superUsers = new HashSet<>();

    public static final String ACCESS_FILE = "simple-access.json";

    public SimpleAuthorization(Map<String, List<String>> userGroupMapping,
                               List<AccessRow> accessRows) {
        var groupAccessRowMap = new HashMap<String, List<AccessRow>>();
        for (var row : accessRows) {
            groupAccessRowMap.compute(row.group(), (key, oldValue) -> {
                if (oldValue == null) {
                    var l = new ArrayList<AccessRow>();
                    l.add(row);
                    return l;
                } else {
                    oldValue.add(row);
                    return oldValue;
                }
            });
        }

        userGroupMapping.forEach((user, groups) -> {
            var map = new HashMap<Transformations.CatalogSchemaTable, AccessValue>();
            groups.forEach(group -> {
                var _accessRows = groupAccessRowMap.get(group);
                if(_accessRows != null) {
                    _accessRows.forEach(accessRow -> {
                        var key = new Transformations.CatalogSchemaTable(accessRow.database(), accessRow.schema(), accessRow.tableOrPath(), accessRow.type());
                        map.compute(key, (k, oldValue) -> {
                            if (oldValue == null) {
                                return collapse(accessRow);
                            } else {
                                return collapse(accessRow, oldValue);
                            }
                        });

                    });
                }
            });
            map.forEach((key, value) -> accessMap.put(new AccessKey(user, key), value));
        });
    }

    public static SqlAuthorizer load() throws IOException {
        var userGroupMapping = loadUsrGroupMapping();
        var accessRows = loadAccessRows();
        return new SimpleAuthorization(userGroupMapping, accessRows);
    }

    public static List<AccessRow> loadAccessRows() throws IOException {
        var result = new ArrayList<AccessRow>();
        try (InputStream inputStream = SimpleAuthorization.class.getResourceAsStream("/" + ACCESS_FILE);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            ObjectMapper mapper = new ObjectMapper();
            var line = reader.readLine();
            var accessRow = mapper.readValue(line, AccessRow.class);
            result.add(accessRow);
        }
        return result;
    }

    public static Map<String, List<String>> loadUsrGroupMapping() throws IOException {
        var conf = ConfigFactory.load().getConfig(Main.CONFIG_PATH);
        var res = new HashMap<String, List<String>>();
        var users = conf.getObjectList("users");
        for (var userConfigObject : users) {
            var userConfig = userConfigObject.toConfig();
            var user = userConfig.getString("username");
            var groups = userConfig.getStringList("groups");
            res.put(user, groups);
        }
        return res;
    }

    private AccessValue collapse(AccessRow r1, AccessValue accessValue) {
        return new AccessValue(collapseFilters(r1, accessValue.filter), collapseColumns(r1, accessValue.columns));
    }

    private AccessValue collapse(AccessRow r) {
        return new AccessValue(fromFilterString(r.filter()), r.columns());
    }

    private JsonNode fromFilterString(String stringFilter) {
        var sql = "select * from t where " + stringFilter;
        JsonNode tree;
        try {
            tree = Transformations.parseToTree(sql);
        } catch (SQLException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return Transformations.getWhereClause(tree);
    }

    private List<String> collapseColumns(AccessRow r1, List<String> columns) {
        return List.of();
    }

    private JsonNode collapseFilters(AccessRow r1, JsonNode node) {
        var qnode = fromFilterString(r1.filter());
        return ExpressionFactory.orFilters(qnode, node);
    }

    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query) throws UnauthorizedException {
        if (superUsers.contains(user)) {
            return query;
        }
        validateForAuthorization(query);
        var catalogSchemaTable = Transformations.getTableOrPath(query, database, schema);
        if (catalogSchemaTable == null) {
            throw new UnauthorizedException("No tableOrPath/Path found");
        }
        var a = accessMap.get(new AccessKey(user, catalogSchemaTable));
        if (a == null) {
            throw new UnauthorizedException(database + "." + schema);
        }
        var columnAccess = hasAccessToColumns(query, a.columns());
        if (!columnAccess) {
            throw new UnauthorizedException("No access to columns specified columns");
        }
        return addFilerToQuery(query, a.filter());
    }

    public static void validateForAuthorization(JsonNode jsonNode) throws UnauthorizedException {

        var supportedFromType = Set.of("TABLE_FUNCTION", "BASE_TABLE");
        var supportedTableFunction = Set.of("generate_series", "read_parquet", "read_delta");
        var statements = (ArrayNode) jsonNode.get("statements");
        if (statements.size() != 1) {
            throw new UnauthorizedException("too many statements");
        }
        var statement = statements.get(0);
        var statementNode = statement.get("node");
        //
        var statementNodeType = statementNode.get("type").asText();
        if (!statementNodeType.equals("SELECT_NODE")) {
            throw new UnauthorizedException("Not authorized. Incorrect Type :" + statementNodeType);
        }
        var where = statementNode.get("where_clause");
        var subQueries = Transformations.collectSubQueries(where);
        if (!subQueries.isEmpty()) {
            throw new UnauthorizedException("Sub queries are not supported");
        }
        var selectList = statementNode.get("select_list");
        var groupExpression = statementNode.get("group_expressions");
        var fromTable = statementNode.get("from_table");
        var fromTableType = fromTable.get("type").asText();
        if (!supportedFromType.contains(fromTableType)) {
            throw new UnauthorizedException("Type " + fromTableType + "Not supported");
        }

        var cteMap = statementNode.get("cte_map");
        if (!cteMap.isEmpty()) {
            var cteMapMap = cteMap.get("map");
            if (!cteMapMap.isEmpty()) {
                throw new UnauthorizedException("CTE expression is not supported");
            }
        }

        if (fromTable.get("type").asText().equals("BASE_TABLE")) {
            return;
        }
        var tableFunction = fromTable.get("function");
        var tableFunctionName = tableFunction.get("function_name").asText();
        if (!supportedTableFunction.contains(tableFunctionName.toLowerCase())) {
            throw new UnauthorizedException("Function " + fromTableType + "Not supported");
        }
    }

    private boolean hasAccessToColumns(JsonNode query, List<String> accessColumn) {
        return true;
    }

    private static List<AccessRow> readAccessRows() throws IOException {
        String resourceName = "simple-access.json";
        ObjectMapper mapper = new ObjectMapper();
        List<AccessRow> result = new ArrayList<>();
        try (InputStream is = SimpleAuthorization.class.getClassLoader().getResourceAsStream(resourceName)) {
            assert is != null;
            try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while ((line = br.readLine()) != null) {
                    AccessRow myObject = mapper.readValue(line, AccessRow.class);
                    result.add(myObject);
                }
            }
        }
        return result;
    }

    public static JsonNode addFilerToQuery(JsonNode query, JsonNode toAdd) {
        var qWhereClause = Transformations.getWhereClause(query);
        JsonNode allWhere ;
        if(qWhereClause == null || qWhereClause instanceof NullNode ) {
            allWhere = toAdd;
        } else {
            allWhere = ExpressionFactory.andFilters(qWhereClause, toAdd);
        }
        return replaceWhereClause(query, allWhere);
    }

    private static JsonNode replaceWhereClause(JsonNode query, JsonNode newWhereClause) {
        var statementNode = (ObjectNode) Transformations.getFirstStatementNode(query);
        statementNode.set("where_clause", newWhereClause);
        return query;
    }
}
