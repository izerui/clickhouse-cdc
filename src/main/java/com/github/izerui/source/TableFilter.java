package com.github.izerui.source;

import com.github.izerui.config.CdcConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 表过滤规则：
 * <ul>
 *     <li>指定了表清单（source.tables）时只同步清单中的表，否则表名需匹配 include 正则；</li>
 *     <li>且不匹配 exclude 正则，且不在额外排除集合中（如无主键表）。</li>
 * </ul>
 * 正则只作用于表名本身（不含库名），整串匹配；所有比较忽略大小写（与 Debezium 的 table.include.list 一致）。
 */
public class TableFilter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<String> tables;
    private final String include;
    private final String exclude;
    private final List<String> excludedTables;

    private transient Pattern includePattern;
    private transient Pattern excludePattern;

    public TableFilter(String include, String exclude, Collection<String> excludedTables) {
        this(List.of(), include, exclude, excludedTables);
    }

    /**
     * @param tables 指定的表清单：表名，或 库名.表名；为空时按 include 正则过滤
     */
    public TableFilter(List<String> tables, String include, String exclude, Collection<String> excludedTables) {
        this.tables = tables == null ? List.of() : List.copyOf(tables);
        this.include = include == null || include.isBlank() ? ".*" : include.trim();
        this.exclude = exclude == null ? "" : exclude.trim();
        this.excludedTables = excludedTables == null ? List.of() : List.copyOf(excludedTables);
        // 提前编译一次，配置错误时在启动阶段就报出来
        validateRegex(this.include);
        validateRegex(this.exclude);
        if (this.include.contains(",") || this.exclude.contains(",")) {
            // Debezium 的 table.include.list 以逗号分隔多个正则，表达式内不能出现逗号
            throw new IllegalArgumentException("source.tables.include / exclude 正则中不能包含逗号，请改用 | 组合");
        }
    }

    private static void validateRegex(String regex) {
        if (!regex.isEmpty()) {
            Pattern.compile(regex);
        }
    }

    public static TableFilter fromConfig(CdcConfig config, Collection<String> excludedTables) {
        return new TableFilter(config.getSourceTables(), config.getSourceTablesInclude(), config.getSourceTablesExclude(), excludedTables);
    }

    /** 表是否在指定的表清单中 */
    private boolean listed(String database, String table) {
        for (String t : tables) {
            if (t.equalsIgnoreCase(table) || t.equalsIgnoreCase(database + "." + table)) {
                return true;
            }
        }
        return false;
    }

    /** 指定清单中属于某个库的表名 */
    public List<String> listedTables(String database) {
        List<String> result = new ArrayList<>();
        for (String t : tables) {
            int dot = t.indexOf('.');
            if (dot < 0) {
                result.add(t);
            } else if (t.substring(0, dot).equalsIgnoreCase(database)) {
                result.add(t.substring(dot + 1));
            }
        }
        return result;
    }

    public boolean hasTableList() {
        return !tables.isEmpty();
    }

    /**
     * 判断某个库下的表是否需要同步。
     */
    public boolean accept(String database, String table) {
        if (includePattern == null) {
            includePattern = Pattern.compile(include, Pattern.CASE_INSENSITIVE);
            excludePattern = exclude.isEmpty() ? null : Pattern.compile(exclude, Pattern.CASE_INSENSITIVE);
        }
        String fullName = database + "." + table;
        if (excludedTables.stream().anyMatch(fullName::equalsIgnoreCase)) {
            return false;
        }
        if (hasTableList() ? !listed(database, table) : !includePattern.matcher(table).matches()) {
            return false;
        }
        return excludePattern == null || !excludePattern.matcher(table).matches();
    }

    /**
     * 生成传给 MySqlSource#tableList 的正则（匹配 "库.表"）。
     * <p>
     * 使用正则而不是枚举出的表名列表，这样运行期间新建的表也能捕获到 binlog。
     */
    public String toCdcTableRegex(String database) {
        StringBuilder sb = new StringBuilder();
        sb.append(Pattern.quote(database)).append("\\.");
        List<String> negatives = excludedTables.stream()
                .filter(t -> t.startsWith(database + "."))
                .map(t -> Pattern.quote(t.substring(database.length() + 1)))
                .collect(Collectors.toList());
        if (!exclude.isEmpty()) {
            negatives.add("(?:" + exclude + ")");
        }
        if (!negatives.isEmpty()) {
            sb.append("(?!(?:").append(String.join("|", negatives)).append(")$)");
        }
        if (hasTableList()) {
            List<String> listed = listedTables(database);
            // 该库没有指定任何表时，生成一个不匹配任何表的正则
            sb.append("(?:").append(listed.isEmpty() ? "(?!)" : listed.stream().map(Pattern::quote).collect(Collectors.joining("|"))).append(")");
        } else {
            sb.append("(?:").append(include).append(")");
        }
        return sb.toString();
    }
}
