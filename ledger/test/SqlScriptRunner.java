package ledger.test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public final class SqlScriptRunner {

    public void run(Connection cx, String sql) throws Exception {
        for (String statementSql : splitStatements(sql)) {
            if (statementSql.isBlank()) {
                continue;
            }
            try (Statement st = cx.createStatement()) {
                st.execute(statementSql);
            }
        }
    }

    List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;
        String activeDollarTag = null;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : '\0';

            if (inLineComment) {
                current.append(c);
                if (c == '\n') {
                    inLineComment = false;
                }
                continue;
            }

            if (inBlockComment) {
                current.append(c);
                if (c == '*' && next == '/') {
                    current.append(next);
                    i++;
                    inBlockComment = false;
                }
                continue;
            }

            if (activeDollarTag != null) {
                if (startsWith(sql, i, activeDollarTag)) {
                    current.append(activeDollarTag);
                    i += activeDollarTag.length() - 1;
                    activeDollarTag = null;
                } else {
                    current.append(c);
                }
                continue;
            }

            if (inSingleQuote) {
                current.append(c);
                if (c == '\'' && next == '\'') {
                    current.append(next);
                    i++;
                } else if (c == '\'') {
                    inSingleQuote = false;
                }
                continue;
            }

            if (inDoubleQuote) {
                current.append(c);
                if (c == '"') {
                    inDoubleQuote = false;
                }
                continue;
            }

            if (c == '-' && next == '-') {
                current.append(c).append(next);
                i++;
                inLineComment = true;
                continue;
            }

            if (c == '/' && next == '*') {
                current.append(c).append(next);
                i++;
                inBlockComment = true;
                continue;
            }

            if (c == '\'') {
                current.append(c);
                inSingleQuote = true;
                continue;
            }

            if (c == '"') {
                current.append(c);
                inDoubleQuote = true;
                continue;
            }

            String dollarTag = readDollarTag(sql, i);
            if (dollarTag != null) {
                current.append(dollarTag);
                i += dollarTag.length() - 1;
                activeDollarTag = dollarTag;
                continue;
            }

            if (c == ';') {
                statements.add(current.toString().trim());
                current.setLength(0);
                continue;
            }

            current.append(c);
        }

        if (!current.toString().isBlank()) {
            statements.add(current.toString().trim());
        }

        return statements;
    }

    private static boolean startsWith(String value, int index, String prefix) {
        return value.regionMatches(index, prefix, 0, prefix.length());
    }

    private static String readDollarTag(String sql, int start) {
        if (sql.charAt(start) != '$') {
            return null;
        }

        int i = start + 1;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c == '$') {
                return sql.substring(start, i + 1);
            }
            if (!(Character.isLetterOrDigit(c) || c == '_')) {
                return null;
            }
            i++;
        }
        return null;
    }
}
