//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.flywaydb.community.database.kingbase;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.parser.*;
import org.flywaydb.core.internal.sqlscript.Delimiter;
import org.flywaydb.core.internal.sqlscript.ParsedSqlStatement;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class KingbaseESParser extends Parser {
    private static final Delimiter PLSQL_DELIMITER = new Delimiter("/", true);
    private static final Pattern PLSQL_TYPE_BODY_REGEX = Pattern.compile("^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\sTYPE\\sBODY\\s([^\\s]*\\s)?(IS|AS)");
    private static final Pattern PLSQL_PACKAGE_BODY_REGEX = Pattern.compile("^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\sPACKAGE\\sBODY\\s([^\\s]*\\s)?(IS|AS)");
    private static final StatementType PLSQL_PACKAGE_BODY_STATEMENT = new StatementType();
    private static final Pattern PLSQL_PACKAGE_DEFINITION_REGEX = Pattern.compile("^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\sPACKAGE\\s([^\\s]*\\s)?(IS|AS)");
    private static final Pattern PLSQL_VIEW_REGEX = Pattern.compile("^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\sVIEW\\s([^\\s]*\\s)?AS\\sWITH\\s(PROCEDURE|FUNCTION)");
    private static final StatementType PLSQL_VIEW_STATEMENT = new StatementType();
    private static final Pattern PLSQL_REGEX = Pattern.compile("^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\s(FUNCTION|PROCEDURE|TYPE|TRIGGER)");
    private static final Pattern DECLARE_BEGIN_REGEX = Pattern.compile("^DECLARE|BEGIN|WITH");
    private static final StatementType PLSQL_STATEMENT = new StatementType();
    private static final Pattern JAVA_REGEX = Pattern.compile("^CREATE(\\sOR\\sREPLACE)?(\\sAND\\s(RESOLVE|COMPILE))?(\\sNOFORCE)?\\sJAVA\\s(SOURCE|RESOURCE|CLASS)");
    private static final StatementType PLSQL_JAVA_STATEMENT = new StatementType();
    private static final Pattern COPY_FROM_STDIN_REGEX = Pattern.compile("^COPY( .*)? FROM STDIN");
    private static final Pattern CREATE_DATABASE_TABLESPACE_SUBSCRIPTION_REGEX = Pattern.compile("^(CREATE|DROP) (DATABASE|TABLESPACE|SUBSCRIPTION)");
    private static final Pattern ALTER_SYSTEM_REGEX = Pattern.compile("^ALTER SYSTEM");
    private static final Pattern CREATE_INDEX_CONCURRENTLY_REGEX = Pattern.compile("^(CREATE|DROP)( UNIQUE)? INDEX CONCURRENTLY");
    private static final Pattern REINDEX_REGEX = Pattern.compile("^REINDEX( VERBOSE)? (SCHEMA|DATABASE|SYSTEM)");
    private static final Pattern VACUUM_REGEX = Pattern.compile("^VACUUM");
    private static final Pattern DISCARD_ALL_REGEX = Pattern.compile("^DISCARD ALL");
    private static final Pattern ALTER_TYPE_ADD_VALUE_REGEX = Pattern.compile("^ALTER TYPE( .*)? ADD VALUE");
    private static final StatementType COPY = new StatementType();
    private static final List<String> CONTROL_FLOW_KEYWORDS = Arrays.asList("IF", "LOOP", "CASE");


    public KingbaseESParser(Configuration configuration, ParsingContext parsingContext) {
        super(configuration, parsingContext, 3);
    }

    @Override
    protected char getAlternativeStringLiteralQuote() {
        return '$';
    }

    @Override
    protected ParsedSqlStatement createStatement(PeekingReader reader, Recorder recorder, int statementPos, int statementLine, int statementCol, int nonCommentPartPos, int nonCommentPartLine, int nonCommentPartCol, StatementType statementType, boolean canExecuteInTransaction, Delimiter delimiter, String sql) throws IOException {
        return (ParsedSqlStatement)(statementType == COPY ? new KingbaseESCopyParsedStatement(nonCommentPartPos, nonCommentPartLine, nonCommentPartCol, sql.substring(nonCommentPartPos - statementPos), this.readCopyData(reader, recorder)) : super.createStatement(reader, recorder, statementPos, statementLine, statementCol, nonCommentPartPos, nonCommentPartLine, nonCommentPartCol, statementType, canExecuteInTransaction, delimiter, sql));
    }

    private String readCopyData(PeekingReader reader, Recorder recorder) throws IOException {
        reader.readUntilIncluding('\n');
        recorder.start();
        boolean done = false;

        do {
            String line = reader.readUntilIncluding('\n');
            if ("\\.".equals(line.trim())) {
                done = true;
            } else {
                recorder.confirm();
            }
        } while(!done);

        return recorder.stop();
    }

    @Override
    protected StatementType detectStatementType(String simplifiedStatement, ParserContext context, PeekingReader reader) {
        if (COPY_FROM_STDIN_REGEX.matcher(simplifiedStatement).matches()) {
            return COPY;
        } else if (PLSQL_PACKAGE_BODY_REGEX.matcher(simplifiedStatement).matches()) {
            return PLSQL_PACKAGE_BODY_STATEMENT;
        } else if (!PLSQL_REGEX.matcher(simplifiedStatement).matches() && !PLSQL_PACKAGE_DEFINITION_REGEX.matcher(simplifiedStatement).matches() && !DECLARE_BEGIN_REGEX.matcher(simplifiedStatement).matches()) {
            if (JAVA_REGEX.matcher(simplifiedStatement).matches()) {
                return PLSQL_JAVA_STATEMENT;
            } else {
                return PLSQL_VIEW_REGEX.matcher(simplifiedStatement).matches() ? PLSQL_VIEW_STATEMENT : super.detectStatementType(simplifiedStatement, context, reader);
            }
        } else {
            return PLSQL_STATEMENT;
        }
    }

    @Override
    protected Boolean detectCanExecuteInTransaction(String simplifiedStatement, List<Token> keywords) {
        return !CREATE_DATABASE_TABLESPACE_SUBSCRIPTION_REGEX.matcher(simplifiedStatement).matches() && !ALTER_SYSTEM_REGEX.matcher(simplifiedStatement).matches() && !CREATE_INDEX_CONCURRENTLY_REGEX.matcher(simplifiedStatement).matches() && !REINDEX_REGEX.matcher(simplifiedStatement).matches() && !VACUUM_REGEX.matcher(simplifiedStatement).matches() && !DISCARD_ALL_REGEX.matcher(simplifiedStatement).matches() && !ALTER_TYPE_ADD_VALUE_REGEX.matcher(simplifiedStatement).matches() ? null : false;
    }

    @Override
    protected Token handleAlternativeStringLiteral(PeekingReader reader, ParserContext context, int pos, int line, int col) throws IOException {
        String dollarQuote = (char)reader.read() + reader.readUntilIncluding('$');
        reader.swallowUntilExcluding(dollarQuote);
        reader.swallow(dollarQuote.length());
        return new Token(TokenType.STRING, pos, line, col, (String)null, (String)null, context.getParensDepth());
    }

    @Override
    protected void adjustBlockDepth(ParserContext context, List<Token> tokens, Token keyword, PeekingReader reader) throws IOException {
        String keywordText = keyword.getText();
        if (context.getStatementType() == PLSQL_JAVA_STATEMENT) {
            if ("{".equals(keywordText)) {
                context.increaseBlockDepth("");
            } else if ("}".equals(keywordText)) {
                context.decreaseBlockDepth();
            }

        } else {
            int parensDepth = keyword.getParensDepth();
            if (!"BEGIN".equals(keywordText) && (!CONTROL_FLOW_KEYWORDS.contains(keywordText) || lastTokenIs(tokens, parensDepth, "END")) && (!"TRIGGER".equals(keywordText) || !lastTokenIs(tokens, parensDepth, "COMPOUND")) && !doTokensMatchPattern(tokens, keyword, PLSQL_PACKAGE_BODY_REGEX) && !doTokensMatchPattern(tokens, keyword, PLSQL_PACKAGE_DEFINITION_REGEX) && !doTokensMatchPattern(tokens, keyword, PLSQL_TYPE_BODY_REGEX)) {
                if ("END".equals(keywordText)) {
                    context.decreaseBlockDepth();
                }
            } else {
                context.increaseBlockDepth("");
            }

            TokenType tokenType = keyword.getType();
            if (context.getStatementType() == PLSQL_PACKAGE_BODY_STATEMENT && (TokenType.EOF == tokenType || TokenType.DELIMITER == tokenType) && context.getBlockDepth() == 1) {
                context.decreaseBlockDepth();
            }
        }
    }

    @Override
    protected void adjustDelimiter(ParserContext context, StatementType statementType) {
        if (statementType != PLSQL_STATEMENT && statementType != PLSQL_VIEW_STATEMENT && statementType != PLSQL_JAVA_STATEMENT && statementType != PLSQL_PACKAGE_BODY_STATEMENT) {
            context.setDelimiter(Delimiter.SEMICOLON);
        } else {
            context.setDelimiter(PLSQL_DELIMITER);
        }

    }
}
