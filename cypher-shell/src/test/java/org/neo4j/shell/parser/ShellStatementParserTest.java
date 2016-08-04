package org.neo4j.shell.parser;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.shell.exception.CypherSyntaxError;
import org.neo4j.shell.exception.IncompleteCypherError;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.neo4j.shell.parser.ShellStatementParser.splitOnLine;

public class ShellStatementParserTest {
    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    private ShellStatementParser parser;

    @Before
    public void setup() {
        parser = new ShellStatementParser();
    }

    @Test
    public void canParseShellCommand() throws Exception {
        String stmt = ":help exit";
        List<String> statements = parser.parse(stmt);

        assertEquals(1, statements.size());
        assertEquals(stmt, statements.get(0));
    }

    @Test
    public void incompleteCypherTest() throws Exception {
        thrown.expect(IncompleteCypherError.class);

        String stmt = "CREATE\n";

        parser.parse(stmt);
    }

    @Test
    public void canParseCypherCommand() throws Exception {
        String stmt = "CREATE (n) RETURN n";
        List<String> statements = parser.parse(stmt);

        assertEquals(1, statements.size());
        assertEquals(stmt, statements.get(0));
    }

    @Test
    public void canParseCypherCommands() throws Exception {
        String stmt1 = "CREATE (n) RETURN n\n";
        String stmt2 = "CREATE (n) RETURN n";

        List<String> statements = parser.parse(String.join("", stmt1, stmt2));

        assertEquals(2, statements.size());
        assertEquals(stmt1, statements.get(0));
        assertEquals(stmt2, statements.get(1));
    }

    @Test
    public void canParseReturnsStacked() throws Exception {
        String stmt = "RETURN 1 RETURN 2 RETURN 3";

        List<String> statements = parser.parse(stmt);

        assertEquals(3, statements.size());
        assertEquals("RETURN 1 ", statements.get(0));
        assertEquals("RETURN 2 ", statements.get(1));
        assertEquals("RETURN 3", statements.get(2));
    }

    @Test
    public void canParseMixedShellAndCypherCommands() throws Exception {
        String stmt0 = ":begin\n";
        String stmt1 = "CREATE (n) RETURN n\n";
        String stmt2 = "CREATE (n) RETURN n\n";
        String stmt3 = ":commit\n";
        String stmt4 = ":exit";

        List<String> statements = parser.parse(String.join("", stmt0, stmt1, stmt2, stmt3, stmt4));

        assertEquals(5, statements.size());
        assertEquals(stmt0, statements.get(0) + "\n");
        assertEquals(stmt1, statements.get(1));
        assertEquals(stmt2, statements.get(2) + "\n");
        assertEquals(stmt3, statements.get(3) + "\n");
        assertEquals(stmt4, statements.get(4));
    }

    @Test
    public void canParseMixedShellAndCypherCommandsWithSomeWhiteSpace() throws Exception {
        String stmt0 = "   :begin  \n";
        String stmt1 = "CREATE (n) RETURN n\n";
        String stmt2 = "CREATE (n) RETURN n\n";
        String stmt3 = "  :commit  \n";
        String stmt4 = "  :exit  ";

        List<String> statements = parser.parse(String.join("", stmt0, stmt1, stmt2, stmt3, stmt4));

        assertEquals(5, statements.size());
        assertEquals(stmt0, statements.get(0) + "\n");
        assertEquals(stmt1, statements.get(1));
        assertEquals(stmt2, statements.get(2) + "\n");
        assertEquals(stmt3, statements.get(3) + "\n");
        assertEquals(stmt4, statements.get(4));
    }

    @Test
    public void throwsErrorOnMisformedMixedScript() throws Exception {
        thrown.expect(CypherSyntaxError.class);

        String stmt0 = "   :begin  \n";
        String stmt1 = "CREATE (n) RETURN n\n";
        String stmt2 = "CREATE (n) RETNRU n\n";
        String stmt3 = "  :commit  \n";
        String stmt4 = "  :exit  ";

        parser.parse(String.join("", stmt0, stmt1, stmt2, stmt3, stmt4));
    }

    @Test
    public void splitLineTest() throws Exception {
        String txt = "one\ntwo\nthree";
        ShellStatementParser.LineSplitResult res = splitOnLine(txt, 0);

        assertNull(res.getBefore());
        assertEquals("one", res.getLine());
        assertEquals("two\nthree", res.getAfter());

        txt = "one";
        res = splitOnLine(txt, 0);

        assertNull(res.getBefore());
        assertEquals("one", res.getLine());
        assertNull(res.getAfter());
        assertEquals(txt, res.getOriginalText());

        txt = "one\ntwo";
        res = splitOnLine(txt, 0);

        assertNull(res.getBefore());
        assertEquals("one", res.getLine());
        assertEquals("two", res.getAfter());
        assertEquals(txt, res.getOriginalText());

        txt = "one\ntwo";
        res = splitOnLine(txt, 1);

        assertEquals("one", res.getBefore());
        assertEquals("two", res.getLine());
        assertNull(res.getAfter());
        assertEquals(txt, res.getOriginalText());

        txt = "one\ntwo\nthree\nfour";
        res = splitOnLine(txt, 0);

        assertNull(res.getBefore());
        assertEquals("one", res.getLine());
        assertEquals("two\nthree\nfour", res.getAfter());
        assertEquals(txt, res.getOriginalText());

        txt = "one\ntwo\nthree\nfour";
        res = splitOnLine(txt, 2);

        assertEquals("one\ntwo", res.getBefore());
        assertEquals("three", res.getLine());
        assertEquals("four", res.getAfter());
        assertEquals(txt, res.getOriginalText());

        txt = "one\ntwo\nthree\nfour";
        res = splitOnLine(txt, 3);

        assertEquals("one\ntwo\nthree", res.getBefore());
        assertEquals("four", res.getLine());
        assertNull(res.getAfter());
        assertEquals(txt, res.getOriginalText());
    }

    @Test
    public void splitLineTestError1() throws Exception {
        thrown.expect(IllegalArgumentException.class);

        splitOnLine("one", 1);
    }

    @Test
    public void splitLineTestError2() throws Exception {
        thrown.expect(IllegalArgumentException.class);

        splitOnLine("one\ntwo", 2);
    }

    @Test
    public void splitLineTestError3() throws Exception {
        thrown.expect(IllegalArgumentException.class);

        splitOnLine("one\ntwo", -1);
    }
}