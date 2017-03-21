package org.neo4j.shell.state;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.driver.internal.logging.ConsoleLogging;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Transaction;

import java.util.logging.Level;

/**
 * This demonstrates a scenario where two people are accessing the database at once.
 *
 * Session 1 simulates a user who is not necessarily using the drivers to access th`e database - the user might be
 * using REST/Neo4j-shell/embedded api.
 *
 * Session 2 simulates a user who IS accessing with the driver (via cypher-shell). Once deadlocked even session.reset()
 * doesn't work, it is necessary to kill entire cypher-shell process.
 */
public class DeadlockTest {
    private Driver driver;

    @Before
    public void setup() throws Exception {
        Config config = Config.build()
                .withLogging(new ConsoleLogging(Level.ALL))
                .withEncryptionLevel(Config.EncryptionLevel.REQUIRED).toConfig();
        driver = GraphDatabase.driver("bolt://localhost:7687",
                AuthTokens.basic("neo4j", "neo"),
                config);
    }

    @Test
    public void deadlockTest() throws Exception {
        Session session1 = driver.session();
        Session session2 = driver.session();

        System.err.println("Make sure two suitable nodes exist");
        session1.run(new Statement("merge (n1:Lock {id:1})"));
        session1.run(new Statement("merge (n1:Lock {id:2})"));

        System.err.println("Start transactions on both sessions");
        Transaction tx1 = session1.beginTransaction();
        Transaction tx2 = session2.beginTransaction();

        System.err.println("TX1 locks node 1");
        tx1.run("match (n:Lock {id:1}) set n.age=100");

        System.err.println("TX2 locks node 2");
        tx2.run("match (n:Lock {id:2}) set n.age=200");

        System.err.println("TX2 locks node 1");
        tx2.run("match (n:Lock {id:1}) set n.age=200");

        // Commit should not deadlock forever
        System.err.println("Committing TX2");
        tx2.success();
        tx2.close();

        System.err.println("Committing TX1");
        tx1.success();
        tx1.close();
    }

    @Test
    public void deadlockTestWithLambda() throws Exception {
        Session session1 = driver.session();
        Session session2 = driver.session();

        System.err.println("Make sure two suitable nodes exist");
        session1.run(new Statement("merge (n1:Lock {id:1})"));
        session1.run(new Statement("merge (n1:Lock {id:2})"));

        System.err.println("Start transactions on session 1");
        Transaction tx1 = session1.beginTransaction();

        System.err.println("TX1 locks node 1");
        tx1.run("match (n:Lock {id:1}) set n.age=100");

        // Should not deadlock forever
        System.err.println("TX2 executes in lambda");
        session2.writeTransaction(tx -> {
            tx.run(new Statement("match (n:Lock {id:2}) set n.age=200")).consume();
            tx.run(new Statement("match (n:Lock {id:1}) set n.age=200")).consume();
            return "DONE";
        });

        System.err.println("Committing TX1");
        tx1.success();
        tx1.close();
    }
}
