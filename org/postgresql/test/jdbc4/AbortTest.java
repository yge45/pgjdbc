/*-------------------------------------------------------------------------
*
* Copyright (c) 2010-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/
package org.postgresql.test.jdbc4;

import java.sql.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.postgresql.test.TestUtil;

public class AbortTest extends TestCase
{

    private static final int SLEEP_SECONDS = 30;
    private static final int SLEEP_MILLISECONDS = SLEEP_SECONDS * 1000;

    private Connection _conn0;
    private Connection _conn1;
    private Connection _conn2;

    public AbortTest(String name)
    {
        super(name);
    }

    protected void setUp() throws Exception
    {
        _conn0 = TestUtil.openDB();
        _conn1 = TestUtil.openDB();
        _conn2 = TestUtil.openDB();
    }

    protected void tearDown() throws SQLException
    {
        for (Connection c : new Connection[] { _conn0, _conn1, _conn2 })
        {
            try
            {
                TestUtil.closeDB(c);
            }
            catch (SQLException e)
            {
            }
        }
    }

    public void testAbort() throws SQLException, InterruptedException, ExecutionException
    {
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        long startTime = System.currentTimeMillis();
        Future<SQLException> workerFuture = executor.submit(new Callable<SQLException>()
        {
            public SQLException call()
            {
                try
                {
                    Statement stmt = _conn0.createStatement();
                    stmt.execute("SELECT pg_sleep(" + SLEEP_SECONDS + ")");
                }
                catch(SQLException e)
                {
                    return e;
                }
                return null;
            }
        });
        Future<SQLException> abortFuture = executor.submit(new Callable<SQLException>()
        {
            public SQLException call()
            {
                ExecutorService abortExecutor = Executors.newSingleThreadExecutor();
                try
                {
                    _conn0.abort(abortExecutor);
                }
                catch (SQLException e)
                {
                    return e;
                }
                abortExecutor.shutdown();
                try
                {
                    abortExecutor.awaitTermination(SLEEP_SECONDS, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                }
                return null;
            }
        });
        SQLException workerException = workerFuture.get();
        long endTime = System.currentTimeMillis();
        SQLException abortException = abortFuture.get();
        if (abortException != null)
        {
            throw abortException;
        }
        if (workerException == null)
        {
            fail("Statement execution should have been aborted, thus throwing an exception");
        }
        // suppose that if it took at least 95% of sleep time, aborting has failed and we've waited the full time
        assertTrue(endTime - startTime < SLEEP_MILLISECONDS * 95 / 100);
        assertTrue(_conn0.isClosed());
    }

    /**
     * According to the javadoc, calling abort on a closed connection is a no-op.
     */
    public void testAbortOnClosedConnection() throws SQLException
    {
        _conn0.close();
        try
        {
            _conn0.abort(Executors.newSingleThreadExecutor());
        }
        catch (SQLException e)
        {
            fail(e.getMessage());
        }
    }

    /**
     * Test that what we set is what we get
     */
    public void testGetSetNetworkTimeout() throws SQLException
    {
        _conn0.setNetworkTimeout(new InlineExecutor(), 3000);
        assertEquals(3000, _conn0.getNetworkTimeout());

        _conn0.setNetworkTimeout(new InlineExecutor(), 0);
        assertEquals(0, _conn0.getNetworkTimeout());
    }

    /**
     * Test that setting the network timeout will effectively break any network
     * transmission longer than the given value, without breaking others.
     */
    public void testNetworkTimeout() throws SQLException, ExecutionException, InterruptedException
    {
        // test that a single threaded executor is sufficient to monitor timeouts of multiple connections.
        ExecutorService timeoutExecutor = Executors.newSingleThreadExecutor();
        _conn0.setNetworkTimeout(timeoutExecutor, SLEEP_MILLISECONDS / 10);
        _conn1.setNetworkTimeout(timeoutExecutor, SLEEP_MILLISECONDS / 10);
        _conn2.setNetworkTimeout(timeoutExecutor, SLEEP_MILLISECONDS / 10);

        ExecutorService workerExecutor = Executors.newFixedThreadPool(3);
        long startTime = System.currentTimeMillis();

        // this worker should never be interrupted by network timeout because it does its calls are not long enough
        Future<SQLException> shouldNOTAbortWorker = workerExecutor.submit(new Callable<SQLException>()
        {
            public SQLException call()
            {
                for (int i = 0; i < 11; i++)
                {
                    try
                    {
                        Statement stmt = _conn0.createStatement();
                        stmt.execute("SELECT pg_sleep(" + (SLEEP_SECONDS / 11) + ")");
                    }
                    catch(SQLException e)
                    {
                        return e;
                    }
                }
                return null;
            }
        });

        // both following workers should be interrupted by network timeout
        Future<SQLException> shouldAbortWorker1 = workerExecutor.submit(new Callable<SQLException>()
        {
            public SQLException call()
            {
                try
                {
                    Statement stmt = _conn1.createStatement();
                    stmt.execute("SELECT pg_sleep(" + SLEEP_SECONDS + ")");
                }
                catch(SQLException e)
                {
                    return e;
                }
                return null;
            }
        });

        Future<SQLException> shouldAbortWorker2 = workerExecutor.submit(new Callable<SQLException>()
        {
            public SQLException call()
            {
                try
                {
                    Statement stmt = _conn2.createStatement();
                    stmt.execute("SELECT pg_sleep(" + SLEEP_SECONDS + ")");
                }
                catch(SQLException e)
                {
                    return e;
                }
                return null;
            }
        });

        SQLException worker1Exception = shouldAbortWorker1.get();
        SQLException worker2Exception = shouldAbortWorker2.get();
        long endTime = System.currentTimeMillis();

        // suppose that if it took at least 95% of sleep time, aborting has failed and we've waited the full time
        assertTrue(endTime - startTime < SLEEP_MILLISECONDS * 95 / 100);
        assertTrue(worker1Exception != null);
        assertTrue(worker2Exception != null);
        assertTrue(_conn1.isClosed());
        assertTrue(_conn2.isClosed());

        SQLException worker0Exception = shouldNOTAbortWorker.get();
        assertTrue(worker0Exception == null);
        assertFalse(_conn0.isClosed());
    }

    /**
     * The most simple executor running commands in the calling thread.
     * 
     * It guarantees that the command is finished when execute returns, what we need
     * to make some assertions.
     */
    public static class InlineExecutor implements Executor
    {
        public void execute(Runnable command)
        {
            command.run();
        }
    }
}
