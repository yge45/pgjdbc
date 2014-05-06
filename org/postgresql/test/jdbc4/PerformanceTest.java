/*-------------------------------------------------------------------------
*
* Copyright (c) 2010-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/
package org.postgresql.test.jdbc4;

import java.sql.*;
import junit.framework.TestCase;
import org.postgresql.test.TestUtil;

public class PerformanceTest extends TestCase
{

    private Connection _conn;

    public PerformanceTest(String name)
    {
        super(name);
    }

    protected void setUp() throws Exception
    {
        _conn = TestUtil.openDB();
        TestUtil.createTable(_conn, "perf", "id integer");
    }

    protected void tearDown() throws SQLException
    {
        TestUtil.dropTable(_conn, "perf");
        TestUtil.closeDB(_conn);
    }

    public void testTonsOfSelect() throws SQLException
    {
        long startInsert = System.currentTimeMillis();
        Statement stmt = _conn.createStatement();
        for (int i = 0; i < 1000; i++)
        {
            stmt.executeUpdate(TestUtil.insertSQL("perf", Integer.toString(i)));
        }
        long endInsert = System.currentTimeMillis();
        System.out.println("INSERT took " + (endInsert - startInsert) + " ms");

        long startSelect = System.currentTimeMillis();
        for (int i = 0; i < 2000; i++)
        {
            ResultSet rs = stmt.executeQuery(TestUtil.selectSQL("perf", "*"));
            while(rs.next())
            {
                rs.getInt(1);
            }
        }
        long endSelect = System.currentTimeMillis();
        System.out.println("SELECT took " + (endSelect - startSelect) + " ms");
    }

}
