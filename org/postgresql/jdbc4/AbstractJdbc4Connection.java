/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/
package org.postgresql.jdbc4;

import java.sql.*;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.postgresql.core.Oid;
import org.postgresql.core.StreamObserver;
import org.postgresql.core.Utils;
import org.postgresql.core.TypeInfo;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLState;
import org.postgresql.util.PSQLException;
import org.postgresql.jdbc2.AbstractJdbc2Array;

abstract class AbstractJdbc4Connection extends org.postgresql.jdbc3g.AbstractJdbc3gConnection
{
    private static final SQLPermission SQL_PERMISSION_ABORT = new SQLPermission("callAbort");
    private static final SQLPermission SQL_PERMISSION_NETWORK_TIMEOUT = new SQLPermission("setNetworkTimeout");

    private final Properties _clientInfo;

    private int _networkTimeout = 0;
    private Executor _networkTimeoutExecutor;
    private NetworkTimeoutCommand _networkTimeoutCommand;
    private NetworkTimeoutObserver _networkTimeoutObserver;

    public AbstractJdbc4Connection(HostSpec[] hostSpecs, String user, String database, Properties info, String url) throws SQLException {
        super(hostSpecs, user, database, info, url);

        TypeInfo types = getTypeInfo();
        if (haveMinimumServerVersion("8.3")) {
            types.addCoreType("xml", Oid.XML, java.sql.Types.SQLXML, "java.sql.SQLXML", Oid.XML_ARRAY);
        }

        _clientInfo = new Properties();
        if (haveMinimumServerVersion("9.0")) {
            String appName = info.getProperty("ApplicationName");
            if (appName == null) {
                appName = "";
            }
            _clientInfo.put("ApplicationName", appName);
        }
    }

    public Clob createClob() throws SQLException
    {
        checkClosed();
        throw org.postgresql.Driver.notImplemented(this.getClass(), "createClob()");
    }

    public Blob createBlob() throws SQLException
    {
        checkClosed();
        throw org.postgresql.Driver.notImplemented(this.getClass(), "createBlob()");
    }

    public NClob createNClob() throws SQLException
    {
        checkClosed();
        throw org.postgresql.Driver.notImplemented(this.getClass(), "createNClob()");
    }

    public SQLXML createSQLXML() throws SQLException
    {
        checkClosed();
        return new Jdbc4SQLXML(this);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException
    {
        checkClosed();
        throw org.postgresql.Driver.notImplemented(this.getClass(), "createStruct(String, Object[])");
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException
    {
        checkClosed();
        int oid = getTypeInfo().getPGArrayType(typeName);
        if (oid == Oid.UNSPECIFIED)
            throw new PSQLException(GT.tr("Unable to find server array type for provided name {0}.", typeName), PSQLState.INVALID_NAME);

        char delim = getTypeInfo().getArrayDelimiter(oid);
        StringBuffer sb = new StringBuffer();
        appendArray(sb, elements, delim);

        // This will not work once we have a JDBC 5,
        // but it'll do for now.
        return new Jdbc4Array(this, oid, sb.toString());
    }

    private static void appendArray(StringBuffer sb, Object elements, char delim)
    {
        sb.append('{');

        int nElements = java.lang.reflect.Array.getLength(elements);
        for (int i=0; i<nElements; i++) {
            if (i > 0) {
                sb.append(delim);
            }

            Object o = java.lang.reflect.Array.get(elements, i);
            if (o == null) {
                sb.append("NULL");
            } else if (o.getClass().isArray()) {
                appendArray(sb, o, delim);
            } else {
                String s = o.toString();
                AbstractJdbc2Array.escapeArrayElement(sb, s);
            }
        }
        sb.append('}');
    }

    public boolean isValid(int timeout) throws SQLException
    {
        if (isClosed()) {
            return false;
        }
        if (timeout < 0) {
            throw new PSQLException(GT.tr("Invalid timeout ({0}<0).", timeout), PSQLState.INVALID_PARAMETER_VALUE);
        }
        boolean valid = false;
        Statement stmt = null;
        ResultSet rs;
        try {
            if (!isClosed()) {
                stmt = createStatement();
                stmt.setQueryTimeout( timeout );
                rs = stmt.executeQuery( "SELECT 1" );
                rs.close();
                valid = true;
            }
        }
        catch ( SQLException e) {
            getLogger().log(GT.tr("Validating connection."),e);
        }
        finally
        {
            if(stmt!=null) try {stmt.close();}catch(Exception ex){}
        }
        return valid;    
}

    public void setClientInfo(String name, String value) throws SQLClientInfoException
    {
        if (haveMinimumServerVersion("9.0") && "ApplicationName".equals(name)) {
            if (value == null)
                value = "";

            try {
                StringBuffer sql = new StringBuffer("SET application_name = '");
                Utils.appendEscapedLiteral(sql, value, getStandardConformingStrings());
                sql.append("'");
                execSQLUpdate(sql.toString());
            } catch (SQLException sqle) {
                Map<String, ClientInfoStatus> failures = new HashMap<String, ClientInfoStatus>();
                failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
                throw new SQLClientInfoException(GT.tr("Failed to set ClientInfo property: {0}", "ApplicationName"), sqle.getSQLState(), failures, sqle);
            }

            _clientInfo.put(name, value);
            return;
        }

        Map<String, ClientInfoStatus> failures = new HashMap<String, ClientInfoStatus>();
        failures.put(name, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
        throw new SQLClientInfoException(GT.tr("ClientInfo property not supported."), PSQLState.NOT_IMPLEMENTED.getState(), failures);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException
    {
        if (properties == null || properties.size() == 0)
            return;

        Map<String, ClientInfoStatus> failures = new HashMap<String, ClientInfoStatus>();

        Iterator<String> i = properties.stringPropertyNames().iterator();
        while (i.hasNext()) {
            String name = i.next();
            if (haveMinimumServerVersion("9.0") && "ApplicationName".equals(name)) {
                String value = properties.getProperty(name);
                setClientInfo(name, value);
            } else {
                failures.put(i.next(), ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            }
        }

        if (!failures.isEmpty())
            throw new SQLClientInfoException(GT.tr("ClientInfo property not supported."), PSQLState.NOT_IMPLEMENTED.getState(), failures);
    }

    public String getClientInfo(String name) throws SQLException
    {
        checkClosed();
        return _clientInfo.getProperty(name);
    }

    public Properties getClientInfo() throws SQLException
    {
        checkClosed();
        return _clientInfo;
    }

    public <T> T createQueryObject(Class<T> ifc) throws SQLException
    {
        checkClosed();
        throw org.postgresql.Driver.notImplemented(this.getClass(), "createQueryObject(Class<T>)");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        checkClosed();
        return iface.isAssignableFrom(getClass());
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        checkClosed();
        if (iface.isAssignableFrom(getClass()))
        {
            return (T) this;
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
        throw org.postgresql.Driver.notImplemented(this.getClass(), "getParentLogger()");
    }

    public void setSchema(String schema) throws SQLException
    {
        checkClosed();
        Statement stmt = createStatement();
        try
        {
            stmt.executeUpdate("SET SESSION SCHEMA '" + schema + "'");
        }
        finally
        {
            stmt.close();
        }
    }

    public String getSchema() throws SQLException
    {
        checkClosed();
        String searchPath;
        Statement stmt = createStatement();
        try
        {
            ResultSet rs = stmt.executeQuery( "SHOW search_path");
            try
            {
                if (!rs.next())
                {
                    return null;
                }
                searchPath = rs.getString(1);
            }
            finally
            {
                rs.close();
            }
        }
        finally
        {
            stmt.close();
        }

        // keep only the first schema of the search path if there are many
        int commaIndex = searchPath.indexOf(',');
        if (commaIndex == -1)
        {
            return searchPath;
        }
        else
        {
            return searchPath.substring(0, commaIndex);
        }
    }

    public void abort(Executor executor) throws SQLException
    {
        if (isClosed())
        {
            return;
        }

        SQL_PERMISSION_ABORT.checkGuard(this);

        AbortCommand command = new AbortCommand();
        if (executor != null)
        {
            executor.execute(command);
        }
        else
        {
            command.run();
        }
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
    {
        checkClosed();

        SQL_PERMISSION_NETWORK_TIMEOUT.checkGuard(this);

        if (milliseconds < 0)
        {
            throw new PSQLException(GT.tr("Invalid timeout ({0}<0).", milliseconds), PSQLState.INVALID_PARAMETER_VALUE);
        }

        _networkTimeout = milliseconds;
        if (_networkTimeout == 0)
        {
            if (_networkTimeoutObserver != null)
            {
                removeNetworkObserver(_networkTimeoutObserver);
                _networkTimeoutObserver = null;
            }
            _networkTimeoutExecutor = null;
        }
        else
        {
            if (executor == null)
            {
                throw new PSQLException(GT.tr("Executor must be non-null when timeout is positive"), PSQLState.INVALID_PARAMETER_VALUE);
            }

            _networkTimeoutExecutor = executor;
            if (_networkTimeoutObserver == null)
            {
                _networkTimeoutObserver = new NetworkTimeoutObserver();
                addNetworkObserver(_networkTimeoutObserver);
            }
        }
    }

    public int getNetworkTimeout() throws SQLException
    {
        checkClosed();
        return _networkTimeout;
    }

    public class AbortCommand implements Runnable
    {
        public void run()
        {
            abort();
        }
    }

    public class NetworkTimeoutCommand extends AbortCommand
    {
        private boolean _cancelled = false;
        private long _startTime;

        public NetworkTimeoutCommand()
        {
            _startTime = System.currentTimeMillis();
        }
        
        public synchronized void run()
        {
            // we don't know when the executor will schedule the command
            // it may be already outdated
            if (!_cancelled)
            {
                try
                {
                    // take into account time already spent in the executor queue
                    long elapsed = System.currentTimeMillis() - _startTime;
                    long remaining = _networkTimeout - elapsed;
                    if (remaining >= 0)
                    {
                        wait(_networkTimeout);
                    }
                    if (!_cancelled && _networkTimeoutCommand == this)
                    {
                        super.run();
                    }
                }
                catch (InterruptedException e)
                {
                    return;
                }
            }
        }

        public synchronized void cancelTask()
        {
            _cancelled = true;
            notify();
        }
    }

    public class NetworkTimeoutObserver implements StreamObserver
    {
        public void startOperation(Object ioObject)
        {
            _networkTimeoutCommand = new NetworkTimeoutCommand();
            if (_networkTimeoutExecutor != null)
            {
                try
                {
                    _networkTimeoutExecutor.execute(_networkTimeoutCommand);
                }
                catch (RejectedExecutionException e)
                {
                    // the executor is probably shutting down
                    // we do not want to get this exception at each network call
                    _networkTimeoutExecutor = null;
                }
            }
        }

        public void endOperation(Object ioObject)
        {
            if (_networkTimeoutCommand != null)
            {
                _networkTimeoutCommand.cancelTask();
            }
            _networkTimeoutCommand = null;
        }
    }

}
