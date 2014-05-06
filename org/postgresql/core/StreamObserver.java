package org.postgresql.core;

public interface StreamObserver
{

    void startOperation(Object ioObject);

    void endOperation(Object ioObject);

}
