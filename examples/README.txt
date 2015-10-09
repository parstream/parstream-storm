Example for the Apache Storm adaptor for ParStream
====================================================

This presents an example on how to compile and use the Apache Storm adaptor for ParStream.


----------------------------------------------------
1) Prepare ParStream database environment
----------------------------------------------------

  NOTE:
  - Ensure you use the correct path to "parstream-server"

  - Export java.library.path by executing:
    $ export LD_LIBRARY_PATH=$PARSTREAM_HOME/lib


----------------------------------------------------
2) Prepare Apache Storm environment
----------------------------------------------------

  1. Download and unpack the Apache Storm binary into $HOME/apache-storm


----------------------------------------------------
2) Build and execute example using maven
----------------------------------------------------

The maven project has a dependency on the "ps-streaming-import-<PARSTREAM_VERSION>.jar" found in $PARSTREAM_HOME/lib.

  1. Install ps-streaming-import JAR file into maven repository:
     $ cd $PARSTREAM_HOME/lib
     $ mvn install:install-file -Dfile=ps-streaming-import-<PARSTREAM_VERSION>.jar -DgroupId=com.parstream.driver -DartifactId=ps-streaming-import -Dversion=<PARSTREAM_VERSION> -Dpackaging=jar

  2. Clone and compile the Apache Storm adaptor source code:
     $ cd $HOME
     $ git clone https://github.com/parstream/storm.git
     $ cd storm
     $ mvn -Dparstream.version=<PARSTREAM_VERSION> install

  3. Start a local ParStream database instance:
     $ cd $HOME/storm/examples
     $ $PARSTREAM_HOME/bin/parstream-server first &

  4. Run the Apache Storm adaptor example:
     $ cd $HOME/storm/examples/example
     $ $HOME/apache-storm/bin/storm jar target/storm-example-local-<ADAPTOR_VERSION>.jar com.parstream.example.storm.LocalTopology
