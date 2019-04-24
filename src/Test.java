import java.nio.ByteBuffer;
import java.util.List;

import org.codehaus.jackson.map.ser.std.MapSerializer;

import com.oracle.jrockit.jfr.DataType;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;

// https://hector-client.github.io/hector/build/html/content/cql_basics.html?highlight=cql
public class Test {
    
    protected static final StringSerializer ss = new StringSerializer();
    
    protected static final ByteBufferSerializer bfs = new ByteBufferSerializer();
    
    public static void main(String[] args) {
        
        System.out.println("hi");
        
        CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator();
        cassandraHostConfigurator.setHosts("gc01.opentracker.net");
        
        
        Cluster cluster = HFactory.getOrCreateCluster("MyCluster", cassandraHostConfigurator);
        Keyspace keyspace = HFactory.createKeyspace("cql3test", cluster);
        
        CqlQuery<String,String,ByteBuffer> cqlQuery = new CqlQuery<String,String,ByteBuffer>(keyspace, ss, ss, bfs);
        cqlQuery.setQuery("select * from email");
        QueryResult<CqlRows<String,String,ByteBuffer>> result = cqlQuery.execute();
        
        List<Row<String, String, ByteBuffer>> list = result.get().getList();
        for (Row<String, String, ByteBuffer> row : list) {
            System.out.println(row.getKey().toString());
            ColumnSlice<String, ByteBuffer> columns = row.getColumnSlice();
            List<HColumn<String, ByteBuffer>> cc = columns.getColumns();
            for (HColumn<String, ByteBuffer> c : cc) {
                System.out.println(c.getName());
                System.out.println(c.getValueBytes().toString());
            }
            
        }

        
        
    }

}
