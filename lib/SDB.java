package demo;
import org.bson.BSONObject;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;

public class SDB {
    public static void main(String[] args) throws Exception {
    	String cmd = args[0];
    	String ip = args[1];
    	String port = args[2];
    	String user = args[3];
    	String passwd = args[4];
    	String collection_space = args[5];
    	String collection = args[6];
    	
        String connString = ip+":"+port;
        Sequoiadb sdb = new Sequoiadb(connString, user, passwd);
        DBCollection cl = sdb.getCollectionSpace(collection_space).getCollection(collection);
        
        if (cmd.toLowerCase().equals("get_all_data")) {
        	DBCursor cursor = cl.query();
            try {
                while (cursor.hasNext()) {
                   BSONObject record = cursor.getNext();
                   System.out.println((String) record.toString());
                } 
            } finally {
             cursor.close();
            }	
        }
        else if (cmd.toLowerCase().equals("delete_table")) {
        	sdb.getCollectionSpace("dp_auto").dropCollection(collection);
        }
        else {
        	throw new Exception("Not Supported CMD");
        }
        sdb.disconnect();
	}
}
