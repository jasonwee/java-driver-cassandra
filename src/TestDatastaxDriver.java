import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.SecretKeySpec;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import net.tinyelements.util.TxtBean;

public class TestDatastaxDriver {
    
    public final static String PASSWORD = "passwrod";
    
    SecretKeySpec key;
    Cipher cipher;
    Cluster cluster;
    Session session;
    
    public TestDatastaxDriver() {
        try {
            cluster = Cluster.builder().addContactPoints("node1.google.com").build();
            session = cluster.connect("cql3test");
            
            key = new SecretKeySpec(PASSWORD.getBytes("UTF-8"), "Blowfish");
            cipher = Cipher.getInstance("Blowfish");
            cipher.init(Cipher.ENCRYPT_MODE, key);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public String encrypt(String input) {
        try {
            return TxtBean.hex(cipher.doFinal(input.getBytes("UTF-8")));
        } catch (IllegalBlockSizeException | BadPaddingException
                | UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }

    public void insert() {
        

        try (BufferedReader br = Files.newBufferedReader(Paths.get("1m_email.txt"))) {

            // read line by line
            String email;
            while ((email = br.readLine()) != null) {
                String encEmail = encrypt(String.format("%-254s", email).replaceFirst(" ", "\\$").replace(' ', '0'));
                Set<String> useId = new HashSet<String>();
                useId.add("");
                Map<Date, String> campaign = new HashMap<>();
                
                PreparedStatement ps = session.prepare("insert into email (unique_id, use_id, utm_campaign) VALUES (?, ?, ?)");
                session.execute(ps.bind(encEmail, useId, ""));
            }

        } catch (IOException e) {
            System.err.format("IOException: %s%n", e);
        }
    }
    
    public void read() {
        
        for (Row row : session.execute("select * from email")) {
            // do something ...
            System.out.println(row.getString(0));
            System.out.println(row.getSet("use_id", String.class));
            System.out.println(row.getMap("utm_campaign", Date.class, String.class));
            
            
        }
        session.shutdown();
        cluster.shutdown();
    }

    public static void main(String[] args) {
        TestDatastaxDriver tester = new TestDatastaxDriver();
        tester.insert();
    }
}
