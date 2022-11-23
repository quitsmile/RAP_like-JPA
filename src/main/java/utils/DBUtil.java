package utils;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class DBUtil {
    private static final Properties p = new Properties();
    static {
        InputStream asStream;
        try {
            asStream = new FileInputStream("D:\\IDeal\\Spring_Test\\src\\main\\resources\\db.properties");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            p.load(asStream);
            Class.forName(p.getProperty("jdbc.driver"));
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    public static Connection getConnection(){
        Connection c;
        try {
            c = DriverManager.getConnection(p.getProperty("jdbc.url"), p.getProperty("jdbc.user"),p.getProperty("jdbc.password"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return c;
    }

    public static void closeAll(Connection con, PreparedStatement past, ResultSet rs) {
        try {
            if(rs!=null) {

                rs.close();
                rs=null;
            }
            if(past!=null) {
                past.close();
                past=null;
            }
            if(con!=null) {
                con.close();
                con=null;
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }


}
