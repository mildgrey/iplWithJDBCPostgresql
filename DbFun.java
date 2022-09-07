import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class DbFun {
    public static  void main(String[] args) throws  Exception{
        Connection con=connectToDb("IPLdb","postgres","Root@2022");

        totalMatchPerYear(con);
        totalMatchPerTeam(con);
        extraRunConcededPerTeam(con);
        bowlerWithEconomy(con);
        
        con.close();
    }
    public static Connection connectToDb(String dbname,String user,String pass){
        Connection con=null;
        try {
            Class.forName("org.postgresql.Driver");
            con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/"+dbname,user,pass);
            System.out.println("Connection successful");
        }
        catch (Exception e){
            System.out.println(e);
        }
        return con;
    }
    public static void totalMatchPerYear(Connection con) throws Exception{
        Statement st = con.createStatement();
        String query="select season,count(season) AS totalMatch from matches group by season";
        ResultSet rs = st.executeQuery(query);

        System.out.println("IPL Total Match played Each Year Detail:\n");
        while(rs.next()) {
            String season = "for the season "+rs.getString("season")+" total Match Played "+ rs.getInt("totalMatch");
            System.out.println(season);
        }
        st.close();
    }
    public  static  void  totalMatchPerTeam(Connection con) throws  Exception{
        Statement st = con.createStatement();
        String query="select winner,count(winner) AS totalWins from matches group by winner";
        ResultSet rs = st.executeQuery(query);

        System.out.println("IPL Total Match played Each Year Detail:\n");
        while(rs.next()) {
            String winsPerTeam = "Team "+rs.getString("winner")+"  Wins total  "+ rs.getInt("totalWins")+" matches ";
            System.out.println(winsPerTeam);
        }
        st.close();
    }
    public  static  void extraRunConcededPerTeam(Connection con) throws Exception{
        Statement st = con.createStatement();
        String query="\n" +
                "SELECT bowling_team,SUM(extra_runs) FROM deliveries\n" +
                "WHERE match_id IN (SELECT id FROM matches\n" +
                "WHERE season=2016)\n" +
                "GROUP BY bowling_team;\n";
        ResultSet rs = st.executeQuery(query);

        System.out.println("IPL Total Match played Each Year Detail:\n");
        while(rs.next()) {
            String extraRunsPerTeam = "Team "+rs.getString(1)+"  Conceded  "+ rs.getInt(2)+" runs ";
            System.out.println(extraRunsPerTeam);
        }
        st.close();
    }
    public  static  void bowlerWithEconomy(Connection con) throws  Exception{
        Statement st = con.createStatement();
        String query="SELECT  bowler,6*(1.0*SUM(total_runs)/COUNT(bowler)) AS economy\n" +
                "FROM deliveries\n" +
                "WHERE match_id IN (SELECT id FROM matches \n" +
                "\t\t\t\t   WHERE season=2015)\n" +
                "GROUP BY bowler\n" +
                "ORDER BY economy asc;";
        ResultSet rs = st.executeQuery(query);

        System.out.println("IPL Total Match played Each Year Detail:\n");
        while(rs.next()) {
            String bowlerWithEconomy = "Bowler "+rs.getString("bowler")+"  with Economy "+ rs.getFloat("economy");
            System.out.println(bowlerWithEconomy);
        }
        st.close();
    }

}
