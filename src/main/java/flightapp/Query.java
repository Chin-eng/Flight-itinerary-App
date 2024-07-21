package flightapp;

import java.io.Console;
import java.io.IOException;
import java.security.spec.KeySpec;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;



/**
 * Runs queries against a back-end database
 */
public class Query extends QueryAbstract {
  //
  // Canned queries
  //
  
  private static final String createStmt1 = "INSERT INTO Users_chinehuu VALUES (?, ?, ?)";
  private static final String createStmt2 = "SELECT hashedPassword FROM Users_chinehuu WHERE username = ?";
  private PreparedStatement flightCapacityStmt;
  private static final String FLIGHT_CAPACITY_SQL = "SELECT capacity FROM Flights WHERE fid = ?";
  private PreparedStatement ps1;
  private PreparedStatement ps2;
  private PreparedStatement checkFlightDayStatement;
  private static final String CHECK_FLIGHT_DAY =  "SELECT day_of_month FROM Flights WHERE fid = ?";
  private PreparedStatement GetCapacityTakenStatement;

  private static final String GET_CAPACITY_TAKEN = "SELECT count(*) AS total FROM BOOKING_chinehuu WHERE fid = ?";
  private PreparedStatement CheckForDayInReservationTable;
  private static final String CHECK_DAY_IN_BOOKING_RESERVATION = "SELECT res_day FROM Reservations_chinehuu WHERE res_day = ? AND username = ?";
  private PreparedStatement insertDataIntoBookingStatement;
  private static final String INSERT_INTO_BOOKING_TABLE = "INSERT INTO BOOKING_chinehuu (fid, capacity, capacity_taken, bday, username) VALUES(? , ?,0,?,?)";
  private PreparedStatement checkFlightPriceStatement;
  private static final String CHECK_FLIGHT_PRICE =  "SELECT price FROM Flights WHERE fid = ?";

  private PreparedStatement CurrentRidStatement;
  private static final String CURRENT_RID = "SELECT res_id FROM Reservations_chinehuu ORDER BY res_id desc";
  private PreparedStatement InsertReservationStatement;
  private static final String INSERT_RESERVATION = "INSERT INTO Reservations_chinehuu (res_id, username, price, paid , res_day, fid1, fid2) VALUES( ? , ?, ?, ?, ?,?,?)";
  private PreparedStatement UpdateCapacityTakenStatement;
  private static final String UPDATE_CAPACITY_TAKEN = "UPDATE BOOKING_chinehuu SET capacity_taken = ? WHERE fid = ?;";
  private PreparedStatement CheckInfoForReservation;
  private static final String CHECK_FLIGHT_INFO_FOR_RESERVATION = "SELECT res_id, paid, fid1, fid2 FROM reservations_chinehuu WHERE username = ?";
  private PreparedStatement FlightInfoStatement;
  private static final String FLIGHT_INFO = "SELECT fid, day_of_month, carrier_id, flight_num, origin_city, dest_city, actual_time, capacity, price FROM FLIGHTS WHERE fid = ?";

  private PreparedStatement countDirectStatement;
  private static final String DIRECT_FLIGHT_COUNT = "SELECT COUNT(*) AS DirectFlightsCount FROM FLIGHTS WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? AND canceled = 0";
  
  private PreparedStatement directFlightStmt;
  private static final String DIRECT_FLIGHT = "SELECT TOP (?) * FROM FLIGHTS WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? AND canceled = 0 ORDER BY price ASC";

  private PreparedStatement inDirectFlightStmt;
  private static final String INDIRECT_FLIGHT = 
"SELECT TOP (?) F1.fid AS FirstFlightID, F2.fid AS SecondFlightID " +
  "FROM FLIGHTS AS F1 " +
  "JOIN FLIGHTS AS F2 ON F1.dest_city = F2.origin_city " +
  "WHERE F1.origin_city = ? " + 
  "AND F2.dest_city = ? " +     
  "AND F1.day_of_month = ? " + 
  "AND F2.day_of_month = ? " + 
  "AND F1.canceled = 0 " +
  "AND F2.canceled = 0 " +
  "AND F1.fid != F2.fid";

  

  private int fid1_array[] = new int[100];
  private int fid2_array[] = new int[100];
  private int result_time_array[] = new int[100];
  private String result_array[] = new String[100];
  private String currentusername_array[] = new String[100];
  private int numOfsearch=0;
  private int itnum = 0;
  private String currentUsername;
  private int rid = 1;
  private StringBuffer sbReservation = new StringBuffer();
  ArrayList<Itinerary> itineraries = new ArrayList<>();
  int itineraryCounter;
  //
  // Instance variables
  //


  protected Query() throws SQLException, IOException {
    prepareStatements();
  }

  /**
   * Clear the data in any custom tables created.
   * 
   * WARNING! Do not drop any tables and do not clear the flights table.
   */
  public void clearTables() {
    try {
      String sql1 = "Delete from Users_chinehuu";
      String sql2 = "Delete from Reservations_chinehuu";
      PreparedStatement pstmt1 = conn.prepareStatement(sql1);
      PreparedStatement pstmt2 = conn.prepareStatement(sql2);
      pstmt1.executeUpdate();
      pstmt2.executeUpdate();
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * prepare all the SQL statements in this method.
   */
  private void prepareStatements() throws SQLException {
    flightCapacityStmt = conn.prepareStatement(FLIGHT_CAPACITY_SQL);
    ps1 = conn.prepareStatement(createStmt1);
    ps2 = conn.prepareStatement(createStmt2);
    GetCapacityTakenStatement = conn.prepareStatement(GET_CAPACITY_TAKEN);
    checkFlightDayStatement = conn.prepareStatement(CHECK_FLIGHT_DAY);
    CheckForDayInReservationTable = conn.prepareStatement(CHECK_DAY_IN_BOOKING_RESERVATION);
    insertDataIntoBookingStatement = conn.prepareStatement(INSERT_INTO_BOOKING_TABLE);
    checkFlightPriceStatement = conn.prepareStatement(CHECK_FLIGHT_PRICE);
    CurrentRidStatement = conn.prepareStatement(CURRENT_RID);
    InsertReservationStatement = conn.prepareStatement(INSERT_RESERVATION);
    UpdateCapacityTakenStatement = conn.prepareStatement(UPDATE_CAPACITY_TAKEN);
    CheckInfoForReservation = conn.prepareStatement(CHECK_FLIGHT_INFO_FOR_RESERVATION);
    FlightInfoStatement = conn.prepareStatement(FLIGHT_INFO);
    
    
    countDirectStatement = conn.prepareStatement(DIRECT_FLIGHT_COUNT);
    directFlightStmt = conn.prepareStatement(DIRECT_FLIGHT);
    inDirectFlightStmt = conn.prepareStatement(INDIRECT_FLIGHT);
  }


  /* See QueryAbstract.java for javadoc */
  public String transaction_login(String username, String password) {
    try {
      if (currentUsername != null) {
        return "User already logged in\n";
      }
      ps2.setString(1, username);
      ResultSet result = ps2.executeQuery();

      if (result.next()) {
        byte[] storedHashedPassword = result.getBytes("hashedPassword");

        if ((PasswordUtils.plaintextMatchesSaltedHash(password, storedHashedPassword))) {
            currentUsername = username;
            return "Logged in as " + username + "\n";
        }
      }
      return "Login failed\n";
    } catch (SQLException e) {
      e.printStackTrace();
      return "Login failed\n";
    }
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_createCustomer(String username, String password, int initAmount) {
  try {
    byte[] saltedAndHashed = PasswordUtils.saltAndHashPassword(password);
    ps1.setString(1, username);
    ps1.setBytes(2, saltedAndHashed);
    ps1.setInt(3, initAmount);
    int resultSet = ps1.executeUpdate();
    if (resultSet > 0) {
      return "Created user " + username + "\n";
    } else {
      return "Failed to create user\n";
    }
  } catch (SQLException e) {
    e.printStackTrace();
    return "Failed to create user due to an exception\n";
  }
  }


  @SuppressWarnings("Duplicates")
public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
                                 int numberOfItineraries) {

    // Validation checks
    if (dayOfMonth < 1 || dayOfMonth > 31 || numberOfItineraries < 0) {
        return "Failed to search\n"; 
    }
  
    try {
        itineraries.clear();
        StringBuilder sb = new StringBuilder();

        // Retrieve the count of direct flights
        countDirectStatement.clearParameters();
        countDirectStatement.setString(1, originCity);
        countDirectStatement.setString(2, destinationCity);
        countDirectStatement.setInt(3, dayOfMonth);
        ResultSet flightDirect = countDirectStatement.executeQuery();

        flightDirect.next();
        int directFlightCount = flightDirect.getInt(1);
        flightDirect.close();

        // Calculate count for indirect flights if needed
        int inDirectFlightCount = directFlight ? 0 : Math.max(numberOfItineraries - directFlightCount, 0);

        // Fetch direct flights
        processFlightQuery(directFlightStmt, numberOfItineraries, originCity, destinationCity, dayOfMonth, true);

        // Fetch indirect flights
        if (!directFlight) {
            processFlightQuery(inDirectFlightStmt, inDirectFlightCount, originCity, destinationCity, dayOfMonth, false);
        }

        // Sort and construct the result string
        Collections.sort(itineraries);
        itineraries.forEach(itinerary -> sb.append(itinerary.toString()));

        return sb.length() == 0 ? "No flights match your selection\n" : sb.toString();
    } catch (SQLException e) {
        e.printStackTrace();
        return "Failed to search\n";
    }
}

private void processFlightQuery(PreparedStatement stmt, int limit, String originCity, String destinationCity, int dayOfMonth, boolean isDirect) throws SQLException {
    stmt.clearParameters();
    stmt.setInt(1, limit);
    stmt.setString(2, originCity);
    stmt.setString(3, destinationCity);
    stmt.setInt(4, dayOfMonth);
    ResultSet results = stmt.executeQuery();

    while (results.next()) {
        Itinerary itinerary = createItinerary(results, isDirect);
        itineraries.add(itinerary);
    }
    results.close();
}

private Itinerary createItinerary(ResultSet rs, boolean isDirect) throws SQLException {
    int fid = rs.getInt("fid");
    int day_of_month = rs.getInt("day_of_month");
    String carrier_id= rs.getString("carrier_id");
    String flight_num = rs.getString("flight_num");
    String origin = rs.getString("origin_city");
    String dest_city = rs.getString("dest_city");
    int time = rs.getInt("actual_time");
    int capacity = rs.getInt("capacity");
    String price = rs.getString("price");
    Flight_direct flight = new Flight_direct(fid, day_of_month, carrier_id, flight_num, origin, dest_city, time, capacity, price);
    return new Itinerary(flight);
}



  public String transaction_book(int itineraryId) {
    if (currentUsername == null ) {
      System.out.println("hello" + currentUsername);
      return "Cannot book reservations, not logged in\n";
    }


    if (itineraryId > itineraries.size() || itineraries.size() == 0) {
      System.out.println("hello 3");
      return "No such itinerary hello " + itineraryId + "\n";
    }


    Itinerary itinerary = itineraries.get(itineraryId); 
    
    
    try {
      String sameDayCheckQuery = "SELECT * FROM Reservations_chinehuu JOIN FLIGHTS ON Reservations_chinehuu.fid1 = FLIGHTS.fid" +
      "OR Reservations_chinehuu.fid2 = FLIGHTS.fid WHERE username = ? AND month_id = ?" +
      "AND day_of_month = ?"; 


      PreparedStatement pst1 = conn.prepareStatement(sameDayCheckQuery);
      pst1.setString(1, currentUsername);
      pst1.setInt(2, itinerary.flight1.month);
      pst1.setInt(3, itinerary.flight1.dayOfMonth);
      ResultSet rs = pst1.executeQuery();

      System.out.println("hello 3");

      if (rs.next()) {
        return "You cannot book two flights in the same day\n";
      }

      if (!checkFlightCapacity(itinerary.flight1.fid)) {
        return "Flight " + itinerary.flight1.fid + " is fully booked\n";
      }

      if (itinerary.flight2 != null && !checkFlightCapacity(itinerary.flight2.fid)) {
        return "Flight " + itinerary.flight2.fid + " is fully booked\n";
      }

      String insertQuery = "INSERT INTO Reservations_chinehuu (res_id, paid, username, fid1, fid2) VALUES (?, 0, ?, ?, ?)";
      PreparedStatement pst2 = conn.prepareStatement(insertQuery);
      int reservationId = getNextReservationId();
      pst2.setInt(1, reservationId);
      pst2.setString(2, currentUsername);
      pst2.setInt(3, itinerary.flight1.fid);
      pst2.setInt(4, itinerary.flight2 != null ? itinerary.flight2.fid : java.sql.Types.INTEGER);
      int updateCount = pst2.executeUpdate();
      if (updateCount > 0) {
        return "Booked flight(s), reservation ID: " + reservationId + "\n";
      }
      return "Booking failed  1\n";
    } catch(SQLException e) {
      e.printStackTrace();
      if (isDeadlock(e)) {
        return transaction_book(itineraryId);
      }
      return "Booking failed  2\n";
    }
  }

  private boolean checkFlightCapacity(int fid) throws SQLException {
    try {
      String countQuery = "SELECT COUNT(*) as bookedCount FROM Reservations_chinehuu WHERE fid1 = ? OR fid2 = ?";
      PreparedStatement pst = conn.prepareStatement(countQuery);
      pst.setInt(1, fid);
      pst.setInt(2, fid);
      ResultSet rs1 = pst.executeQuery();
      int bookedCount = 0;
      if (rs1.next()) {
        bookedCount = rs1.getInt("bookedCount");
      }

      String capacityQuery = "SELECT capacity FROM FLIGHTS WHERE fid = ?";
      PreparedStatement pst1 = conn.prepareStatement(capacityQuery);
      pst1.setInt(1, fid);
      ResultSet rs2 = pst.executeQuery();
      if (rs2.next()) {
        int capacity = rs2.getInt("capacity");  
        return bookedCount < capacity ;
      }

    } catch( SQLException e) {
      e.printStackTrace();
    }
    return false;
  }

  private int getNextReservationId() throws SQLException {
    String query = "SELECT MAX(res_id) FROM Reservations_chinehuu";
    try (PreparedStatement pst = conn.prepareStatement(query);
         ResultSet rs = pst.executeQuery()) {
        if (rs.next()) {
            return rs.getInt(1) + 1;  // Increment the highest ID found by 1
        } else {
            return 1;  // Start from 1 if no IDs are found (i.e., the table is empty)
        }
    }
}



public String transaction_pay(int reservationId) {
  if (currentUsername == null) {
      return "Cannot pay, not logged in\n";
  }

  try {
      conn.setAutoCommit(false);  // Start transaction
      // Check reservation details
      String reservationCheckQuery = "SELECT price, paid, username FROM Reservations_chinehuu WHERE res_id = ?";
      PreparedStatement checkRes = conn.prepareStatement(reservationCheckQuery);
      checkRes.setInt(1, reservationId);
      ResultSet rs = checkRes.executeQuery();

      if (!rs.next()) {
          conn.rollback();
          return "Cannot find unpaid reservation " + reservationId + " under user: " + currentUsername + "\n";
      }

      boolean isPaid = rs.getBoolean("paid");
      if (isPaid) {
          conn.rollback();
          return "Reservation " + reservationId + " is already paid.\n";
      }

      String owner = rs.getString("username");
      if (!currentUsername.equals(owner)) {
          conn.rollback();
          return "Cannot find unpaid reservation " + reservationId + " under user: " + currentUsername + "\n";
      }

      double price = rs.getDouble("price");

      // Check user's balance
      String balanceQuery = "SELECT balance FROM Users_chinehuu WHERE username = ?";
      PreparedStatement balanceStmt = conn.prepareStatement(balanceQuery);
      balanceStmt.setString(1, currentUsername);
      ResultSet balanceResult = balanceStmt.executeQuery();

      if (!balanceResult.next()) {
          conn.rollback();
          return "Failed to pay for reservation " + reservationId + "\n";
      }

      double balance = balanceResult.getDouble("balance");
      if (balance < price) {
          conn.rollback();
          return "User has only " + balance + " in account but itinerary costs " + price + "\n";
      }

      // Update reservation to paid
      String updateResQuery = "UPDATE Reservations_chinehuu SET paid = TRUE WHERE res_id = ?";
      PreparedStatement updateResStmt = conn.prepareStatement(updateResQuery);
      updateResStmt.setInt(1, reservationId);
      updateResStmt.executeUpdate();

      // Update user's balance
      double newBalance = balance - price;
      String updateBalanceQuery = "UPDATE Users_chinehuu SET balance = ? WHERE username = ?";
      PreparedStatement updateBalanceStmt = conn.prepareStatement(updateBalanceQuery);
      updateBalanceStmt.setDouble(1, newBalance);
      updateBalanceStmt.setString(2, currentUsername);
      updateBalanceStmt.executeUpdate();

      conn.commit();  // Commit transaction
      return "Paid reservation: " + reservationId + " remaining balance: " + newBalance + "\n";
  } catch (SQLException e) {
      e.printStackTrace();
      try {
          if (conn != null) conn.rollback();
      } catch (SQLException se2) {
          se2.printStackTrace();
      }
      return "Failed to pay for reservation " + reservationId + "\n";
  } finally {
      try {
          if (conn != null) conn.setAutoCommit(true);
      } catch (SQLException se) {
          se.printStackTrace();
      }
  }
}




  
public String transaction_reservations() {
  if (currentUsername == null) {
      return "Cannot view reservations, not logged in\n";
  }

  try {
      String query = "SELECT res_id, paid, fid1, fid2 FROM Reservations_chinehuu WHERE username = ?";
      PreparedStatement pst = conn.prepareStatement(query);
      pst.setString(1, currentUsername);
      ResultSet rs = pst.executeQuery();

      StringBuilder sb = new StringBuilder();
      boolean hasReservations = false;
      while (rs.next()) {
          hasReservations = true;
          int resId = rs.getInt("res_id");
          boolean paid = rs.getBoolean("paid");
          int fid1 = rs.getInt("fid1");
          int fid2 = rs.getInt("fid2");

          sb.append("Reservation " + resId + " paid: " + paid + ":\n");
          sb.append(getFlightDetails(fid1));
          if (fid2 != 0) {
              sb.append(getFlightDetails(fid2));
          }
          sb.append("\n");
      }
      if (!hasReservations) {
          return "No reservations found\n";
      }
      return sb.toString();
  } catch (SQLException e) {
      e.printStackTrace();
      return "Failed to retrieve reservations\n";
  }
}

private String getFlightDetails(int fid) throws SQLException {
  String query = "SELECT * FROM FLIGHTS WHERE fid = ?";
  PreparedStatement pst = conn.prepareStatement(query);
  pst.setInt(1, fid);
  ResultSet rs = pst.executeQuery();
  if (rs.next()) {
      return new Flight_direct(rs.getInt("fid"), rs.getInt("day_of_month"), rs.getString("carrier_id"), rs.getString("flight_num"),
                                rs.getString("origin_city"), rs.getString("dest_city"), rs.getInt("actual_time"), rs.getInt("capacity"), rs.getString("price")).toString() + "\n";
  }
  return "";
}


     



  /**
   * Utility function to determine whether an error was caused by a deadlock
   */
  private static boolean isDeadlock(SQLException e) {
    return e.getErrorCode() == 1205;
  }



  class Itinerary implements Comparable<Itinerary> {
      public Flight_direct flight1;
      public Flight_direct flight2;
      public int time;
      public boolean direct;
      public int month; 
  
      Itinerary(Flight_direct f1) {
        this.flight1 = f1;
        this.direct = true;
        this.time = f1.actualTime;
        this.month = f1.month;
      }
  
      Itinerary(Flight_direct f1, Flight_direct f2) {
        this.flight1 = f1;
        this.flight2 = f2;
        this.direct = false;
        this.time = f1.actualTime + f2.actualTime;
        this.month = f1.month;
      }
  
      public int getDay() {
        return flight1.dayOfMonth;
      }

      public int getMonth() {
        return this.month;
      }
  
      public String toString() {
        String output = "Itinerary " + itineraryCounter + ": ";
        if (direct) {
          output += "1 ";
        } else {
          output += "2 ";
        }
        output += "flight(s), " + this.time + " minutes\n" + flight1 + "\n";
        if (!direct) {
          output += flight2 + "\n";
        }
        return output;
      }
  
      public int compareTo(Itinerary other) {
        if (this.time != other.time) {
          return -1 * Integer.compare(this.time, other.time);
        } 
         else if (this.flight1.fid != other.flight1.fid) {
          return -1 * Integer.compare(this.flight1.fid, other.flight1.fid);
        } else if (this.flight2 == null && other.flight2 == null) {
          return 0;
        } else if (this.flight2 == null) {
          return -1;
        } else if (other.flight2 == null) {
          return 1;
        } else {
          return -1 * Integer.compare(this.flight2.fid, other.flight2.fid);
        }
      }
    }




  class Flight_direct {
    private int fid;
    private int dayOfMonth;
    private int month;
    private String carrierId;
    private String flightNum;
    private String originCity;
    private String destCity;
    private int actualTime;
    private int capacity;
    private String price;


    Flight_direct(int id, int day, String carrier, String fnum, String origin, String dest, int tm, int cap, String pri) {
      fid = id;
      dayOfMonth = day;
      carrierId = carrier;
      flightNum = fnum;
      originCity = origin;
      destCity = dest;
      actualTime = tm;
      capacity = cap;
      price = pri; 

    }
  
    public String toString() {
      return "ID: " + fid +
             " Day: " + dayOfMonth + 
             " Carrier: " + carrierId + 
             " Number: " + flightNum + 
             " Origin: " + originCity + 
             " Dest: " + destCity + 
             " Duration: " + actualTime + 
             " Capacity: " + capacity + 
             " Price: " + price;
    }
  }
}

