import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.LoggerFactory;


public class TestingOne{
	
	static final org.slf4j.Logger logger = LoggerFactory.getLogger(TestingOne.class);
	private static long beginTime = Timestamp.valueOf("2005-01-01 00:00:00").getTime();
	private static long endTime = Timestamp.valueOf("2017-12-01 11:59:59").getTime();

	public static boolean USE_TMP_TABLE = true;	
	private static String[] CASETYPEID_CARD = {"0003", "0008"};
	private static String[] CASETYPEID_LOAN = {"0001", "0002", "0004", "0005", "0006", "0007"};

	
	public static void main(String[] args) throws Exception {

		System.out.println("QUERY START");		

        Class.forName("oracle.jdbc.driver.OracleDriver");
        try (Connection conn = DriverManager.getConnection(
                "jdbc:oracle:thin:@localhost:1522:wcs", "wcss_owner",
                "oracle")) {
            generateRecords(conn, 500000);
        }       

        System.out.println("QUERY END");
    }
	
	public static java.sql.Date timeOutputter(java.sql.Date sqlDate) {
		
		java.util.Date randomDate;
        
        randomDate = new java.util.Date(beginTime 
    			+ (long)(Math.random() * (endTime - beginTime + 1)));
    	sqlDate = new java.sql.Date(randomDate.getTime());
        
		return sqlDate;
	}
	
	public static void generateRecords(Connection conn, int count) throws Exception {
        conn.setAutoCommit(false);        	
        /**
         * FIXME: The "CLOSE_DATE" should be distributed randomly and equally between 2005-01-01 and
         * 2017-12-01. However the "CREATE_DATE" can be hard-coded.
         */
        /**
         * FIXME: The "CASE_TYPE_ID" should be depending on the case category (LOAN or CARD). For
         * LOAN cases (which mean the case also in CM_LOAN_CASE_HS) should have the case types of
         * "0001,0002,0004,0005,0006,0007". For the CARD cases (which mean the case also in
         * CM_CARD_CASE_HS) should have the case types of "0003,0008". Please try to distributed
         * them equally.
         */
        String sql, custName;
        java.sql.Date sqlDate = null;
        
        if (USE_TMP_TABLE) 
        	sql = "INSERT ALL INTO TMP_CM_CASE_HS (CASE_ID, WFID, CREATE_DATE, CLOSE_DATE, PROJECT_ID, STATUS_ID, CASE_TYPE_ID, UPDATE_FLAG, CFG_ID, UN_WORKED, CASE_PRIORITY, ACTION_CODE_ID, PTP_BROKEN_FLAG, REASSIGN_DATE, NOREASSIGN_FLAG, PTP_STATUS) "
        			+ "VALUES (?, '-1', TO_DATE('2008-12-19 10:17:30', 'YYYY-MM-DD HH:MI:SS'), ?, '10000001', '0090', ?, 'Y', '0205', 'C', NULL, NULL, NULL, TO_DATE('2011-12-19 11:00:00', 'YYYY/MM/DD HH:MI:SS'), 'N', 'N') "
        			+ "INTO TMP_CM_ACCOUNT_HS (CASE_ID, ACCT_ENTITY_ID, SHOW_NAME, CFG_ID) VALUES (?, ?, ?, '0105') "
        			+ "INTO TMP_CM_BASIC_CC_HS (CASE_ID, HX_DTE) VALUES (?, ?) "
        			+ "SELECT * FROM DUAL";
        else
        	sql = "INSERT ALL INTO CM_CASE_HS (CASE_ID, WFID, CREATE_DATE, CLOSE_DATE, PROJECT_ID, STATUS_ID, CASE_TYPE_ID, UPDATE_FLAG, CFG_ID, UN_WORKED, CASE_PRIORITY, ACTION_CODE_ID, PTP_BROKEN_FLAG, REASSIGN_DATE, NOREASSIGN_FLAG, PTP_STATUS) "
    				+ "VALUES (?, '-1', TO_DATE('2008-12-19 10:17:30', 'YYYY-MM-DD HH:MI:SS'), ?, '10000001', '0090', ?, 'Y', '0205', 'C', NULL, NULL, NULL, TO_DATE('2011-12-19 11:00:00', 'YYYY/MM/DD HH:MI:SS'), 'N', 'N') "
    				+ "INTO CM_ACCOUNT_HS (CASE_ID, ACCT_ENTITY_ID, SHOW_NAME, CFG_ID) VALUES (?, ?, ?, '0105') "
    				+ "INTO CM_BASIC_CC_HS (CASE_ID, HX_DTE) VALUES (?, ?) "
    				+ "SELECT * FROM DUAL";
        
		try (PreparedStatement ps = conn.prepareStatement(sql)) {

		    for(int i = 0; i < count; i++) {
		    	
		    	sqlDate = timeOutputter(sqlDate);
		    	
		    	custName = RandomStringUtils.random(5, String.valueOf(System.currentTimeMillis()));
		    	
		 	    ps.setString(1, "1" + String.format("%019d", i));
		 	    ps.setDate(2, sqlDate);
		 	    
		 	    if (i < count * 8 / 10)
		 	    	ps.setString(3, CASETYPEID_LOAN[i % 6]);
		 		else
		 			ps.setString(3, CASETYPEID_CARD[i % 2]);
		 	    
		 	    ps.setString(4, "1" + String.format("%019d", i));
		 	    ps.setString(5, "1" + String.format("%019d", i));
		 	    ps.setString(6, custName);
		 	    ps.setString(7, "1" + String.format("%019d", i));
		 	    ps.setDate(8, sqlDate);
		    	
		 	    ps.addBatch();
	          
		 	    if (i % 1000 == 0 && i != 0) {
		 	        ps.executeBatch();
		 	        conn.commit();
		 	        System.out.println(String.format("Inserted %d CASE/ACCOUNT/BASIC records...", i));
		 	    }
		    }
		    
		    ps.executeBatch();
	    }
	    
	    /**
		 *************************************************************
		 *                      CM_LOAN_CASE_HS                      *
		 *************************************************************
	    */
		/**
         * XXX: For the "CIF_NO", please try to make it with the mapping ration of 1:5~100
         * (CIF:cases).
         */
	    Random rand = new Random();
	    
		int id = 0, randomNum;
		
		if (USE_TMP_TABLE) 
			sql = "INSERT INTO TMP_CM_LOAN_CASE_HS (CASE_ID, CUST_NAME, CIF_NO) VALUES (?, ?, ?)";
		else
			sql = "INSERT INTO CM_LOAN_CASE_HS (CASE_ID, CUST_NAME, CIF_NO) VALUES (?, ?, ?)";
		
	    try (PreparedStatement ps = conn.prepareStatement(sql)) {
		    for(int i = 0; i < count * 8 / 10;) {
		    	randomNum = rand.nextInt(100) + 5;
		    	
		    	if( i + randomNum >= count * 8 / 10) 
		    		randomNum = count * 8 / 10 - i;
		    	
		    	for (int j = 0; j < randomNum; j++) {
		    	    /**
		    	     * XXX: For customer name randomization, please use something like RandomStringUtils.
		    	     */
			    	custName = RandomStringUtils.random(5, String.valueOf(System.currentTimeMillis()));
			    	ps.setString(1, "1" + String.format("%019d", i));
			 	    ps.setString(2, custName.toString());
			 	    ps.setString(3, String.format("%09d", id));
			 	    ps.addBatch();
			 	    
					i++;
			    }  
	          
			 	if (i % 10000 >= 0 && i % 10000 <= 10) {
			 	    ps.executeBatch();
			 	    conn.commit();
		 	        ps.clearBatch();
			 	    System.out.println(String.format("Inserted %d CM_LOAN_CASE_HS records...", i));
			 	}
			 	
				id++;
		    }
		    ps.executeBatch();
	    }

	    /**
		 *************************************************************
		 *                      CM_CARD_CASE_HS                      *
		 *************************************************************
	    */

	    id = 0;
	    
	    if (USE_TMP_TABLE)
	    	sql = "INSERT INTO TMP_CM_CARD_CASE_HS (CASE_ID, CIF_NO, CUST_NAME) VALUES (?, ?, ?)";
	    else
	    	sql = "INSERT INTO CM_CARD_CASE_HS (CASE_ID, CIF_NO, CUST_NAME) VALUES (?, ?, ?)";
	    
	    try (PreparedStatement ps = conn.prepareStatement(sql)) {
		    for(int i = count * 8 / 10; i < count;) {
		    	randomNum = rand.nextInt(100) + 5;
		    	
		    	if( i + randomNum >= count) 
		    		randomNum = count - i;
                /**
                 * XXX: For customer name randomization, please use something like RandomStringUtils.
                 */
                for (int j = 0; j < randomNum; j++) {
			    	custName = RandomStringUtils.random(5, String.valueOf(System.currentTimeMillis()));
			 	    ps.setString(1, "1" + String.format("%019d", i));
			 	    ps.setString(2, String.format("%09d", id));
			 	    ps.setString(3, custName.toString());
			 	    ps.addBatch();	      

			 	    i++;
		 	    }  
	  
		 	    if (i % 10000 == 0 && i != 0) {
		 	        ps.executeBatch();
		 	        conn.commit();
		 	        ps.clearBatch();
		 	        System.out.println(String.format("Inserted %d CM_CARD_CASE_HS records...", i));
		 	    }
		 	    
		 	    // Add a random number to distribute the ID for both LOAN and CARD assignment
		 	    randomNum = rand.nextInt(10) + 1;
				id += randomNum;
		    }
		    ps.executeBatch();
	    }

	    /**
		 *************************************************************
		 *           CM_LEGAL_CASE_HS + CM_CLAIM_CASE_HS             *
		 *************************************************************
	    */
	    /**
         * XXX: Not every cases have the associated legal/claim cases, but maybe only 10% of the
         * total cases should have legal/claim cases associated.
         */
	    
	    if (USE_TMP_TABLE) 
	    	sql = "INSERT ALL INTO TMP_CM_LEGAL_CASE_HS (CASE_ID, LEGAL_ID, WRIT_DATE) VALUES (?, ?, TO_DATE('2008-12-19 10:17:30', 'YYYY/MM/DD HH:MI:SS')) "
	    				+ "INTO TMP_CM_CLAIM_CASE_HS (CASE_ID, CLAIM_ID, JUDGMENT_H_DATE, status) VALUES (?, ?, TO_DATE('2011-12-20 11:00:00', 'YYYY/MM/DD HH:MI:SS'), 'A') "
	    				+ "SELECT * FROM DUAL";
	    else
	    	sql = "INSERT ALL INTO CM_LEGAL_CASE_HS (CASE_ID, LEGAL_ID, WRIT_DATE) VALUES (?, ?, TO_DATE('2008-12-19 10:17:30', 'YYYY/MM/DD HH:MI:SS')) "
    				+ "INTO CM_CLAIM_CASE_HS (CASE_ID, CLAIM_ID, JUDGMENT_H_DATE, status) VALUES (?, ?, TO_DATE('2011-12-20 11:00:00', 'YYYY/MM/DD HH:MI:SS'), 'A') "
    				+ "SELECT * FROM DUAL";
	    
	    try (PreparedStatement ps = conn.prepareStatement(sql)) {
		    for(int i = 0; i < count / 20; i++) {		 	    
		 	    ps.setString(1, "1" + String.format("%019d", i));
		 	    ps.setString(2, "1" + String.format("%019d", i));
		 	    ps.setString(3, "1" + String.format("%019d", i + count / 20));
		 	    ps.setString(4, "1" + String.format("%019d", i + count / 20));
		 	    ps.addBatch();		  
		 	          
		 	    if (i % 1000 == 0 && i != 0) {
		 	        ps.executeBatch();
		 	        conn.commit();
		 	        ps.clearBatch();
		 	        System.out.println(String.format("Inserted %d LEGAL&CLAIM_CASE_HS records...", i));
		 	    }
		    }
		    ps.executeBatch();
	    }
    }
}