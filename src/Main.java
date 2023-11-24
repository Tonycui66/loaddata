/*
 * LoadData - Load Sample Data directly into database tables or into
 * CSV files using multiple parallel workers.
 *
 * Copyright (C) 2016, Denis Lussier
 * Copyright (C) 2016, Jan Wieck
 *
 */

import java.nio.FloatBuffer;
import java.sql.*;
import java.util.*;
import java.io.*;
import java.lang.Integer;

public class Main
{
    private static Properties   ini = new Properties();
    private static String       db;
    private static Properties   dbProps;
    private static jTPCCRandom  rnd;
    private static String       fileLocation = null;
    private static String       csvNullValue = null;
    private static String       hint = null;
    private static int          numWarehouses;
    private static int          numWorkers;
    private static int          nextJob = 0;
    private static Object       nextJobLock = new Object();

    private static LoadDataWorker[] workers;
    private static Thread[]     workerThreads;

    private static String[]     argv;

    private static boolean              writeCSV = false;
    private static BufferedWriter       configCSV = null;
    private static BufferedWriter       orderLineCSV = null;
    private static BufferedWriter       newOrderCSV = null;



    public static void main(String[] args) {
        int     i;
        int commitbatch = 0;
        int totalrows = 0;
        String flag = null;
//        if (args.length == 1) {
//            flag = args[0];
//        }else if(args.length == 2){
//            flag = args[0];
//            totalrows = Integer.parseInt(args[1]);
//        }else if(args.length == 3){
//            flag = args[0];
//            totalrows = Integer.parseInt(args[1]);
//            commitbatch = Integer.parseInt(args[2]);
//        }
        if (args.length != 3){
            System.out.println("请输出三个参数: \n"+
                                "\t\t操作类型: insert_infra_only|delete_value_infra_only|delete_value_infra_prep_tmp|delete_value_infra_prep_2\n"+
                                "\t\t操作类型的数据表的行数：表的数据量\n"+
                                "\t\t操作类型：批次的数量,insert_infra_only 建议10000,其它操作类型根据实际测试场景灵活配置\n");
            LoadDataWorker.printHelp();
        }
        flag = args[0];
        totalrows = Integer.parseInt(args[1]);
        commitbatch = Integer.parseInt(args[2]);
        /*
         * Load the Benchmark properties file.
         */
        try
        {
            ini.load(new FileInputStream("props.qb"));
        }
        catch (IOException e)
        {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
        argv = args;

        /*
         * Initialize the global Random generator that picks the
         * C values for the load.
         */
        rnd = new jTPCCRandom();

        /*
         * Load the JDBC driver and prepare the db and dbProps.
         */
        try {
            Class.forName(iniGetString("driver"));
        }
        catch (Exception e)
        {
            System.err.println("ERROR: cannot load JDBC driver - " +
                    e.getMessage());
            System.exit(1);
        }
        db = iniGetString("conn");
        dbProps = new Properties();
        dbProps.setProperty("user", iniGetString("user"));
        dbProps.setProperty("password", iniGetString("password"));

        /*
         * Parse other vital information from the props file.
         */
        numWarehouses   = iniGetInt("warehouses");
        numWorkers      = iniGetInt("loadWorkers", 4);
        if (numWarehouses< numWorkers){
            System.out.println("配置文件warehouse 小于loadWorker的数值了");
            LoadDataWorker.printHelp();
        }

        fileLocation    = iniGetString("fileLocation");
        csvNullValue    = iniGetString("csvNullValue", "NULL");
        hint    = iniGetString("hint");
        if (flag==null) {
            flag = Main.iniGetString("update").trim();
        }
        if (commitbatch == 0) {
            commitbatch = Main.iniGetInt("commitbatch");
        }
//        System.out.println(flag + commitbatch);
//        System.exit(1);
        /*
         * If CSV files are requested, open them all.
         */
        if (fileLocation != null)
        {
            writeCSV = true;

            try
            {
                orderLineCSV = new BufferedWriter(new FileWriter(fileLocation +
                        "order-line.csv"));
            }
            catch (IOException ie)
            {
                System.err.println(ie.getMessage());
                System.exit(3);
            }
        }

        System.out.println("");
        Long sessionstartTimestamp = System.currentTimeMillis();

        /*
         * Create the number of requested workers and start them.
         */
        workers = new LoadDataWorker[numWorkers];
        workerThreads = new Thread[numWorkers];
        for (i = 0; i < numWorkers; i++)
        {
            Connection dbConn;

            try
            {
                dbConn = DriverManager.getConnection(db, dbProps);
                dbConn.setAutoCommit(false);
                if(hint.equals("hint")) {
                    workers[i] = new LoadDataWorker(i, dbConn, rnd.newRandom(),flag,commitbatch,totalrows,numWarehouses,hint);
                }else{
                    workers[i] = new LoadDataWorker(i, dbConn, rnd.newRandom(),flag,commitbatch,totalrows,numWarehouses);
                }
                workerThreads[i] = new Thread(workers[i]);
                workerThreads[i].start();
            }
            catch (SQLException se)
            {
                System.err.println("ERROR: " + se.getMessage());
                System.exit(3);
                return;
            }

        }

        for (i = 0; i < numWorkers; i++)
        {
            try {
                workerThreads[i].join();
            }
            catch (InterruptedException ie)
            {
                System.err.println("ERROR: worker " + i + " - " +
                        ie.getMessage());
                System.exit(4);
            }
        }

        /*
         * Close the CSV files if we are writing them.
         */
        if (writeCSV)
        {
            try
            {
                orderLineCSV.close();
            }
            catch (IOException ie)
            {
                System.err.println(ie.getMessage());
                System.exit(3);
            }
        }
        Long sessionendTimestamp = System.currentTimeMillis();
        System.out.println("Exec "+ flag + "耗时: "+ (sessionendTimestamp-sessionstartTimestamp) + "ms");
    } // End of main()
    public static void orderLineAppend(StringBuffer buf)
            throws IOException
    {
        synchronized(orderLineCSV)
        {
            orderLineCSV.write(buf.toString());
        }
        buf.setLength(0);
    }


    public static int getNextJob()
    {
        int     job;

        synchronized(nextJobLock)
        {
            if (nextJob > numWarehouses)
                job = -1;
            else
                job = nextJob++;
        }

        return job;
    }

    public static int getNumWarehouses()
    {
        return numWarehouses;
    }

    public static String iniGetString(String name)
    {
        String  strVal = null;

        for (int i = 0; i < argv.length - 1; i += 2)
        {
            if (name.toLowerCase().equals(argv[i].toLowerCase()))
            {
                strVal = argv[i + 1];
                break;
            }
        }

        if (strVal == null)
            strVal = ini.getProperty(name);

        if (strVal == null)
            System.out.println(name + " (not defined)");
        else
        if (name.equals("password"))
            System.out.println(name + "=***********");
        else
            System.out.println(name + "=" + strVal);
        return strVal;
    }

    private static String iniGetString(String name, String defVal)
    {
        String  strVal = null;

        for (int i = 0; i < argv.length - 1; i += 2)
        {
            if (name.toLowerCase().equals(argv[i].toLowerCase()))
            {
                strVal = argv[i + 1];
                break;
            }
        }

        if (strVal == null)
            strVal = ini.getProperty(name);

        if (strVal == null)
        {
            System.out.println(name + " (not defined - using default '" +
                    defVal + "')");
            return defVal;
        }
        else
        if (name.equals("password"))
            System.out.println(name + "=***********");
        else
            System.out.println(name + "=" + strVal);
        return strVal;
    }

    private static int iniGetInt(String name)
    {
        String  strVal = iniGetString(name);

        if (strVal == null)
            return 0;
        return Integer.parseInt(strVal);
    }

    private static int iniGetInt(String name, int defVal)
    {
        String  strVal = iniGetString(name);

        if (strVal == null)
            return defVal;
        return Integer.parseInt(strVal);
    }
}


class jTPCCRandom
{
    private static final char[] aStringChars = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
            'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
            'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
    private static final String[] cLastTokens = {
            "BAR", "OUGHT", "ABLE", "PRI", "PRES",
            "ESE", "ANTI", "CALLY", "ATION", "EING"};

    private static long         nURandCLast;
    private static long         nURandCC_ID;
    private static long         nURandCI_ID;
    private static boolean      initialized = false;

    private     Random  random;

    /*
     * jTPCCRandom()
     *
     *     Used to create the master jTPCCRandom() instance for loading
     *     the database. See below.
     */
    jTPCCRandom()
    {
        if (initialized)
            throw new IllegalStateException("Global instance exists");

        this.random = new Random(System.nanoTime());
        jTPCCRandom.nURandCLast = nextLong(0, 255);
        jTPCCRandom.nURandCC_ID = nextLong(0, 1023);
        jTPCCRandom.nURandCI_ID = nextLong(0, 8191);

        initialized = true;
    }

    /*
     * jTPCCRandom(CLoad)
     *
     *     Used to create the master jTPCCRandom instance for running
     *     a benchmark load.
     *
     *     TPC-C 2.1.6 defines the rules for picking the C values of
     *     the non-uniform random number generator. In particular
     *     2.1.6.1 defines what numbers for the C value for generating
     *     C_LAST must be excluded from the possible range during run
     *     time, based on the number used during the load.
     */
    jTPCCRandom(long CLoad)
    {
        long delta;

        if (initialized)
            throw new IllegalStateException("Global instance exists");

        this.random = new Random(System.nanoTime());
        jTPCCRandom.nURandCC_ID = nextLong(0, 1023);
        jTPCCRandom.nURandCI_ID = nextLong(0, 8191);

        do
        {
            jTPCCRandom.nURandCLast = nextLong(0, 255);

            delta = Math.abs(jTPCCRandom.nURandCLast - CLoad);
            if (delta == 96 || delta == 112)
                continue;
            if (delta < 65 || delta > 119)
                continue;
            break;
        } while(true);

        initialized = true;
    }

    private jTPCCRandom(jTPCCRandom parent)
    {
        this.random = new Random(System.nanoTime());
    }

    /*
     * newRandom()
     *
     *     Creates a derived random data generator to be used in another
     *     thread of the current benchmark load or run process. As per
     *     TPC-C 2.1.6 all terminals during a run must use the same C
     *     values per field. The jTPCCRandom Class therefore cannot
     *     generate them per instance, but each thread's instance must
     *     inherit those numbers from a global instance.
     */
    jTPCCRandom newRandom()
    {
        return new jTPCCRandom(this);
    }


    /*
     * nextLong(x, y)
     *
     *     Produce a random number uniformly distributed in [x .. y]
     */
    public long nextLong(long x, long y)
    {
        return (long)(random.nextDouble() * (y - x + 1) + x);
    }

    /*
     * nextInt(x, y)
     *
     *     Produce a random number uniformly distributed in [x .. y]
     */
    public int nextInt(int x, int y)
    {
        return (int)(random.nextDouble() * (y - x + 1) + x);
    }

    /*
     * getAString(x, y)
     *
     *     Procude a random alphanumeric string of length [x .. y].
     *
     *     Note: TPC-C 4.3.2.2 asks for an "alhpanumeric" string.
     *     Comment 1 about the character set does NOT mean that this
     *     function must eventually produce 128 different characters,
     *     only that the "character set" used to store this data must
     *     be able to represent 128 different characters. '#@!%%ÄÖß'
     *     is not an alphanumeric string. We can save ourselves a lot
     *     of UTF8 related trouble by producing alphanumeric only
     *     instead of cartoon style curse-bubbles.
     */
    public String getAString(long x, long y)
    {
        String result = new String();
        long len = nextLong(x, y);
        long have = 1;

        if (y <= 0)
            return result;

        result += aStringChars[(int)nextLong(0, 51)];
        while (have < len)
        {
            result += aStringChars[(int)nextLong(0, 61)];
            have++;
        }

        return result;
    }

    /*
     * getNString(x, y)
     *
     *     Produce a random numeric string of length [x .. y].
     */
    public String getNString(long x, long y)
    {
        String result = new String();
        long len = nextLong(x, y);
        long have = 0;

        while (have < len)
        {
            result += (char)(nextLong((long)'0', (long)'9'));
            have++;
        }

        return result;
    }

    /*
     * getItemID()
     *
     *     Produce a non uniform random Item ID.
     */
    public int getItemID()
    {
        return (int)((((nextLong(0, 8191) | nextLong(1, 100000)) + nURandCI_ID)
                % 100000) + 1);
    }

    /*
     * getCustomerID()
     *
     *     Produce a non uniform random Customer ID.
     */
    public int getCustomerID()
    {
        return (int)((((nextLong(0, 1023) | nextLong(1, 3000)) + nURandCC_ID)
                % 3000) + 1);
    }

    /*
     * getCLast(num)
     *
     *     Produce the syllable representation for C_LAST of [0 .. 999]
     */
    public String getCLast(int num)
    {
        String result = new String();

        for (int i = 0; i < 3; i++)
        {
            result = cLastTokens[num % 10] + result;
            num /= 10;
        }

        return result;
    }

    /*
     * getCLast()
     *
     *     Procude a non uniform random Customer Last Name.
     */
    public String getCLast()
    {
        long num;
        num = (((nextLong(0, 255) | nextLong(0, 999)) + nURandCLast) % 1000);
        return getCLast((int)num);
    }

    public String getState()
    {
        String result = new String();

        result += (char)nextInt((int)'A', (int)'Z');
        result += (char)nextInt((int)'A', (int)'Z');

        return result;
    }

    /*
     * Methods to retrieve the C values used.
     */
    public long getNURandCLast()
    {
        return nURandCLast;
    }

    public long getNURandCC_ID()
    {
        return nURandCC_ID;
    }

    public long getNURandCI_ID()
    {
        return nURandCI_ID;
    }
} // end jTPCCRandom


class LoadDataWorker implements Runnable {
    private String hint = "nohint";
    private int worker;
    private Connection dbConn;
    private jTPCCRandom rnd;
    private String flag;
    private int commitbatch;
    private int totalrows;
    private int warehouses;
    private int connrows;

    private StringBuffer sb;
    private Formatter fmt;

    private boolean writeCSV = false;
    private String csvNull = null;

    private PreparedStatement stmtOrderLine = null;
    private PreparedStatement stmtOrderLineOnly = null;
    private PreparedStatement stmtOrderLineinfra = null;
    private PreparedStatement stmtInsertReturnOrderLine = null;
    private PreparedStatement stmtUpdateOrderLine = null;
    private PreparedStatement stmtUpdateOrderLineforunnest = null;
    private PreparedStatement stmtDeleteOrderLineforunnest = null;
    private PreparedStatement stmtDeleteetmcnapsprepforunnest = null;
    private PreparedStatement stmtDeleteetmcnapsprepforunnest_2 = null;
    private PreparedStatement stmtDeleteetmcnapsprepforunnest_tmp = null;
    private PreparedStatement stmtInsertUpdateOrderLine = null;
    private PreparedStatement stmtInsertUpdateBucketOrderLine = null;
    private PreparedStatement stmtUpsertBucketOrderLine = null;
    private PreparedStatement stmtDeleteOrderLine = null;
    private ResultSet rs =null;
    private StringBuffer sbConfig = null;
    private Formatter fmtConfig = null;
    private StringBuffer sbItem = null;
    private Formatter fmtItem = null;
    private StringBuffer sbWarehouse = null;
    private Formatter fmtWarehouse = null;
    private StringBuffer sbDistrict = null;
    private Formatter fmtDistrict = null;
    private StringBuffer sbStock = null;
    private Formatter fmtStock = null;
    private StringBuffer sbCustomer = null;
    private Formatter fmtCustomer = null;
    private StringBuffer sbHistory = null;
    private Formatter fmtHistory = null;
    private StringBuffer sbOrder = null;
    private Formatter fmtOrder = null;
    private StringBuffer sbOrderLine = null;
    private Formatter fmtOrderLine = null;
    private StringBuffer sbNewOrder = null;
    private Formatter fmtNewOrder = null;
    private static final String[] STRINGS = {
            "111111111111111111", // 0的取模结果
            "222222222222222222", // 1的取模结果
            "333333333333333333", // 2的取模结果
            "444444444444444444", // ...
            "555555555555555555",
            "666666666666666666",
            "777777777777777777",
            "888888888888888888",
            "999999999999999991",
            "AAAAAAAAAAAAAAAAAA",
            "BBBBBBBBBBBBBBBBBB",
            "CCCCCCCCCCCCCCCCCC",
            "DDDDDDDDDDDDDDDDDD",
            "EEEEEEEEEEEEEEEEEE",
            "FFFFFFFFFFFFFFFFFF",
            "G00000000000000000",
            "H21111111111111111",
            "I32222222222222222",
            "J43333333333333333",
            "K44444444444444444",
            "L55555555555555555",
            "M66666666666666666",
            "N77777777777777777",
            "O88888888888888888",
            "P99999999999999991",
            "QAAAAAAAAAAAAAAAAA",
            "RBBBBBBBBBBBBBBBBB",
            "SCCCCCCCCCCCCCCCCC",
            "TDDDDDDDDDDDDDDDDD",
            "UEEEEEEEEEEEEEEEEE",
            "VFFFFFFFFFFFFFFFFF",
            "W00000000000000000"  // 31的取模结果
    };


    LoadDataWorker(int worker, Connection dbConn, jTPCCRandom rnd,String flag,int commitbatch,int totalrows,int warehouses)
            throws SQLException {
        this.worker = worker;
        this.dbConn = dbConn;
        this.rnd = rnd;
        this.flag = flag;
        this.commitbatch = commitbatch;
        this.totalrows = totalrows;
        this.warehouses = warehouses;
        this.connrows = Math.round(totalrows/warehouses);

        this.sb = new StringBuffer();
        this.fmt = new Formatter(sb);


        stmtOrderLine = dbConn.prepareStatement(
                "INSERT INTO bmsql_order_line (" +
                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                        "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
                        "  ol_amount, ol_dist_info) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );

        stmtOrderLineOnly = dbConn.prepareStatement(
                "INSERT INTO bmsql_order_line (" +
                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                        "  ol_supply_w_id, ol_delivery_d, ol_delivery_now,ol_quantity, " +
                        "  ol_amount, ol_dist_info) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)"
        );
        stmtOrderLineinfra = dbConn.prepareStatement(
                "INSERT INTO etm_cnaps_prep (" +
                        "  bankno,pbocseqno ,msgsno,dttran ,telseqno,flginto,dtfront,frontseqno,bakfld1," +
                        "  bakfld2, bakfld3, bakamt,updtms, modteller,modbrc ,moddate," +
                        "  stsrcd, crttime,updtime,version) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );
        stmtInsertReturnOrderLine = dbConn.prepareStatement(
                "INSERT INTO bmsql_order_line (" +
                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                        "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
                        "  ol_amount, ol_dist_info) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                        " return ol_w_id,ol_d_id,ol_o_id,ol_number"
        );
        stmtUpdateOrderLine = dbConn.prepareStatement(
                "update bmsql_order_line@{no_full_scan} " +
                        "  set ol_i_id = ? " +
                        "  where ol_o_id =? and  ol_d_id = ? and " +
                        "ol_w_id = ? and ol_number =?"
        );
        stmtUpdateOrderLineforunnest = dbConn.prepareStatement(
                "update bmsql_order_line@{no_full_scan} " +
                        "  set ol_i_id = data.d1 " +
                        " from (select unnest(?),unnest(?),unnest(?),unnest(?),unnest(?)) as data(d1,d2,d3,d4,d5)" +
                "  where ol_o_id =data.d2 and  ol_d_id = data.d3 and " +
                        "ol_w_id = data.d4 and ol_number = data.d5"
        );

        stmtDeleteOrderLine = dbConn.prepareStatement(
                "delete from  bmsql_order_line " +
                        "  where ol_o_id =? and  ol_d_id = ? and " +
                        "ol_w_id = ? and ol_number =?"
        );

        stmtDeleteOrderLineforunnest = dbConn.prepareStatement(
                "delete from  bmsql_order_line " +
                        " using (select unnest(?),unnest(?),unnest(?),unnest(?)) as data(d1,d2,d3,d4)" +
                        "  where ol_o_id =data.d1 and  ol_d_id = data.d2 and " +
                        "ol_w_id = data.d3 and ol_number = data.d4"
        );

        stmtDeleteetmcnapsprepforunnest = dbConn.prepareStatement(
                "delete from  etm_cnaps_prep@{no_full_scan} " +
                        " using (select unnest(?),unnest(?),unnest(?)) as data(d1,d2,d3)" +
                        "  where telseqno =data.d1 and  dttran = data.d2 and " +
                        "bankno = data.d3"
        );
        stmtDeleteetmcnapsprepforunnest_2 = dbConn.prepareStatement(
                "delete from  etm_cnaps_prep " +
                        "  where (telseqno,dttran,bankno) in "+
                        "  (select unnest(?),unnest(?),unnest(?))"
        );
        if (this.hint.equals("hint")){
            stmtDeleteetmcnapsprepforunnest_2 = dbConn.prepareStatement(
                    "delete from  etm_cnaps_prep@{no_full_scan} " +
                            "  where (telseqno,dttran,bankno) in "+
                            "  (select unnest(?),unnest(?),unnest(?))"
            );
        }
        stmtDeleteetmcnapsprepforunnest_tmp = dbConn.prepareStatement(Generatestr(commitbatch));
//        stmtInsertUpdateOrderLine = dbConn.prepareStatement(
//                "INSERT INTO bmsql_order_line (" +
//                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
//                        "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
//                        "  ol_amount, ol_dist_info) " +
//                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
//                        "ON CONFLICT (ol_o_id, ol_d_id, ol_w_id, ol_number) DO UPDATE SET ol_i_id = excluded.ol_i_id"
//        );
        stmtInsertUpdateOrderLine = dbConn.prepareStatement(
                "INSERT INTO bmsql_order_line (" +
                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                        "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
                        "  ol_amount, ol_dist_info) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (bmsql_o_l,ol_o_id, ol_d_id, ol_w_id, ol_number) DO UPDATE SET ol_i_id = excluded.ol_i_id"
        );

        stmtInsertUpdateBucketOrderLine = dbConn.prepareStatement(
                "INSERT INTO bmsql_order_line@{no_full_scan} (" +
                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                        "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
                        "  ol_amount, ol_dist_info) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (dbms_internal_ol_d_id_ol_number_ol_o_id_ol_w_id_shard_12,ol_o_id, ol_d_id, ol_w_id, ol_number) DO UPDATE SET ol_i_id = excluded.ol_i_id"
        );
        stmtUpsertBucketOrderLine = dbConn.prepareStatement(
                "upsert INTO bmsql_order_line@{no_full_scan} (" +
                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                        "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
                        "  ol_amount, ol_dist_info) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        );
    }
    LoadDataWorker(int worker, Connection dbConn, jTPCCRandom rnd,String flag,int commitbatch,int totalrows,int warehouses,String hint)
            throws SQLException {
        this.worker = worker;
        this.dbConn = dbConn;
        this.rnd = rnd;
        this.flag = flag;
        this.commitbatch = commitbatch;
        this.totalrows = totalrows;
        this.warehouses = warehouses;
        this.connrows = Math.round(totalrows/warehouses);

        this.sb = new StringBuffer();
        this.fmt = new Formatter(sb);
        this.hint = hint;


        stmtOrderLine = dbConn.prepareStatement(
                "INSERT INTO bmsql_order_line (" +
                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                        "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
                        "  ol_amount, ol_dist_info) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );

        stmtOrderLineOnly = dbConn.prepareStatement(
                "INSERT INTO bmsql_order_line (" +
                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                        "  ol_supply_w_id, ol_delivery_d, ol_delivery_now,ol_quantity, " +
                        "  ol_amount, ol_dist_info) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)"
        );
        stmtOrderLineinfra = dbConn.prepareStatement(
                "INSERT INTO etm_cnaps_prep (" +
                        "  bankno,pbocseqno ,msgsno,dttran ,telseqno,flginto,dtfront,frontseqno,bakfld1," +
                        "  bakfld2, bakfld3, bakamt,updtms, modteller,modbrc ,moddate," +
                        "  stsrcd, crttime,updtime,version) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );
        stmtInsertReturnOrderLine = dbConn.prepareStatement(
                "INSERT INTO bmsql_order_line (" +
                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                        "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
                        "  ol_amount, ol_dist_info) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                        " return ol_w_id,ol_d_id,ol_o_id,ol_number"
        );
        stmtUpdateOrderLine = dbConn.prepareStatement(
                "update bmsql_order_line@{no_full_scan} " +
                        "  set ol_i_id = ? " +
                        "  where ol_o_id =? and  ol_d_id = ? and " +
                        "ol_w_id = ? and ol_number =?"
        );
        stmtUpdateOrderLineforunnest = dbConn.prepareStatement(
                "update bmsql_order_line@{no_full_scan} " +
                        "  set ol_i_id = data.d1 " +
                        " from (select unnest(?),unnest(?),unnest(?),unnest(?),unnest(?)) as data(d1,d2,d3,d4,d5)" +
                        "  where ol_o_id =data.d2 and  ol_d_id = data.d3 and " +
                        "ol_w_id = data.d4 and ol_number = data.d5"
        );

        stmtDeleteOrderLine = dbConn.prepareStatement(
                "delete from  bmsql_order_line " +
                        "  where ol_o_id =? and  ol_d_id = ? and " +
                        "ol_w_id = ? and ol_number =?"
        );

        stmtDeleteOrderLineforunnest = dbConn.prepareStatement(
                "delete from  bmsql_order_line " +
                        " using (select unnest(?),unnest(?),unnest(?),unnest(?)) as data(d1,d2,d3,d4)" +
                        "  where ol_o_id =data.d1 and  ol_d_id = data.d2 and " +
                        "ol_w_id = data.d3 and ol_number = data.d4"
        );

        stmtDeleteetmcnapsprepforunnest = dbConn.prepareStatement(
                "delete from  etm_cnaps_prep@{no_full_scan} " +
                        " using (select unnest(?),unnest(?),unnest(?)) as data(d1,d2,d3)" +
                        "  where telseqno =data.d1 and  dttran = data.d2 and " +
                        "bankno = data.d3"
        );
        stmtDeleteetmcnapsprepforunnest_2 = dbConn.prepareStatement(
                "delete from  etm_cnaps_prep " +
                        "  where (telseqno,dttran,bankno) in "+
                        "  (select unnest(?),unnest(?),unnest(?))"
        );
        if (this.hint.equals("hint")){
            stmtDeleteetmcnapsprepforunnest_2 = dbConn.prepareStatement(
                    "delete from  etm_cnaps_prep@{no_full_scan} " +
                            "  where (telseqno,dttran,bankno) in "+
                            "  (select unnest(?),unnest(?),unnest(?))"
            );
        }
        stmtDeleteetmcnapsprepforunnest_tmp = dbConn.prepareStatement(Generatestr(commitbatch));
//        stmtInsertUpdateOrderLine = dbConn.prepareStatement(
//                "INSERT INTO bmsql_order_line (" +
//                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
//                        "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
//                        "  ol_amount, ol_dist_info) " +
//                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
//                        "ON CONFLICT (ol_o_id, ol_d_id, ol_w_id, ol_number) DO UPDATE SET ol_i_id = excluded.ol_i_id"
//        );
        stmtInsertUpdateOrderLine = dbConn.prepareStatement(
                "INSERT INTO bmsql_order_line (" +
                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                        "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
                        "  ol_amount, ol_dist_info) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (bmsql_o_l,ol_o_id, ol_d_id, ol_w_id, ol_number) DO UPDATE SET ol_i_id = excluded.ol_i_id"
        );

        stmtInsertUpdateBucketOrderLine = dbConn.prepareStatement(
                "INSERT INTO bmsql_order_line@{no_full_scan} (" +
                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                        "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
                        "  ol_amount, ol_dist_info) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (dbms_internal_ol_d_id_ol_number_ol_o_id_ol_w_id_shard_12,ol_o_id, ol_d_id, ol_w_id, ol_number) DO UPDATE SET ol_i_id = excluded.ol_i_id"
        );
        stmtUpsertBucketOrderLine = dbConn.prepareStatement(
                "upsert INTO bmsql_order_line@{no_full_scan} (" +
                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                        "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
                        "  ol_amount, ol_dist_info) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        );
    }

    /*
     * run()
     */
    public  String Generatestr(int count) {
        // 假设 tuples 是一个包含你的元组的 List
//        List<Integer[]> tuples = Arrays.asList(new Integer[]{1,1,1}, new Integer[]{2,2,2}, new Integer[]{3,3,3});

        StringBuilder queryBuilder = new StringBuilder("DELETE FROM  etm_cnaps_prep WHERE (telseqno, dttran, bankno) IN (");
        if (this.hint.equals("hint")){
            queryBuilder = new StringBuilder("DELETE FROM  etm_cnaps_prep@{no_full_scan} WHERE (telseqno, dttran, bankno) IN (");
        }
        StringJoiner joiner = new StringJoiner(",");

        for (int i = 1; i <= count; i++) {
            joiner.add("(?,?,?)");
        }

        queryBuilder.append(joiner.toString());
        queryBuilder.append(")");

        String query = queryBuilder.toString();
        return query;
    }

    public void run() {
        int job;

        try {
            while ((job = Main.getNextJob()) >= 0) {
                if (job == 0) {
                    continue;
                }
                if (flag.toLowerCase().equals("insert")) {
                    fmt.format("Worker %03d: Loading Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    loadWarehouse(job);
                    fmt.format("Worker %03d: Loading Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                } else if (flag.toLowerCase().equals("update")) {
                    fmt.format("Worker %03d: Updating Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    Update(job);
                    fmt.format("Worker %03d: Updating Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                } else if (flag.toLowerCase().equals("update_batch")) {
                    fmt.format("Worker %03d: Updating Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    Update_batch(job,commitbatch);
                    fmt.format("Worker %03d: Updating Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("delete_batch")) {
                    fmt.format("Worker %03d: deleting Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    delete_batch(job,commitbatch);
                    fmt.format("Worker %03d: deleting Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("insert_update")) {
                    fmt.format("Worker %03d: Insert_updating Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    InsertUpdate(job,commitbatch);
                    fmt.format("Worker %03d: Insert_updating Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("insert_update_bucket")) {
                    fmt.format("Worker %03d: Insert_updating 1234567 Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    InsertUpdateBucket(job,commitbatch);
                    fmt.format("Worker %03d: Insert_updating 1234567 Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("update_batch_value")) {
                    fmt.format("Worker %03d: Insert_updating 1234 Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    update_batch_value(job, commitbatch);
                    fmt.format("Worker %03d: Insert_updating 1234 Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("delete_batch_value")) {
                    fmt.format("Worker %03d: Delete_valuing Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    delete_batch_value(job, commitbatch);
                    fmt.format("Worker %03d: Delete_valuing Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("insert_batch_value")) {
                    fmt.format("Worker %03d: Inserting Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    insert_batch_value(job, commitbatch);
                    fmt.format("Worker %03d: Inserting Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("upsert")) {
                    fmt.format("Worker %03d: Upserting  12345678 Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    UpsertWarehouse(job, commitbatch);
                    fmt.format("Worker %03d: Upserting 12345678 Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("upsert_batch_value")) {
                    fmt.format("Worker %03d: Upserting batch value 123 Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    upsert_batch_value(job, commitbatch);
                    fmt.format("Worker %03d: Upserting batch value 123 Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("update_batch_value_list")) {
                    fmt.format("Worker %03d: Upserting batch value list 1234 Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    update_batch_value_list(job, commitbatch);
                    fmt.format("Worker %03d: Upserting batch value list 1234 Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("insert_return")) {
                    fmt.format("Worker %03d: Insert return Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    insert_return(job,commitbatch);
                    fmt.format("Worker %03d: Insert return Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("update_batch_unnest")) {
                    fmt.format("Worker %03d: Updating unnest Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    Update_batch_unnest(job,commitbatch);
                    fmt.format("Worker %03d: Updating unnest Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("delete_batch_unnest")) {
                    fmt.format("Worker %03d: Deleting unnest Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    Delete_batch_unnest(job,commitbatch);
                    fmt.format("Worker %03d: Deleting unnest Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("update_batch_value_rate")) {
                    fmt.format("Worker %03d: Insert_updating 1234 Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    update_batch_value_rate(job, commitbatch);
                    fmt.format("Worker %03d: Insert_updating 1234 Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("update_batch_rate")) {
                    fmt.format("Worker %03d: Insert_updating 1234 Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    Update_batch_rate(job, commitbatch);
                    fmt.format("Worker %03d: Insert_updating 1234 Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("insert_only")) {
                    fmt.format("Worker %03d: Loading Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    loadWarehouse_only(job);
                    fmt.format("Worker %03d: Loading Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("insert_infra")) {
                    fmt.format("Worker %03d: etm_cnaps_prep Loading Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    loadWarehouse_infra(job);
                    fmt.format("Worker %03d: etm_cnaps_prep Loading Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("delete_batch_value_infra")) {
                    fmt.format("Worker %03d: etm_cnaps_prep Deleting Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    delete_batch_value_infra(job,commitbatch);
                    fmt.format("Worker %03d: etm_cnaps_prep Deleted Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("insert_infra_only")) {
                    fmt.format("Worker %03d: etm_cnaps_prep Loading Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    load_infra(job,connrows,commitbatch);
                    fmt.format("Worker %03d: etm_cnaps_prep Loading Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("delete_value_infra_only")) {
                    fmt.format("Worker %03d: etm_cnaps_prep Deleting Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    delete_value_infra(job,connrows,commitbatch);
                    fmt.format("Worker %03d: etm_cnaps_prep Deleted Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("delete_value_infra_prep")) {
                    fmt.format("Worker %03d: etm_cnaps_prep prepare Deleting Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    delete_value_infra_prep(job,connrows,commitbatch);
                    fmt.format("Worker %03d: etm_cnaps_prep prepare Deleted Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("delete_value_infra_prep_tmp")) {
                    fmt.format("Worker %03d: etm_cnaps_prep prepare Deleting Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    delete_value_infra_prep_tmp(job,connrows,commitbatch);
                    fmt.format("Worker %03d: etm_cnaps_prep prepare Deleted Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("delete_value_infra_prep_2")) {
                    fmt.format("Worker %03d: etm_cnaps_prep prepare Deleting Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    delete_value_infra_prep_2(job,connrows,commitbatch);
                    fmt.format("Worker %03d: etm_cnaps_prep prepare Deleted Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else{
                    printHelp();
                }
            }

            /*
             * Close the DB connection if in direct DB mode.
             */
            if (!writeCSV)
                dbConn.close();
        } catch (SQLException se) {
            while (se != null) {
                fmt.format("Worker %03d: ERROR: %s", worker, se.getMessage());
                System.err.println(sb.toString());
                sb.setLength(0);
                se = se.getNextException();
            }
        } catch (Exception e) {
            fmt.format("Worker %03d: ERROR: %s", worker, e.getMessage());
            System.err.println(sb.toString());
            sb.setLength(0);
            e.printStackTrace();
            return;
        }
    } // End run()
    public static void printHelp() {
        System.out.println("没有匹配到对应的逻辑");
        System.out.println("帮助信息：");
        System.out.println("    - 使用说明: 这个程序用于演示如何打印帮助信息。");
        System.out.println("    - 编译示例:javac -Djava.ext.dirs=./lib Main.java");
        System.out.println("    - 示例:java -cp .:./lib/*  Main insert_infra_only 200000 10");
        System.out.println("    - 示例:java -cp .:./lib/*  Main delete_value_infra_only 200000 10");
        System.out.println("    - 示例:当前的路径下:  Main.java的源码文件" +
                                                  "要包含lib目录:放置jdbc包"+
                                                  "props.qb配置文件:默认配置如下\n"+
                                                  "\t\tdb=qianbase 不需要修改\n"+
                                                  "\t\tdriver=org.qianbase.Driver 一般不需要修改\n"+
                                                  "\t\tconn=jdbc:qianbase://{ip}:{port}/{dbname}?reWriteBatchedInserts=true&&sslmode=disable&targetServerType=any&loadBalanceHosts=true&&loadBalanceHostsExtend=true 根据需要修改\n"+
                                                  "\t\tuser=qbadmin 根据需要修改\n"+
                                                  "\t\tpassword=qianbase 根据需要修改\n"+
                                                  "\t\thint=hint 值为hint则走hint逻辑tbname@{no_full_scan}\n"+
                                                  "\t\twarehouse=800 和 loadWorker=800 根据需要修改且满足warehouse >=loadWorkder: 执行加载(insert)和删除(delete)的并发数\n"
                                                    );
        System.out.println("    - 参数1: 执行的类别。");
        System.out.println("    - 参数2: 执行数据表的总行数(加载或者删除)。");
        System.out.println("    - 参数3: 执行批量的行数。");
        System.out.println("    - 参数1: 执行的类别的介绍。");
        System.out.println("    - 参数1: delete_value_infra_prep_2 : "+
                                        "prepare的方式的删除数据,sql 样式"+
                                        "DELETE FROM  etm_cnaps_prep@{no_full_scan} "+
                                        " WHERE (telseqno, dttran, bankno) IN (('CCCCCCCCCCCCCCCCCC00000011774','yzabcdef','mng'),('DDDDDDDDDDDDDDDDDD00000012774','ghijklmn','xyz'))"
                                  );
        System.out.println("    - 参数1: delete_value_infra_prep_tmp :"+
                                        "prepare的方式的删除数据,sql 样式"+
                                        "delete from etm_cnaps_prep "+"" +
                                        "where (telseqno, dttran, bankno) IN (select unnest(array['FFFFFFFFFFFFFFFFFF00000014593','I3222222222222222200000017294','DDDDDDDDDDDDDDDDDD00000012315']),"+
                                        "unnest(array['wxyzabcd','ijklmnop','ghijklmn']),unnest(array['mng','mng','xyz']));");
        System.out.println("    - 参数1: delete_value_infra_prep: "+
                                        "prepare的方式的删除数据,sql 样式"+
                                        "delete from etm_cnaps_prep@{no_full_scan} join "+
                                        "using (select unnest(array['FFFFFFFFFFFFFFFFFF00000014593','I3222222222222222200000017294','DDDDDDDDDDDDDDDDDD00000012315']),"+
                                        "unnest(array['wxyzabcd','ijklmnop','ghijklmn']),unnest(array['mng','mng','xy','xyz'])) " +
                                        " data(d1,d2,d3) where  etm_cnaps_prep.telseqno = data.d1 and etm_cnaps_prep.dttran = data.d2 and etm_cnaps_prep.bankno = data.d3;");
        System.out.println("    - 参数1: delete_value_infra_only statement方式删除数据: 。");
        System.out.println("    - 参数1: insert_infra_only: prepare方式加载数据。");
        System.out.println("    - 对应的建表语句"+
                "create table etm_cnaps_prep ( "+
                "bankno varchar(3) not null,"+
                "pbocseqno varchar(60) null,"+
                "msgsno varchar(32) null,"+
                "dttran varchar(8) not null,"+
                "telseqno varchar(32) not null,"+
                "flginto varchar(1) null,"+
                "dtfront varchar(8) null,"+
                "frontseqno varchar(32) null,"+
                "bakfld1 varchar(200) null,"+
                "bakfld2 varchar(200) null,"+
                "bakfld3 varchar(200) null,"+
                "bakamt decimal(26,8) null,"+
                "updtms varchar(26) null,"+
                "modteller varchar(8) null,"+
                "modbrc varchar(10) null,"+
                "moddate varchar(8) null,"+
                "stsrcd varchar(2) null,"+
                "crttime timestamp null,"+
                "updtime timestamp null,"+
                "version decimal null,"+
                "primary key(telseqno asc,dttran asc,bankno asc),"+
                "index etm_cnaps_prep_3_idx03(pbocseqno asc,bankno asc),"+
                "index etm_cnaps_prep_2_idx02(msgsno asc,dttran asc,bankno asc),"+
                "index etm_cnaps_prep_4_idx04(frontseqno asc,dtfront asc,bankno asc),"+
                "index etm_cnaps_prep_9_idx9 (moddate asc,modbrc asc));"
                );
        System.out.println("没有匹配到对应的逻辑");
        System.exit(1);
        // 可以继续添加更多的帮助信息
    }
    /* ----
     * loadorderline()
     *
     * Load the content of the order_line table.
     * ----
     */
    private void Update(int w_id)
            throws SQLException, IOException {
        for (int o_id = 1; o_id <= 3000; o_id++) {
//            int o_ol_cnt = r
//            nd.nextInt(5, 15);
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                stmtUpdateOrderLine.setInt(1, rnd.nextInt(1, 100000));
                stmtUpdateOrderLine.setInt(2, o_id);
                stmtUpdateOrderLine.setInt(3, w_id + 10);
                stmtUpdateOrderLine.setInt(4, w_id);
                stmtUpdateOrderLine.setInt(5, ol_number);
                stmtUpdateOrderLine.execute();
                dbConn.commit();
            }
        }
    }
    private void Update_batch(int w_id,int commitbatch)
            throws SQLException, IOException {
        int i=0;
        for (int o_id = 1; o_id <= 3000; o_id++) {
//            int o_ol_cnt = rnd.nextInt(5, 15);
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                stmtUpdateOrderLine.setInt(1, rnd.nextInt(1, 100000));
                stmtUpdateOrderLine.setInt(2, o_id);
                stmtUpdateOrderLine.setInt(3, w_id + 10);
                stmtUpdateOrderLine.setInt(4, w_id);
                stmtUpdateOrderLine.setInt(5, ol_number);
//                System.out.println(stmtUpdateOrderLine);
                stmtUpdateOrderLine.addBatch();
                i = i + 1;
                if (i%commitbatch == 0){
                    stmtUpdateOrderLine.executeBatch();
                    stmtUpdateOrderLine.clearBatch();
                    dbConn.commit();
//                    System.out.println(i);
                }
            }

        }
        stmtUpdateOrderLine.executeBatch();
        stmtUpdateOrderLine.clearBatch();
        dbConn.commit();
    }
    private void Update_batch_rate(int w_id,int commitbatch)
            throws SQLException, IOException {
        int i=0;
        for (int o_id = 1; o_id <= 2000; o_id++) {
//            int o_ol_cnt = rnd.nextInt(5, 15);
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                stmtUpdateOrderLine.setInt(1, rnd.nextInt(1, 100000));
                stmtUpdateOrderLine.setInt(2, o_id);
                stmtUpdateOrderLine.setInt(3, w_id + 10);
                stmtUpdateOrderLine.setInt(4, w_id);
                stmtUpdateOrderLine.setInt(5, ol_number);
//                System.out.println(stmtUpdateOrderLine);
                stmtUpdateOrderLine.addBatch();
                i = i + 1;
                if (i%commitbatch == 0){
                    stmtUpdateOrderLine.executeBatch();
                    stmtUpdateOrderLine.clearBatch();
                    dbConn.commit();
//                    System.out.println(i);
                }
            }

        }
        stmtUpdateOrderLine.executeBatch();
        stmtUpdateOrderLine.clearBatch();
        dbConn.commit();
    }

    private void Update_batch_unnest(int w_id,int commitbatch)
            throws SQLException, IOException {
        int i=0;
        ArrayList<Object> d1 = new ArrayList<Object>();
        ArrayList<Object> d2 = new ArrayList<Object>();
        ArrayList<Object> d3 = new ArrayList<Object>();
        ArrayList<Object> d4 = new ArrayList<Object>();
        ArrayList<Object> d5 = new ArrayList<Object>();
        for (int o_id = 1; o_id <= 3000; o_id++) {
//            int o_ol_cnt = rnd.nextInt(5, 15);
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
//                stmtUpdateOrderLine.setInt(1, rnd.nextInt(1, 100000));
//                stmtUpdateOrderLine.setInt(2, o_id);
//                stmtUpdateOrderLine.setInt(3, w_id + 10);
//                stmtUpdateOrderLine.setInt(4, w_id);
//                stmtUpdateOrderLine.setInt(5, ol_number);
//                stmtUpdateOrderLine.addBatch();
                  d1.add(rnd.nextInt(1,100000));
                  d2.add(o_id);
                  d3.add(w_id+10);
                  d4.add(w_id);
                  d5.add(ol_number);

                i = i + 1;
                if (i%commitbatch == 0){
                    Integer[] array1 = d1.toArray(new Integer[0]);
                    Integer[] array2 = d2.toArray(new Integer[0]);
                    Integer[] array3 = d3.toArray(new Integer[0]);
                    Integer[] array4 = d4.toArray(new Integer[0]);
                    Integer[] array5 = d5.toArray(new Integer[0]);
                    stmtUpdateOrderLineforunnest.setArray(1,dbConn.createArrayOf("Integer",array1));
                    stmtUpdateOrderLineforunnest.setArray(2,dbConn.createArrayOf("Integer",array2));
                    stmtUpdateOrderLineforunnest.setArray(3,dbConn.createArrayOf("Integer",array3));
                    stmtUpdateOrderLineforunnest.setArray(4,dbConn.createArrayOf("Integer",array4));
                    stmtUpdateOrderLineforunnest.setArray(5,dbConn.createArrayOf("Integer",array5));
//                    System.out.println(stmtUpdateOrderLineforunnest);
                    stmtUpdateOrderLineforunnest.execute();
                    dbConn.commit();
                    d1.clear();
                    d2.clear();
                    d3.clear();
                    d4.clear();
                    d5.clear();
//                    System.out.println(i);
                }
            }

        }
//        Array array1 = dbConn.createArrayOf("Integer",d1.toArray());
//        Array array2 = dbConn.createArrayOf("Integer",d2.toArray());
//        Array array3 = dbConn.createArrayOf("Integer",d3.toArray());
//        Array array4 = dbConn.createArrayOf("Integer",d4.toArray());
//        Array array5 = dbConn.createArrayOf("Integer",d5.toArray());
        stmtUpdateOrderLineforunnest.setArray(1,dbConn.createArrayOf("Integer",d1.toArray()));
        stmtUpdateOrderLineforunnest.setArray(2,dbConn.createArrayOf("Integer",d2.toArray()));
        stmtUpdateOrderLineforunnest.setArray(3,dbConn.createArrayOf("Integer",d3.toArray()));
        stmtUpdateOrderLineforunnest.setArray(4,dbConn.createArrayOf("Integer",d4.toArray()));
        stmtUpdateOrderLineforunnest.setArray(5,dbConn.createArrayOf("Integer",d5.toArray()));
        dbConn.commit();
        d1.clear();
        d2.clear();
        d3.clear();
        d4.clear();
        d5.clear();
    }

    private void Delete_batch_unnest(int w_id,int commitbatch)
            throws SQLException, IOException {
        int i=0;
        ArrayList<Object> d2 = new ArrayList<Object>();
        ArrayList<Object> d3 = new ArrayList<Object>();
        ArrayList<Object> d4 = new ArrayList<Object>();
        ArrayList<Object> d5 = new ArrayList<Object>();
        for (int o_id = 1; o_id <= 3000; o_id++) {
//            int o_ol_cnt = rnd.nextInt(5, 15);
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                d2.add(o_id);
                d3.add(w_id+10);
                d4.add(w_id);
                d5.add(ol_number);

                i = i + 1;
                if (i%commitbatch == 0){
                    Integer[] array2 = d2.toArray(new Integer[0]);
                    Integer[] array3 = d3.toArray(new Integer[0]);
                    Integer[] array4 = d4.toArray(new Integer[0]);
                    Integer[] array5 = d5.toArray(new Integer[0]);
                    stmtDeleteOrderLineforunnest.setArray(1,dbConn.createArrayOf("Integer",array2));
                    stmtDeleteOrderLineforunnest.setArray(2,dbConn.createArrayOf("Integer",array3));
                    stmtDeleteOrderLineforunnest.setArray(3,dbConn.createArrayOf("Integer",array4));
                    stmtDeleteOrderLineforunnest.setArray(4,dbConn.createArrayOf("Integer",array5));
                    stmtDeleteOrderLineforunnest.execute();
                    dbConn.commit();
                    d2.clear();
                    d3.clear();
                    d4.clear();
                    d5.clear();
//                    System.out.println(i);
                }
            }

        }

        stmtDeleteOrderLineforunnest.setArray(1,dbConn.createArrayOf("Integer",d2.toArray()));
        stmtDeleteOrderLineforunnest.setArray(2,dbConn.createArrayOf("Integer",d3.toArray()));
        stmtDeleteOrderLineforunnest.setArray(3,dbConn.createArrayOf("Integer",d4.toArray()));
        stmtDeleteOrderLineforunnest.setArray(4,dbConn.createArrayOf("Integer",d5.toArray()));
        dbConn.commit();
        d2.clear();
        d3.clear();
        d4.clear();
        d5.clear();
    }
    private void InsertUpdate(int w_id,int commitbatch)
            throws SQLException, IOException {
        int i=0;
        for (int o_id = 1; o_id <= 3000; o_id++) {
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                long now = System.currentTimeMillis();
                stmtInsertUpdateOrderLine.setInt(1, o_id);
                stmtInsertUpdateOrderLine.setInt(2, w_id + 10);
                stmtInsertUpdateOrderLine.setInt(3, w_id);
                stmtInsertUpdateOrderLine.setInt(4, ol_number);
//                stmtInsertUpdateOrderLine.setInt(5, rnd.nextInt(1, 100000));
                stmtInsertUpdateOrderLine.setInt(5, 12345);
                stmtInsertUpdateOrderLine.setInt(6, w_id);
                if (o_id < 2101)
                    stmtInsertUpdateOrderLine.setTimestamp(7, new java.sql.Timestamp(now));
                else
                    stmtInsertUpdateBucketOrderLine.setNull(7, java.sql.Types.TIMESTAMP);
                stmtInsertUpdateOrderLine.setInt(8, 5);
                if (o_id < 2101)
                    stmtInsertUpdateOrderLine.setDouble(9, 0.00);
                else
                    stmtInsertUpdateOrderLine.setDouble(9, ((double) rnd.nextLong(1, 999999)) / 100.0);
                stmtInsertUpdateOrderLine.setString(10, rnd.getAString(24, 24));
//                stmtInsertUpdateOrderLine.setString(11, "12345");
                stmtInsertUpdateOrderLine.addBatch();
                i = i + 1;
                if (i%commitbatch == 0) {
                    stmtInsertUpdateOrderLine.executeBatch();
                    stmtInsertUpdateOrderLine.clearBatch();
//                    System.out.println(stmtInsertUpdateOrderLine);
                    dbConn.commit();
                }
            }
        }
        stmtInsertUpdateOrderLine.executeBatch();
        stmtInsertUpdateOrderLine.clearBatch();
        dbConn.commit();
    }

    private void InsertUpdateBucket(int w_id,int commitbatch)
            throws SQLException, IOException {
        int i=0;
        for (int o_id = 1; o_id <= 3000; o_id++) {
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                long now = System.currentTimeMillis();
                stmtInsertUpdateBucketOrderLine.setInt(1, o_id);
                stmtInsertUpdateBucketOrderLine.setInt(2, w_id + 10);
                stmtInsertUpdateBucketOrderLine.setInt(3, w_id);
                stmtInsertUpdateBucketOrderLine.setInt(4, ol_number);
//                stmtInsertUpdateBucketOrderLine.setInt(5, rnd.nextInt(1, 100000));
                stmtInsertUpdateBucketOrderLine.setInt(5, 1234567);
                stmtInsertUpdateBucketOrderLine.setInt(6, w_id);
                if (o_id < 2101)
                    stmtInsertUpdateBucketOrderLine.setTimestamp(7, new java.sql.Timestamp(now));
                else
                    stmtInsertUpdateBucketOrderLine.setNull(7, java.sql.Types.TIMESTAMP);
                stmtInsertUpdateBucketOrderLine.setInt(8, 5);
                if (o_id < 2101)
                    stmtInsertUpdateBucketOrderLine.setDouble(9, 0.00);
                else
                    stmtInsertUpdateBucketOrderLine.setDouble(9, ((double) rnd.nextLong(1, 999999)) / 100.0);
                stmtInsertUpdateBucketOrderLine.setString(10, rnd.getAString(24, 24));
//                stmtInsertUpdateBucketOrderLine.setString(11, "123456");
                stmtInsertUpdateBucketOrderLine.addBatch();
                i = i + 1;
                if (i%commitbatch == 0) {
                    stmtInsertUpdateBucketOrderLine.executeBatch();
                    stmtInsertUpdateBucketOrderLine.clearBatch();
//                    System.out.println(stmtInsertUpdateOrderLine);
                    dbConn.commit();
                }
            }
        }
        stmtInsertUpdateBucketOrderLine.executeBatch();
        stmtInsertUpdateBucketOrderLine.clearBatch();
        dbConn.commit();
    }
    private void delete_batch(int w_id,int commitbatch)
            throws SQLException, IOException {
        int i=0;
        for (int o_id = 1; o_id <= 3000; o_id++) {
//            int o_ol_cnt = rnd.nextInt(5, 15);
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                stmtDeleteOrderLine.setInt(1, o_id);
                stmtDeleteOrderLine.setInt(2, w_id + 10);
                stmtDeleteOrderLine.setInt(3, w_id);
                stmtDeleteOrderLine.setInt(4, ol_number);
                stmtDeleteOrderLine.addBatch();
                i = i + 1;
                if (i%commitbatch == 0){
                    stmtDeleteOrderLine.executeBatch();
                    stmtDeleteOrderLine.clearBatch();
                    dbConn.commit();
//                    System.out.println(i);
                }
            }

        }
        stmtDeleteOrderLine.executeBatch();
        stmtDeleteOrderLine.clearBatch();
        dbConn.commit();
    }

    private void update_batch_value(int w_id,int commitbatch)
            throws SQLException, IOException {
        int i=0;
        String str1 = "update bmsql_order_line@{no_full_scan} " +
                "  set ol_i_id = 1234 " +
                "  from (values ";
        String str2 = "  ) as data(d1,d2,d3,d4) where ol_o_id =data.d1 and  ol_d_id = data.d2 and " +
                "ol_w_id = data.d3 and ol_number =data.d4";
        String strtmp = "";
        Statement st = dbConn.createStatement();
        for (int o_id = 1; o_id <= 3000; o_id++) {
//            int o_ol_cnt = rnd.nextInt(5, 15);
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                strtmp = strtmp +"(" +o_id + "," + (w_id +10) + "," + w_id + "," + ol_number +")";
                i = i + 1;
                if (i%commitbatch == 0){
                    strtmp=str1 + strtmp + str2;
//                    System.out.println(strtmp);
                    st.execute(strtmp);
                    dbConn.commit();
                    strtmp = "";
//                    System.out.println(i);
                }
                if (!strtmp.equals("")){
                    strtmp = strtmp + ",";
                }
            }

        }
        if (!strtmp.endsWith("")) {
            if (strtmp.endsWith(",")) {
                strtmp = strtmp.substring(0, strtmp.length() - 1);
            }
            strtmp = str1 + strtmp + str2;
            st.execute(strtmp);
            dbConn.commit();
            strtmp = "";
        }
    }

    private void update_batch_value_rate(int w_id,int commitbatch)
            throws SQLException, IOException {
        int i=0;
        String str1 = "update bmsql_order_line@{no_full_scan} " +
                "  set ol_i_id = 1234 " +
                "  from (values ";
        String str2 = "  ) as data(d1,d2,d3,d4) where ol_o_id =data.d1 and  ol_d_id = data.d2 and " +
                "ol_w_id = data.d3 and ol_number =data.d4";
        String strtmp = "";
        Statement st = dbConn.createStatement();
        for (int o_id = 1; o_id <= 300; o_id++) {
//            int o_ol_cnt = rnd.nextInt(5, 15);
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                strtmp = strtmp +"(" +o_id + "," + (w_id +10) + "," + w_id + "," + ol_number +")";
                i = i + 1;
                if (i%commitbatch == 0){
                    strtmp=str1 + strtmp + str2;
//                    System.out.println(strtmp);
                    st.execute(strtmp);
                    dbConn.commit();
                    strtmp = "";
//                    System.out.println(i);
                }
                if (!strtmp.equals("")){
                    strtmp = strtmp + ",";
                }
            }

        }
        if (!strtmp.endsWith("")) {
            if (strtmp.endsWith(",")) {
                strtmp = strtmp.substring(0, strtmp.length() - 1);
            }
            strtmp = str1 + strtmp + str2;
            st.execute(strtmp);
            dbConn.commit();
            strtmp = "";
        }
    }
    private void update_batch_value_list(int w_id,int commitbatch)
            throws SQLException, IOException {
        int i=0;
        String str1 = "update bmsql_order_line@{no_full_scan} " +
                "  set ol_i_id = 1 " +
                "  from (values ";
        String str2 = "  ) as data(d0,d1,d2,d3,d4) where bmsql_o_l = data.d0 and  ol_o_id =data.d1 and  ol_d_id = data.d2 and " +
                "ol_w_id = data.d3 and ol_number =data.d4";
        String strtmp = "";
        Statement st = dbConn.createStatement();
        for (int o_id = 1; o_id <= 3000; o_id++) {
//            int o_ol_cnt = rnd.nextInt(5, 15);
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                strtmp = strtmp +"(" + (w_id%12) + "," +o_id + "," + (w_id +10) + "," + w_id + "," + ol_number +")";
                i = i + 1;
                if (i%commitbatch == 0){
                    strtmp=str1 + strtmp + str2;
//                    System.out.println(strtmp);
                    st.execute(strtmp);
                    dbConn.commit();
                    strtmp = "";
//                    System.out.println(i);
                }
                if (!strtmp.equals("")){
                    strtmp = strtmp + ",";
                }
            }

        }
        if (!strtmp.endsWith("")) {
            if (strtmp.endsWith(",")) {
                strtmp = strtmp.substring(0, strtmp.length() - 1);
            }
            strtmp = str1 + strtmp + str2;
            st.execute(strtmp);
            dbConn.commit();
            strtmp = "";
        }
    }

    private void delete_batch_value(int w_id,int commitbatch)
            throws SQLException, IOException {
        int i=0;
        String str1 = "delete from bmsql_order_line@{no_full_scan} " +
                "  where (ol_o_id,ol_d_id,ol_w_id,ol_number) in ( ";
        String strtmp = "";
        Statement st = dbConn.createStatement();
        for (int o_id = 1; o_id <= 3000; o_id++) {
//            int o_ol_cnt = rnd.nextInt(5, 15);
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                strtmp = strtmp +"(" +o_id + "," + (w_id +10) + "," + w_id + "," + ol_number +")";
                i = i + 1;
                if (i%commitbatch == 0){
                    strtmp=str1 + strtmp + ")";
//                    System.out.println(strtmp);
                    st.execute(strtmp);
                    dbConn.commit();
                    strtmp = "";
//                    System.out.println(i);
                }
                if (!strtmp.equals("")){
                    strtmp = strtmp + ",";
                }
            }

        }
        if (!strtmp.endsWith("")) {
            if (strtmp.endsWith(",")) {
                strtmp = strtmp.substring(0, strtmp.length() - 1);
            }
            strtmp = str1 + strtmp + ")";
            st.execute(strtmp);
            dbConn.commit();
            strtmp = "";
        }
    }

    private void insert_batch_value(int w_id,int commitbatch)
            throws SQLException, IOException {
        int i = 0;
        String str1 = "INSERT INTO bmsql_order_line (" +
                "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
                "  ol_amount, ol_dist_info) " +
                "VALUES ";
        String strtmp = "";
        Statement st = dbConn.createStatement();
        for (int o_id = 1; o_id <= 3000; o_id++) {
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                long now = System.currentTimeMillis();
                strtmp = strtmp +"(" + o_id + "," + (w_id+10) + "," + w_id + "," + ol_number + "," + rnd.nextInt(1, 100000) +"," + w_id + ",";

                if (o_id < 2101)
                    strtmp = strtmp +  "'" + (new java.sql.Timestamp(now)) + "'"+ ", ";
                else
                    strtmp = strtmp +   null + ", ";
                strtmp = strtmp + 5 + ", ";
                if (o_id < 2101)
                    strtmp = strtmp + 0.00 + ", ";
                else
                    strtmp = strtmp + ((double) rnd.nextLong(1, 999999)) / 100.0 + ",";
                i = i+1;
                if (i%commitbatch == 0) {
                    strtmp = str1 + strtmp + "'" + rnd.getAString(24, 24) + "')";
//                    System.out.println(strtmp);
                    st.execute(strtmp);
                    dbConn.commit();
                    strtmp = "";
                }else {
                    strtmp = strtmp + "'" + rnd.getAString(24, 24) + "'" + "),";
                }
            }
            if (!strtmp.endsWith("")) {
                if (strtmp.endsWith(",")) {
                    strtmp = strtmp.substring(0, strtmp.length() - 1);
                }
                strtmp = str1 + strtmp;
                st.execute(strtmp);
                dbConn.commit();
                strtmp = "";
            }
        }

        if (!writeCSV) {
            dbConn.commit();
        }
    }

    private void upsert_batch_value(int w_id,int commitbatch)
            throws SQLException, IOException {
        int i = 0;
        String str1 = "UPSERT INTO bmsql_order_line@{no_full_scan} (" +
                "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
                "  ol_amount, ol_dist_info) " +
                "VALUES ";
        String strtmp = "";
        Statement st = dbConn.createStatement();
        for (int o_id = 1; o_id <= 3000; o_id++) {
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                long now = System.currentTimeMillis();
                strtmp = strtmp +"(" + o_id + "," + (w_id+10) + "," + w_id + "," + ol_number + "," + 123 +"," + w_id + ",";

                if (o_id < 2101)
                    strtmp = strtmp +  "'" + (new java.sql.Timestamp(now)) + "'"+ ", ";
                else
                    strtmp = strtmp +   null + ", ";
                strtmp = strtmp + 5 + ", ";
                if (o_id < 2101)
                    strtmp = strtmp + 0.00 + ", ";
                else
                    strtmp = strtmp + ((double) rnd.nextLong(1, 999999)) / 100.0 + ",";
                i = i+1;
                if (i%commitbatch == 0) {
                    strtmp = str1 + strtmp + "'" + rnd.getAString(24, 24) + "')";
//                    System.out.println(strtmp);
                    st.execute(strtmp);
                    dbConn.commit();
                    strtmp = "";
                }else {
                    strtmp = strtmp + "'" + rnd.getAString(24, 24) + "'" + "),";
                }
            }
            if (!strtmp.endsWith("")) {
                if (strtmp.endsWith(",")) {
                    strtmp = strtmp.substring(0, strtmp.length() - 1);
                }
                strtmp = str1 + strtmp;
                st.execute(strtmp);
                dbConn.commit();
                strtmp = "";
            }
        }

        if (!writeCSV) {
            dbConn.commit();
        }
    }

    private void UpsertWarehouse(int w_id,int commitbatch)
            throws SQLException, IOException {
        int i=0;
        for (int o_id = 1; o_id <= 3000; o_id++) {
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                long now = System.currentTimeMillis();

                stmtUpsertBucketOrderLine.setInt(1, o_id);
                stmtUpsertBucketOrderLine.setInt(2, w_id + 10);
                stmtUpsertBucketOrderLine.setInt(3, w_id);
                stmtUpsertBucketOrderLine.setInt(4, ol_number);
                stmtUpsertBucketOrderLine.setInt(5, 12345678);
                stmtUpsertBucketOrderLine.setInt(6, w_id);
                if (o_id < 2101)
                    stmtUpsertBucketOrderLine.setTimestamp(7, new java.sql.Timestamp(now));
                else
                    stmtUpsertBucketOrderLine.setNull(7, java.sql.Types.TIMESTAMP);
                stmtUpsertBucketOrderLine.setInt(8, 5);
                if (o_id < 2101)
                    stmtUpsertBucketOrderLine.setDouble(9, 0.00);
                else
                    stmtUpsertBucketOrderLine.setDouble(9, ((double) rnd.nextLong(1, 999999)) / 100.0);
                stmtUpsertBucketOrderLine.setString(10, rnd.getAString(24, 24));
                stmtUpsertBucketOrderLine.addBatch();
            }
            i=i+1;
            if(i%commitbatch ==0) {
                stmtUpsertBucketOrderLine.executeBatch();
                stmtUpsertBucketOrderLine.clearBatch();
                dbConn.commit();
            }
        }
        dbConn.commit();
        }

    private void insert_return(int w_id,int commitbatch)
            throws SQLException, IOException {
//        dbConn.setAutoCommit(false);
        int i=0;
        Statement st = dbConn.createStatement();
        String str1="INSERT INTO bmsql_order_line (" +
                "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
                "  ol_amount, ol_dist_info) " +
                "VALUES ";
        String str2 =  " returning ol_w_id,ol_d_id,ol_o_id,ol_number";
        String strtmp="";
        for (int o_id = 1; o_id <= 3000; o_id++) {
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                long now = System.currentTimeMillis();
                strtmp = strtmp  +"(" + o_id + "," + (w_id+10) + "," + w_id + "," + ol_number + "," + 123 +"," + w_id + ",";

                if (o_id < 2101)
                    strtmp = strtmp +  "'" + (new java.sql.Timestamp(now)) + "'"+ ", ";
                else
                    strtmp = strtmp +   null + ", ";
                strtmp = strtmp + 5 + ", ";
                if (o_id < 2101)
                    strtmp = strtmp + 0.00 + ", ";
                else
                    strtmp = strtmp + ((double) rnd.nextLong(1, 999999)) / 100.0 + ",";
                i = i+1;
                if (i%commitbatch == 0) {
                    strtmp = str1 + strtmp + "'" + rnd.getAString(24, 24) + "')" + str2;
//                    System.out.println(strtmp);
                    rs = st.executeQuery(strtmp);
                    while (true) {
                        if (rs.next()) {
                            stmtInsertUpdateOrderLine.setInt(3,rs.getInt(1));
                            stmtInsertUpdateOrderLine.setInt(2,rs.getInt(2));
                            stmtInsertUpdateOrderLine.setInt(1,rs.getInt(3));
                            stmtInsertUpdateOrderLine.setInt(4,rs.getInt(4));
                            stmtInsertUpdateOrderLine.setInt(5,1);
                            stmtInsertUpdateOrderLine.setInt(6,java.sql.Types.TIMESTAMP);
                            stmtInsertUpdateOrderLine.setInt(7,Types.INTEGER);
                            stmtInsertUpdateOrderLine.setInt(8,Types.INTEGER);
                            stmtInsertUpdateOrderLine.setInt(9,Types.INTEGER);
                            stmtInsertUpdateOrderLine.setInt(10,'2');
                            stmtInsertUpdateOrderLine.addBatch();
                        }else {
                            stmtInsertUpdateOrderLine.executeBatch();
                            stmtInsertUpdateOrderLine.clearBatch();
                            break;
                        }
                    }
                    strtmp = "";
                    dbConn.commit();

                }else {
                    strtmp = strtmp + "'" + rnd.getAString(24, 24) + "'" + "),";
                    }

            }

                }
//            if(rs.next()){
//            System.out.println(rs.getInt(1));
//            System.out.println(rs.getInt(2));
//            System.out.println(rs.getInt(3));
//            System.out.println(rs.getInt(4));
//            }
            dbConn.commit();
    }




    private void loadWarehouse(int w_id)
            throws SQLException, IOException {
            for (int o_id = 1; o_id <= 3000; o_id++) {
                int o_ol_cnt = 10;

                /*
                 * Create the ORDER_LINE rows for this ORDER.
                 */
                for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                    long now = System.currentTimeMillis();

                    if (writeCSV) {
                        fmtOrderLine.format("%d,%d,%d,%d,%d,%s,%.2f,%d,%d,%s\n",
                                w_id,
                                w_id + 10,
                                o_id,
                                ol_number,
                                rnd.nextInt(1, 100000),
                                (o_id < 2101) ? new java.sql.Timestamp(now).toString() : csvNull,
                                (o_id < 2101) ? 0.00 : ((double) rnd.nextLong(1, 999999)) / 100.0,
                                w_id,
                                5,
                                rnd.getAString(24, 24));
                    } else {
                        stmtOrderLine.setInt(1, o_id);
                        stmtOrderLine.setInt(2, w_id + 10);
                        stmtOrderLine.setInt(3, w_id);
                        stmtOrderLine.setInt(4, ol_number);
                        stmtOrderLine.setInt(5, rnd.nextInt(1, 100000));
                        stmtOrderLine.setInt(6, w_id);
                        if (o_id < 2101)
                            stmtOrderLine.setTimestamp(7, new java.sql.Timestamp(now));
                        else
                            stmtOrderLine.setNull(7, java.sql.Types.TIMESTAMP);
                        stmtOrderLine.setInt(8, 5);
                        if (o_id < 2101)
                            stmtOrderLine.setDouble(9, 0.00);
                        else
                            stmtOrderLine.setDouble(9, ((double) rnd.nextLong(1, 999999)) / 100.0);
                        stmtOrderLine.setString(10, rnd.getAString(24, 24));

                        stmtOrderLine.addBatch();
                    }
                }
                if (writeCSV) {
                    Main.orderLineAppend(sbOrderLine);
                } else {
                    stmtOrderLine.executeBatch();
                    stmtOrderLine.clearBatch();
                }
                if (!writeCSV)
                    dbConn.commit();
            }

            if (!writeCSV) {
                dbConn.commit();
            }
    }

    private void loadWarehouse_only(int w_id)
            throws SQLException, IOException {
        for (int o_id = 1; o_id <= 3000; o_id++) {
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                long now = System.currentTimeMillis();

                if (writeCSV) {
                    fmtOrderLine.format("%d,%d,%d,%d,%d,%s,%.2f,%d,%d,%s\n",
                            w_id,
                            w_id + 10,
                            o_id,
                            ol_number,
                            rnd.nextInt(1, 100000),
                            (o_id < 2101) ? new java.sql.Timestamp(now).toString() : csvNull,
                            (o_id < 2101) ? 0.00 : ((double) rnd.nextLong(1, 999999)) / 100.0,
                            w_id,
                            5,
                            rnd.getAString(24, 24));
                } else {
                    stmtOrderLineOnly.setInt(1, o_id);
                    stmtOrderLineOnly.setInt(2, w_id + 10);
                    stmtOrderLineOnly.setInt(3, w_id);
                    stmtOrderLineOnly.setInt(4, ol_number);
                    stmtOrderLineOnly.setInt(5, rnd.nextInt(1, 100000));
                    stmtOrderLineOnly.setInt(6, w_id);
                    if (o_id < 2101)
                        stmtOrderLineOnly.setTimestamp(7, new java.sql.Timestamp(now));
                    else
                        stmtOrderLineOnly.setNull(7, java.sql.Types.TIMESTAMP);
                    stmtOrderLineOnly.setTimestamp(8, new java.sql.Timestamp(now));
                    stmtOrderLineOnly.setInt(9, 5);
                    if (o_id < 2101)
                        stmtOrderLineOnly.setDouble(10, 0.00);
                    else
                        stmtOrderLineOnly.setDouble(10, ((double) rnd.nextLong(1, 999999)) / 100.0);
                    stmtOrderLineOnly.setString(11, rnd.getAString(24, 24));

                    stmtOrderLineOnly.addBatch();
                }
            }
            if (writeCSV) {
                Main.orderLineAppend(sbOrderLine);
            } else {
                stmtOrderLineOnly.executeBatch();
                stmtOrderLineOnly.clearBatch();
            }
            if (!writeCSV)
                dbConn.commit();
        }

        if (!writeCSV) {
            dbConn.commit();
        }
    }
    public static String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder();
        Random random = new Random();

        for (int i = 0; i < length; i++) {
            int index = random.nextInt(characters.length());
            sb.append(characters.charAt(index));
        }

        return sb.toString();
    }

    public static String getStringByModul(int number) {
        switch (number % 3) {
            case 0:
                return "xyz";
            case 1:
                return "ABC";
            case 2:
                return "mng";
            default:
                // 可以选择抛出异常或返回null，这取决于你的需求
                throw new IllegalArgumentException("Unexpected modulo result");
        }
    }

    public static String getStringByModuloEight(int number) {
        switch (number % 8) {
            case 0:
                return "abcdefgh";
            case 1:
                return "ijklmnop";
            case 2:
                return "qrstuvwx";
            case 3:
                return "yzabcdef";
            case 4:
                return "ghijklmn";
            case 5:
                return "opqrstuv";
            case 6:
                return "wxyzabcd";
            case 7:
                return "efghijkl";
            default:
                // 由于number % 8的结果只可能是0到7，因此默认情况不会发生
                throw new IllegalStateException("Unexpected modulo result");
        }
    }
    public static String getStringByModul32(int number) {
        int index = Math.abs(number % 32); // 确保即使是负数也能正确工作
        return STRINGS[index];
    }


    public static String padRight(String originalString, int numberToPad) {
        while (originalString.length() < 8) {
            originalString = numberToPad+originalString;
        }
        return originalString;
    }
    public static String padRight2(String originalString, int numberToPad) {
        while (originalString.length() < 5) {
            originalString = numberToPad+originalString;
        }
        return originalString;
    }



    private void loadWarehouse_infra(int w_id)
            throws SQLException, IOException {
        int m =0;
        for (int o_id = 1; o_id <= 3000; o_id++) {
            int o_ol_cnt = 10;

            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                long now = System.currentTimeMillis();
//                System.out.println(getStringByModul(o_id * ol_number) + getStringByModuloEight(o_id * ol_number) +getStringByModul32(o_id * ol_number)+padRight(padRight(Integer.toString(m),0)+Integer.toString(w_id),9));

                // primary bankno  3
                stmtOrderLineinfra.setString(1, getStringByModul(o_id * ol_number));
                stmtOrderLineinfra.setString(2, generateRandomString(60));
                stmtOrderLineinfra.setString(3, generateRandomString(32));
                // primary dttran 2
                stmtOrderLineinfra.setString(4, getStringByModuloEight(o_id * ol_number));
                // primary telseqno 1
                stmtOrderLineinfra.setString(5, getStringByModul32(o_id * ol_number)+padRight(padRight(Integer.toString(m),0)+Integer.toString(w_id),9));
                stmtOrderLineinfra.setString(6, generateRandomString(1));
                stmtOrderLineinfra.setString(7, generateRandomString(8));
                stmtOrderLineinfra.setString(8, generateRandomString(32));
                stmtOrderLineinfra.setString(9, generateRandomString(200));
                stmtOrderLineinfra.setString(10, generateRandomString(200));
                stmtOrderLineinfra.setString(11, generateRandomString(200));
                stmtOrderLineinfra.setDouble(12,((double) rnd.nextLong(1, 999999)) / 100.0);
                stmtOrderLineinfra.setString(13, generateRandomString(26));
                stmtOrderLineinfra.setString(14, generateRandomString(8));
                stmtOrderLineinfra.setString(15, generateRandomString(10));
                stmtOrderLineinfra.setString(16, generateRandomString(8));
                stmtOrderLineinfra.setString(17, generateRandomString(2));
                stmtOrderLineinfra.setTimestamp(18, new java.sql.Timestamp(now));
                stmtOrderLineinfra.setTimestamp(19, new java.sql.Timestamp(now));
                stmtOrderLineinfra.setDouble(20,((double) rnd.nextLong(1, 999999)) / 100.0);
                m=m+1;
                stmtOrderLineinfra.addBatch();
            }
            if (writeCSV) {
                Main.orderLineAppend(sbOrderLine);
            } else {
                stmtOrderLineinfra.executeBatch();
                stmtOrderLineinfra.clearBatch();
            }
            if (!writeCSV)
                dbConn.commit();
        }

        if (!writeCSV) {
            dbConn.commit();
        }
    }

    private void delete_batch_value_infra(int w_id, int commitbatch)
            throws SQLException, IOException {
        int m = 0;
        StringBuilder strBuilder = new StringBuilder();
        String str1 = "delete from etm_cnaps_prep@{no_full_scan} " +
                "  where (telseqno,dttran,bankno) in ( ";
        strBuilder.append(str1);
        Statement st = dbConn.createStatement();

        for (int o_id = 1; o_id <= 3000; o_id++) {
            int o_ol_cnt = 10; // 似乎是固定的
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
//                if (ol_number > 1 || o_id > 1) {
//                    strBuilder.append(","); // 只有在第一次迭代之后才添加逗号
//                }
                String values = String.format("('%s','%s','%s')",
                        getStringByModul32(o_id * ol_number) + padRight(padRight(Integer.toString(m), 0) + Integer.toString(w_id), 9),
                        getStringByModuloEight(o_id * ol_number),
                        getStringByModul(o_id * ol_number));
                strBuilder.append(values);
                m++;

                if (m % commitbatch == 0) {
                    strBuilder.append(")");
                    st.execute(strBuilder.toString());
                    dbConn.commit();
                    strBuilder.setLength(0);
                    strBuilder.append(str1);
                }else {
                    if (m != 30000 ) {
                        strBuilder.append(",");
                    }
                }
            }


        }
        // 处理剩余的部分
        if (strBuilder.length() > str1.length()) {
            strBuilder.append(")");
            st.execute(strBuilder.toString());
            dbConn.commit();
        }
    }


//    private void delete_batch_value_infra(int w_id,int commitbatch)
//            throws SQLException, IOException {
//        int i=0;
//        int m = 0;
//        String str1 = "delete from etm_cnaps_prep@{no_full_scan} " +
//                "  where (telseqno,dttran,bankno) in ( ";
//        String strtmp = "";
//        Statement st = dbConn.createStatement();
//        for (int o_id = 1; o_id <= 3000; o_id++) {
////            int o_ol_cnt = rnd.nextInt(5, 15);
//            int o_ol_cnt = 10;
//
//            /*
//             * Create the ORDER_LINE rows for this ORDER.
//             */
//            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
//                strtmp = strtmp +"(" +"'"+ getStringByModul32(o_id * ol_number)+padRight(padRight(Integer.toString(m),0)+Integer.toString(w_id),9) + "'"+"," +"'"+ getStringByModuloEight(o_id * ol_number) + "'"+","  +"'"+ getStringByModul(o_id * ol_number) +"'"+")";
//                m =m+1;
//                i = i + 1;
//                if (i%commitbatch == 0){
//                    strtmp=str1 + strtmp + ")";
////                    System.out.println(strtmp);
//                    st.execute(strtmp);
//                    dbConn.commit();
//                    strtmp = "";
////                    System.out.println(i);
//                }
//                if (!strtmp.equals("")){
//                    strtmp = strtmp + ",";
//                }
//            }
//
//        }
//        if (!strtmp.endsWith("")) {
//            if (strtmp.endsWith(",")) {
//                strtmp = strtmp.substring(0, strtmp.length() - 1);
//            }
//            strtmp = str1 + strtmp + ")";
//            st.execute(strtmp);
//            dbConn.commit();
//            strtmp = "";
//        }
//    }
    private void load_infra(int w_id,int connrows,int commitbatch)
            throws SQLException, IOException {
        for (int o_id = 1; o_id <= connrows; o_id++) {
            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
                long now = System.currentTimeMillis();

                // primary bankno  3
                stmtOrderLineinfra.setString(1, getStringByModul(o_id));
                stmtOrderLineinfra.setString(2, generateRandomString(60));
                stmtOrderLineinfra.setString(3, generateRandomString(32));
                // primary dttran 2
                stmtOrderLineinfra.setString(4, getStringByModuloEight(o_id));
                // primary telseqno 1
                stmtOrderLineinfra.setString(5, getStringByModul32(o_id)+padRight(padRight(Integer.toString(o_id),0)+Integer.toString(w_id),9));
                stmtOrderLineinfra.setString(6, generateRandomString(1));
                stmtOrderLineinfra.setString(7, generateRandomString(8));
                stmtOrderLineinfra.setString(8, generateRandomString(32));
                stmtOrderLineinfra.setString(9, generateRandomString(200));
                stmtOrderLineinfra.setString(10, generateRandomString(200));
                stmtOrderLineinfra.setString(11, generateRandomString(200));
                stmtOrderLineinfra.setDouble(12,((double) rnd.nextLong(1, 999999)) / 100.0);
                stmtOrderLineinfra.setString(13, generateRandomString(26));
                stmtOrderLineinfra.setString(14, generateRandomString(8));
                stmtOrderLineinfra.setString(15, generateRandomString(10));
                stmtOrderLineinfra.setString(16, generateRandomString(8));
                stmtOrderLineinfra.setString(17, generateRandomString(2));
                stmtOrderLineinfra.setTimestamp(18, new java.sql.Timestamp(now));
                stmtOrderLineinfra.setTimestamp(19, new java.sql.Timestamp(now));
                stmtOrderLineinfra.setDouble(20,((double) rnd.nextLong(1, 999999)) / 100.0);
                stmtOrderLineinfra.addBatch();
            if (o_id % commitbatch == 0) {
                stmtOrderLineinfra.executeBatch();
                stmtOrderLineinfra.clearBatch();
                dbConn.commit();
            }
        }
        stmtOrderLineinfra.executeBatch();
        stmtOrderLineinfra.clearBatch();
        dbConn.commit();
    }

    private void delete_value_infra(int w_id,int connrows, int commitbatch)
            throws SQLException, IOException {
        StringBuilder strBuilder = new StringBuilder();
        String str1 = "delete from etm_cnaps_prep " +
                "  where (telseqno,dttran,bankno) in ( ";
        if (this.hint.equals("hint")){
            str1 = "delete from etm_cnaps_prep@{no_full_scan} " +
                    "  where (telseqno,dttran,bankno) in ( ";
        }
        strBuilder.append(str1);
        Statement st = dbConn.createStatement();

        for (int o_id = 1; o_id <= connrows; o_id++) {
                String values = String.format("('%s','%s','%s')",
                        getStringByModul32(o_id) + padRight(padRight(Integer.toString(o_id), 0) + Integer.toString(w_id), 9),
                        getStringByModuloEight(o_id),
                        getStringByModul(o_id));
                strBuilder.append(values);

                if (o_id % commitbatch == 0) {
                    strBuilder.append(")");
                    st.execute(strBuilder.toString());
                    dbConn.commit();
                    strBuilder.setLength(0);
                    strBuilder.append(str1);
                }else {
                    if (o_id != connrows) {
                        strBuilder.append(",");
                    }
                }
            }
        // 处理剩余的部分
        if (strBuilder.length() > str1.length()) {
            strBuilder.append(")");
            st.execute(strBuilder.toString());
            dbConn.commit();
        }
    }

    private void delete_value_infra_prep(int w_id,int connrows, int commitbatch)
            throws SQLException, IOException {
//        StringBuilder strBuilder = new StringBuilder();
//        String str1 = "delete from etm_cnaps_prep@{no_full_scan} " +
//                "  where (telseqno,dttran,bankno) in (unnest(?),unnest(?),unnest(?)) ";
//        strBuilder.append(str1);
//        Statement st = dbConn.createStatement();
        ArrayList<Object> d1 = new ArrayList<Object>();
        ArrayList<Object> d2 = new ArrayList<Object>();
        ArrayList<Object> d3 = new ArrayList<Object>();

        for (int o_id = 1; o_id <= connrows; o_id++) {
            d1.add(getStringByModul32(o_id) + padRight(padRight(Integer.toString(o_id), 0) + Integer.toString(w_id), 9));
            d2.add(getStringByModuloEight(o_id));
            d3.add(getStringByModul(o_id));

            if (o_id%commitbatch == 0){
                String[] array1 = d1.toArray(new String[0]);
                String[] array2 = d2.toArray(new String[0]);
                String[] array3 = d3.toArray(new String[0]);
                stmtDeleteetmcnapsprepforunnest.setArray(1,dbConn.createArrayOf("text",array1));
                stmtDeleteetmcnapsprepforunnest.setArray(2,dbConn.createArrayOf("text",array2));
                stmtDeleteetmcnapsprepforunnest.setArray(3,dbConn.createArrayOf("text",array3));
                stmtDeleteetmcnapsprepforunnest.execute();
                dbConn.commit();
                d2.clear();
                d3.clear();
                d1.clear();

            String values = String.format("('%s','%s','%s')",
                    getStringByModul32(o_id) + padRight(padRight(Integer.toString(o_id), 0) + Integer.toString(w_id), 9),
                    getStringByModuloEight(o_id),
                    getStringByModul(o_id));
            }
        }
        // 处理剩余的部分
        String[] array1 = d1.toArray(new String[0]);
        String[] array2 = d2.toArray(new String[0]);
        String[] array3 = d3.toArray(new String[0]);
        stmtDeleteetmcnapsprepforunnest.setArray(1,dbConn.createArrayOf("text",array1));
        stmtDeleteetmcnapsprepforunnest.setArray(2,dbConn.createArrayOf("text",array2));
        stmtDeleteetmcnapsprepforunnest.setArray(3,dbConn.createArrayOf("text",array3));
        stmtDeleteetmcnapsprepforunnest.execute();
        dbConn.commit();
    }

    private void delete_value_infra_prep_tmp(int w_id,int connrows, int commitbatch)
            throws SQLException, IOException {
//        System.out.println("test:"+stmtDeleteetmcnapsprepforunnest_tmp + stmtDeleteetmcnapsprepforunnest);
        List<String[]> tuples = new ArrayList<>();
        for (int o_id = 1; o_id <= connrows; o_id++) {

            tuples.add(new String[]{getStringByModul32(o_id) + padRight(padRight(Integer.toString(o_id), 0) + Integer.toString(w_id), 9), getStringByModuloEight(o_id), getStringByModul(o_id)});
            if (o_id%commitbatch == 0){
                int index = 1;
                for (String[] tuple : tuples) {
                    for (String value : tuple) {
                        stmtDeleteetmcnapsprepforunnest_tmp.setString(index++, value);
                    }
                }
//                System.out.println(stmtDeleteetmcnapsprepforunnest_tmp);
                stmtDeleteetmcnapsprepforunnest_tmp.execute();
                dbConn.commit();
                tuples.clear();
            }

        }
        // 处理剩余的部分
        int index = 1;
        for (String[] tuple : tuples) {
            for (String value : tuple) {
                stmtDeleteetmcnapsprepforunnest_tmp.setString(index++, value);
            }
        }
//        System.out.println("test"+stmtDeleteetmcnapsprepforunnest_tmp);
        stmtDeleteetmcnapsprepforunnest_tmp.execute();
        dbConn.commit();
    }

    private void delete_value_infra_prep_2(int w_id,int connrows, int commitbatch)
            throws SQLException, IOException {
//        StringBuilder strBuilder = new StringBuilder();
//        String str1 = "delete from etm_cnaps_prep@{no_full_scan} " +
//                "  where (telseqno,dttran,bankno) in (unnest(?),unnest(?),unnest(?)) ";
//        strBuilder.append(str1);
//        Statement st = dbConn.createStatement();
        ArrayList<Object> d1 = new ArrayList<Object>();
        ArrayList<Object> d2 = new ArrayList<Object>();
        ArrayList<Object> d3 = new ArrayList<Object>();

        for (int o_id = 1; o_id <= connrows; o_id++) {
            d1.add(getStringByModul32(o_id) + padRight(padRight(Integer.toString(o_id), 0) + Integer.toString(w_id), 9));
            d2.add(getStringByModuloEight(o_id));
            d3.add(getStringByModul(o_id));

            if (o_id%commitbatch == 0){
                String[] array1 = d1.toArray(new String[0]);
                String[] array2 = d2.toArray(new String[0]);
                String[] array3 = d3.toArray(new String[0]);
                stmtDeleteetmcnapsprepforunnest_2.setArray(1,dbConn.createArrayOf("text",array1));
                stmtDeleteetmcnapsprepforunnest_2.setArray(2,dbConn.createArrayOf("text",array2));
                stmtDeleteetmcnapsprepforunnest_2.setArray(3,dbConn.createArrayOf("text",array3));
                stmtDeleteetmcnapsprepforunnest_2.execute();
                dbConn.commit();
                d2.clear();
                d3.clear();
                d1.clear();

                String values = String.format("('%s','%s','%s')",
                        getStringByModul32(o_id) + padRight(padRight(Integer.toString(o_id), 0) + Integer.toString(w_id), 9),
                        getStringByModuloEight(o_id),
                        getStringByModul(o_id));
            }
        }
        // 处理剩余的部分
        String[] array1 = d1.toArray(new String[0]);
        String[] array2 = d2.toArray(new String[0]);
        String[] array3 = d3.toArray(new String[0]);
        stmtDeleteetmcnapsprepforunnest_2.setArray(1,dbConn.createArrayOf("text",array1));
        stmtDeleteetmcnapsprepforunnest_2.setArray(2,dbConn.createArrayOf("text",array2));
        stmtDeleteetmcnapsprepforunnest_2.setArray(3,dbConn.createArrayOf("text",array3));
        stmtDeleteetmcnapsprepforunnest_2.execute();
        dbConn.commit();
    }
}
