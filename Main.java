/*
 * LoadData - Load Sample Data directly into database tables or into
 * CSV files using multiple parallel workers.
 *
 * Copyright (C) 2016, Denis Lussier
 * Copyright (C) 2016, Jan Wieck
 *
 */

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
        fileLocation    = iniGetString("fileLocation");
        csvNullValue    = iniGetString("csvNullValue", "NULL");
        String flag = Main.iniGetString("update").trim();
        int commitbatch = Main.iniGetInt("commitbatch");

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
                if (writeCSV)
                    workers[i] = new LoadDataWorker(i, csvNullValue,
                            rnd.newRandom(),flag);
                else
                    workers[i] = new LoadDataWorker(i, dbConn, rnd.newRandom(),flag,commitbatch);
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
    private int worker;
    private Connection dbConn;
    private jTPCCRandom rnd;
    private String flag;
    private int commitbatch;

    private StringBuffer sb;
    private Formatter fmt;

    private boolean writeCSV = false;
    private String csvNull = null;

    private PreparedStatement stmtOrderLine = null;
    private PreparedStatement stmtUpdateOrderLine = null;
    private PreparedStatement stmtInsertUpdateOrderLine = null;
    private PreparedStatement stmtInsertUpdateBucketOrderLine = null;
    private PreparedStatement stmtUpsertBucketOrderLine = null;
    private PreparedStatement stmtDeleteOrderLine = null;

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

    LoadDataWorker(int worker, String csvNull, jTPCCRandom rnd,String flag) {
        this.worker = worker;
        this.csvNull = csvNull;
        this.rnd = rnd;
        this.flag = flag;

        this.sb = new StringBuffer();
        this.fmt = new Formatter(sb);
        this.writeCSV = true;

        this.sbConfig = new StringBuffer();
        this.fmtConfig = new Formatter(sbConfig);
        this.sbItem = new StringBuffer();
        this.fmtItem = new Formatter(sbItem);
        this.sbWarehouse = new StringBuffer();
        this.fmtWarehouse = new Formatter(sbWarehouse);
        this.sbDistrict = new StringBuffer();
        this.fmtDistrict = new Formatter(sbDistrict);
        this.sbStock = new StringBuffer();
        this.fmtStock = new Formatter(sbStock);
        this.sbCustomer = new StringBuffer();
        this.fmtCustomer = new Formatter(sbCustomer);
        this.sbHistory = new StringBuffer();
        this.fmtHistory = new Formatter(sbHistory);
        this.sbOrder = new StringBuffer();
        this.fmtOrder = new Formatter(sbOrder);
        this.sbOrderLine = new StringBuffer();
        this.fmtOrderLine = new Formatter(sbOrderLine);
        this.sbNewOrder = new StringBuffer();
        this.fmtNewOrder = new Formatter(sbNewOrder);
    }

    LoadDataWorker(int worker, Connection dbConn, jTPCCRandom rnd,String flag,int commitbatch)
            throws SQLException {
        this.worker = worker;
        this.dbConn = dbConn;
        this.rnd = rnd;
        this.flag = flag;
        this.commitbatch = commitbatch;

        this.sb = new StringBuffer();
        this.fmt = new Formatter(sb);

        stmtOrderLine = dbConn.prepareStatement(
                "INSERT INTO bmsql_order_line (" +
                        "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " +
                        "  ol_supply_w_id, ol_delivery_d, ol_quantity, " +
                        "  ol_amount, ol_dist_info) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );
        stmtUpdateOrderLine = dbConn.prepareStatement(
                "update bmsql_order_line " +
                        "  set ol_i_id = ? " +
                        "  where ol_o_id =? and  ol_d_id = ? and " +
                        "ol_w_id = ? and ol_number =?"
        );

        stmtDeleteOrderLine = dbConn.prepareStatement(
                "delete from  bmsql_order_line " +
                        "  where ol_o_id =? and  ol_d_id = ? and " +
                        "ol_w_id = ? and ol_number =?"
        );
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
        String str1 = "update bmsql_order_line " +
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
}
