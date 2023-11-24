/*
 * LoadData - Load Sample Data directly into database tables or into
 * CSV files using multiple parallel workers.
 *
 * Copyright (C) 2016, Denis Lussier
 * Copyright (C) 2016, Jan Wieck
 *
 */

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class Main5428
{
    private static Properties   ini = new Properties();
    private static String       db;
    private static Properties   dbProps;
    private static jTPCCRandom5428  rnd;
    private static String       fileLocation = null;
    private static String       csvNullValue = null;
    private static String       hint = null;
    private static int          numWarehouses;
    private static int          numWorkers;
    private static int          nextJob = 0;
    private static Object       nextJobLock = new Object();

    private static LoadDataWorker5428[] workers;
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
            LoadDataWorker5428.printHelp();
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
        rnd = new jTPCCRandom5428();

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
            LoadDataWorker5428.printHelp();
        }

        fileLocation    = iniGetString("fileLocation");
        csvNullValue    = iniGetString("csvNullValue", "NULL");
        hint    = iniGetString("hint");
        if (flag==null) {
            flag = Main5428.iniGetString("update").trim();
        }
        if (commitbatch == 0) {
            commitbatch = Main5428.iniGetInt("commitbatch");
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
        workers = new LoadDataWorker5428[numWorkers];
        workerThreads = new Thread[numWorkers];
        for (i = 0; i < numWorkers; i++)
        {
            Connection dbConn;

            try
            {
                dbConn = DriverManager.getConnection(db, dbProps);
                dbConn.setAutoCommit(false);
                if(hint.equals("hint")) {
                    workers[i] = new LoadDataWorker5428(i, dbConn, rnd.newRandom(),flag,commitbatch,totalrows,numWarehouses,hint);
                }else{
                    workers[i] = new LoadDataWorker5428(i, dbConn, rnd.newRandom(),flag,commitbatch,totalrows,numWarehouses);
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


class jTPCCRandom5428
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
     * jTPCCRandom5428()
     *
     *     Used to create the master jTPCCRandom5428() instance for loading
     *     the database. See below.
     */
    jTPCCRandom5428()
    {
        if (initialized)
            throw new IllegalStateException("Global instance exists");

        this.random = new Random(System.nanoTime());
        jTPCCRandom5428.nURandCLast = nextLong(0, 255);
        jTPCCRandom5428.nURandCC_ID = nextLong(0, 1023);
        jTPCCRandom5428.nURandCI_ID = nextLong(0, 8191);

        initialized = true;
    }

    /*
     * jTPCCRandom5428(CLoad)
     *
     *     Used to create the master jTPCCRandom5428 instance for running
     *     a benchmark load.
     *
     *     TPC-C 2.1.6 defines the rules for picking the C values of
     *     the non-uniform random number generator. In particular
     *     2.1.6.1 defines what numbers for the C value for generating
     *     C_LAST must be excluded from the possible range during run
     *     time, based on the number used during the load.
     */
    jTPCCRandom5428(long CLoad)
    {
        long delta;

        if (initialized)
            throw new IllegalStateException("Global instance exists");

        this.random = new Random(System.nanoTime());
        jTPCCRandom5428.nURandCC_ID = nextLong(0, 1023);
        jTPCCRandom5428.nURandCI_ID = nextLong(0, 8191);

        do
        {
            jTPCCRandom5428.nURandCLast = nextLong(0, 255);

            delta = Math.abs(jTPCCRandom5428.nURandCLast - CLoad);
            if (delta == 96 || delta == 112)
                continue;
            if (delta < 65 || delta > 119)
                continue;
            break;
        } while(true);

        initialized = true;
    }

    private jTPCCRandom5428(jTPCCRandom5428 parent)
    {
        this.random = new Random(System.nanoTime());
    }

    /*
     * newRandom()
     *
     *     Creates a derived random data generator to be used in another
     *     thread of the current benchmark load or run process. As per
     *     TPC-C 2.1.6 all terminals during a run must use the same C
     *     values per field. The jTPCCRandom5428 Class therefore cannot
     *     generate them per instance, but each thread's instance must
     *     inherit those numbers from a global instance.
     */
    jTPCCRandom5428 newRandom()
    {
        return new jTPCCRandom5428(this);
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
} // end jTPCCRandom5428


class LoadDataWorker5428 implements Runnable {
    private String hint = "nohint";
    private int worker;
    private Connection dbConn;
    private jTPCCRandom5428 rnd;
    private String flag;
    private int commitbatch;
    private int totalrows;
    private int warehouses;
    private int connrows;

    private StringBuffer sb;
    private Formatter fmt;

    private boolean writeCSV = false;
    private String csvNull = null;


    private PreparedStatement stmtOrderLineinfra = null;

    private PreparedStatement stmtDeleteetmcnapsprepforunnest = null;
    private PreparedStatement stmtDeleteetmcnapsprepforunnest_2 = null;
    private PreparedStatement stmtDeleteetmcnapsprepforunnest_tmp = null;
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


    LoadDataWorker5428(int worker, Connection dbConn, jTPCCRandom5428 rnd,String flag,int commitbatch,int totalrows,int warehouses)
            throws SQLException {
        this.worker = worker;
        this.dbConn = dbConn;
        this.rnd = rnd;
        this.flag = flag;
        this.commitbatch = commitbatch;
        this.totalrows = totalrows;
        this.warehouses = warehouses;
        this.connrows = Math.round(totalrows / warehouses);

        this.sb = new StringBuffer();
        this.fmt = new Formatter(sb);


        stmtOrderLineinfra = dbConn.prepareStatement(
                "INSERT INTO acm_vchr (" +
                        "  bankno,accbooktyp,dtacc,accsvccode,actxnseqno,subseqno,bookseq," +
                        "  subsys,tranevntno,accno,brcacc,busno,accnoseq,extbuscode,buscode,flgcr,flgct,flgdc,accentmod,ccy,amttran," +
                        "  ttlcnt,curbal,subnotyp,subno,accfaccode,accfacchnm,flgprinsub,flgsiso,flgentsub,anulcryov,custno,custname,prdcode," +
                        "  prdaccno,paccnoseq,prdaccname,voutyp,vouno,debitno,cashcode,oppaccno,oppaccnm,bnkopp,bnknmopp,oppccy,flgoppcr,trantype," +
                        "  flgbrevcls,dtrevacc,seqnor,trnintftyp,flgaccent,flgclrs,flgclsbook,flgspebook,flgentacc,chnlno,kltrncode,tranname,dtkltrn,telseqno,dttrnsrc,srcseqno," +
                        "  trncodb,memo,memono,remark,dtacctrn,trantm,brc,teller,telchk,telauth,bakfld1,bakfld2,bakfld3,bakfld4,bakfld5,bakfld6,modteller," +
                        "  modbrc,moddate,stsrcd,crttime,updtime,version,brcadsc,brcadscopp) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"+
                        "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"+
                        "?,?,?,?,?,?,?,?)"
        );


        stmtDeleteetmcnapsprepforunnest = dbConn.prepareStatement(
                "delete from  etm_cnaps_prep@{no_full_scan} " +
                        " using (select unnest(?),unnest(?),unnest(?)) as data(d1,d2,d3)" +
                        "  where telseqno =data.d1 and  dttran = data.d2 and " +
                        "bankno = data.d3"
        );
        stmtDeleteetmcnapsprepforunnest_2 = dbConn.prepareStatement(
                "delete from  etm_cnaps_prep " +
                        "  where (telseqno,dttran,bankno) in " +
                        "  (select unnest(?),unnest(?),unnest(?))"
        );
        if (this.hint.equals("hint")) {
            stmtDeleteetmcnapsprepforunnest_2 = dbConn.prepareStatement(
                    "delete from  etm_cnaps_prep@{no_full_scan} " +
                            "  where (telseqno,dttran,bankno) in " +
                            "  (select unnest(?),unnest(?),unnest(?))"
            );
        }
        stmtDeleteetmcnapsprepforunnest_tmp = dbConn.prepareStatement(Generatestr(commitbatch));

    }

    LoadDataWorker5428(int worker, Connection dbConn, jTPCCRandom5428 rnd,String flag,int commitbatch,int totalrows,int warehouses,String hint)
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

        stmtOrderLineinfra = dbConn.prepareStatement(
                "INSERT INTO acm_vchr (" +
                        "  bankno,accbooktyp,dtacc,accsvccode,actxnseqno,subseqno,bookseq," +
                        "  subsys,tranevntno,accno,brcacc,busno,accnoseq,extbuscode,buscode,flgcr,flgct,flgdc,accentmod,ccy,amttran," +
                        "  ttlcnt,curbal,subnotyp,subno,accfaccode,accfacchnm,flgprinsub,flgsiso,flgentsub,anulcryov,custno,custname,prdcode," +
                        "  prdaccno,paccnoseq,prdaccname,voutyp,vouno,debitno,cashcode,oppaccno,oppaccnm,bnkopp,bnknmopp,oppccy,flgoppcr,trantype," +
                        "  flgbrevcls,dtrevacc,seqnor,trnintftyp,flgaccent,flgclrs,flgclsbook,flgspebook,flgentacc,chnlno,kltrncode,tranname,dtkltrn,telseqno,dttrnsrc,srcseqno," +
                        "  trncodb,memo,memono,remark,dtacctrn,trantm,brc,teller,telchk,telauth,bakfld1,bakfld2,bakfld3,bakfld4,bakfld5,bakfld6,modteller," +
                        "  modbrc,moddate,stsrcd,crttime,updtime,version,brcadsc,brcadscopp) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"+
                                "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"+
                                "?,?,?,?,?,?,?,?)"
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
            while ((job = Main5428.getNextJob()) >= 0) {
                if (job == 0) {
                    continue;
                }
               if (flag.toLowerCase().equals("load_infra")) {
                    fmt.format("Worker %03d: acm_vchr prepare Loading Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    load_infra(job,connrows,commitbatch);
                    fmt.format("Worker %03d: acm_vchr prepare Load Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("load_infra_single")) {
                    fmt.format("Worker %03d: acm_vchr prepare Loading Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    load_infra_single(job,connrows);
                    fmt.format("Worker %03d: acm_vchr prepare Load Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("delete_value_infra_prep_tmp")) {
                    fmt.format("Worker %03d: acm_vchr prepare Deleting Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    delete_value_infra_prep_tmp(job,connrows,commitbatch);
                    fmt.format("Worker %03d: acm_vchr prepare Deleted Warehouse %6d done",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                }else if (flag.toLowerCase().equals("delete_value_infra_prep_2")) {
                    fmt.format("Worker %03d: acm_vchr prepare Deleting Warehouse %6d",
                            worker, job);
                    System.out.println(sb.toString());
                    sb.setLength(0);
                    delete_value_infra_prep_2(job,connrows,commitbatch);
                    fmt.format("Worker %03d: acm_vchr prepare Deleted Warehouse %6d done",
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
        System.out.println("    - 编译示例:javac -Djava.ext.dirs=./lib Main5428.java");
        System.out.println("    - 示例:java -cp .:./lib/*  Main5428 insert_infra 200000 10");
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
        System.out.println("    - 参数1: insert_infra: prepare方式加载数据。");
        System.out.println("    - 参数1: insert_infra_single: prepare方式加载数据一条一提交。");
        System.out.println("    - 对应的建表语句"+
                "CREATE TABLE acm_vchr("+
                "bankno VARCHAR(3) NOT NULL,"+
                "accbooktyp VARCHAR(1) NOT NULL,"+
                "dtacc VARCHAR(8) NOT NULL,"+
                "accsvccode VARCHAR(10) NULL,"+
                "actxnseqno VARCHAR(32) NOT NULL,"+
                "subseqno DECIMAL NOT NULL,"+
                "bookseq DECIMAL NOT NULL,"+
                "subsys VARCHAR(3) NULL,"+
                "tranevntno VARCHAR(20) NULL,"+
                "accno VARCHAR(32) NULL,"+
                "brcacc VARCHAR(10) NULL,"+
                "busno VARCHAR(5) NOT NULL,"+
                "accnoseq VARCHAR(6) NULL,"+
                "extbuscode VARCHAR(3) NULL,"+
                "buscode VARCHAR(16) NULL,"+
                "flgcr VARCHAR(1) NULL,"+
                "flgct VARCHAR(1) NULL,"+
                "flgdc VARCHAR(1) NOT NULL,"+
                "accentmod VARCHAR(1) NOT NULL,"+
                "ccy VARCHAR(3) NOT NULL,"+
                "amttran DECIMAL(20,2) NOT NULL,"+
                "ttlcnt DECIMAL NULL,"+
                "curbal DECIMAL(20,2) NULL,"+
                "subnotyp VARCHAR(1) NULL,"+
                "subno VARCHAR(20) NULL,"+
                "accfaccode VARCHAR(20) NULL,"+
                "accfacchnm VARCHAR(60) NULL,"+
                "flgprinsub VARCHAR(1) NULL,"+
                "flgsiso VARCHAR(1) NOT NULL,"+
                "flgentsub VARCHAR(1) NULL,"+
                "anulcryov VARCHAR(1) NULL,"+
                "custno VARCHAR(10) NULL,"+
                "custname VARCHAR(500) NULL,"+
                "prdcode VARCHAR(20) NULL,"+
                "prdaccno VARCHAR(32) NULL,"+
                "paccnoseq VARCHAR(7) NULL,"+
                "prdaccname VARCHAR(200) NULL,"+
                "voutyp VARCHAR(3) NULL,"+
                "vouno VARCHAR(20) NULL,"+
                "debitno VARCHAR(32) NULL,"+
                "cashcode VARCHAR(10) NULL,"+
                "oppaccno VARCHAR(32) NULL,"+
                "oppaccnm VARCHAR(200) NULL,"+
                "bnkopp VARCHAR(14) NULL,"+
                "bnknmopp VARCHAR(200) NULL,"+
                "oppccy VARCHAR(3) NULL,"+
                "flgoppcr VARCHAR(1) NULL,"+
                "trantype VARCHAR(2) NULL,"+
                "flgbrevcls VARCHAR(1) NULL,"+
                "dtrevacc VARCHAR(8) NULL,"+
                "seqnor VARCHAR(32) NULL,"+
                "trnintftyp VARCHAR(1) NULL,"+
                "flgaccent VARCHAR(1) NULL,"+
                "flgclrs VARCHAR(1) NULL,"+
                "flgclsbook VARCHAR(1) NULL,"+
                "flgspebook VARCHAR(1) NULL,"+
                "flgentacc VARCHAR(1) NULL,"+
                "chnlno VARCHAR(6) NULL,"+
                "kltrncode VARCHAR(10) NULL,"+
                "tranname VARCHAR(200) NULL,"+
                "dtkltrn VARCHAR(8) NULL,"+
                "telseqno VARCHAR(32) NULL,"+
                "dttrnsrc VARCHAR(8) NULL,"+
                "srcseqno VARCHAR(32) NULL,"+
                "trncodb VARCHAR(20) NULL,"+
                "memo VARCHAR(200) NULL,"+
                "memono VARCHAR(7) NULL,"+
                "remark VARCHAR(200) NULL,"+
                "dtacctrn VARCHAR(8) NULL,"+
                "trantm VARCHAR(14) NULL,"+
                "brc VARCHAR(10) NOT NULL,"+
                "teller VARCHAR(8) NULL,"+
                "telchk VARCHAR(8) NULL,"+
                "telauth VARCHAR(8) NULL,"+
                "bakfld1 VARCHAR(200) NULL,"+
                "bakfld2 VARCHAR(200) NULL,"+
                "bakfld3 VARCHAR(200) NULL,"+
                "bakfld4 VARCHAR(200) NULL,"+
                "bakfld5 VARCHAR(200)NULL,"+
                "bakfld6 VARCHAR(200) NULL,"+
                "modteller VARCHAR(8)NULL,"+
                "modbrc VARCHAR(10)NULL,"+
                "moddate VARCHAR(8),"+
                "stsrcd VARCHAR(2) NULL,"+
                "crttime TIMESTAMP NULL,"+
                "updtime TIMESTAMP NULL,"+
                "version DECIMAL NULL,"+
                "brcadsc VARCHAR(10) NULL,"+
                "brcadscopp VARCHAR(10) NULL,"+
                "CONSTRAINT \"primary\" primary key(actxnseqno,dtacc,subseqno,bookseq,accbooktyp,bankno),"+
                "INDEX acm_vchr_10_idx10(brc,dtkltrn,flgct,flgdc,accbooktyp,bankno),"+
                "INDEX acm_vchr_3_idx03(teller,brc,ccy,dtkltrn,trncodb),"+
                "INDEX acm_vchr_9_idx09(dtacc,brc,teller,flgsiso),"+
                "INDEX acm_vchr_5_idx05(dtacc,brcacc,ccy),"+
                "INDEX acm_vchr_4_idx04(brcacc,busno,dtacc,ccy,prdcode,flgbrevcls,flgaccent,flgsiso,accbooktyp,bankno),"+
                "INDEX acm_vchr_8_idx08(dtacc,teller,brc,flgsiso),"+
                "INDEX updtime_idx1(updtime));"
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
    public static String getStringByModSingle(int number) {
        switch (number % 3) {
            case 0:
                return "x";
            case 1:
                return "Y";
            case 2:
                return "Z";
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
    public static Double getDoubleByModuloEight(int number) {
        switch (number % 8) {
            case 0:
                return 1.23;
            case 1:
                return 2.34;
            case 2:
                return 3.45;
            case 3:
                return 4.56;
            case 4:
                return 5.67;
            case 5:
                return 6.78;
            case 6:
                return 7.89;
            case 7:
                return 8.91;
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

    private void load_infra(int w_id,int connrows,int commitbatch) throws SQLException, IOException {
        for (int o_id = 1; o_id <= connrows; o_id++) {
            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
                long now = System.currentTimeMillis();
                // primary bankno  6
                stmtOrderLineinfra.setString(1, getStringByModul(o_id));
                // primary accbooktyp 5
                stmtOrderLineinfra.setString(2, getStringByModSingle(o_id));
                // primary dtacc 2
                stmtOrderLineinfra.setString(3, getStringByModuloEight(o_id));
                stmtOrderLineinfra.setString(4, generateRandomString(10));
                // primary actxnseqno 1
                stmtOrderLineinfra.setString(5, getStringByModul32(o_id)+padRight(padRight(Integer.toString(o_id),0)+Integer.toString(w_id),9));
                // primary subseqno  3
                stmtOrderLineinfra.setDouble(6,getDoubleByModuloEight(o_id));
                // primary bookseq 4
                stmtOrderLineinfra.setDouble(7,getDoubleByModuloEight(o_id+1));
                stmtOrderLineinfra.setString(8, generateRandomString(3));
                stmtOrderLineinfra.setString(9, generateRandomString(20));
                stmtOrderLineinfra.setString(10, generateRandomString(32));
                stmtOrderLineinfra.setString(11, generateRandomString(10));
                stmtOrderLineinfra.setString(12, generateRandomString(5));
                // accnoseq  和 paccnoseq
                String accnoseq = generateRandomString(6);
                stmtOrderLineinfra.setString(13, generateRandomString(6));
                stmtOrderLineinfra.setString(36, generateRandomString(1)+accnoseq);
                // buscode 和 extbuscode
                String extbuscode = generateRandomString(3);
                stmtOrderLineinfra.setString(14, extbuscode);
                stmtOrderLineinfra.setString(15, generateRandomString(7)+extbuscode+generateRandomString(6));
                // flgcr ,flgct,flgdc
                stmtOrderLineinfra.setString(16, generateRandomString(1));
                stmtOrderLineinfra.setString(17, generateRandomString(1));
                stmtOrderLineinfra.setString(18, generateRandomString(1));
                stmtOrderLineinfra.setString(19, generateRandomString(1));
                stmtOrderLineinfra.setString(20, generateRandomString(3));
                // amttran  decimal(20,2)  real 3,2
                stmtOrderLineinfra.setDouble(21, getDoubleByModuloEight(o_id));
                // decimal
                stmtOrderLineinfra.setDouble(22, getDoubleByModuloEight(o_id));
                // curbal  decimal(20,2)  real 3,2
                stmtOrderLineinfra.setDouble(23, getDoubleByModuloEight(o_id));
                //subnotyp 1
                stmtOrderLineinfra.setString(24, getStringByModSingle(o_id));
                //subno 20
                stmtOrderLineinfra.setString(25, generateRandomString(19)+getStringByModSingle(o_id));
                // accfacccode 20
                stmtOrderLineinfra.setString(26, generateRandomString(20));
                // accfacchnm  60
                stmtOrderLineinfra.setString(27, generateRandomString(60));
                // flgprinsub,flgsiso,flgentsub
                stmtOrderLineinfra.setString(28, generateRandomString(1));
                stmtOrderLineinfra.setString(29, generateRandomString(1));
                stmtOrderLineinfra.setString(30, generateRandomString(1));
                // anulcryov
                stmtOrderLineinfra.setString(31, generateRandomString(1));
                stmtOrderLineinfra.setString(32, generateRandomString(10));
                stmtOrderLineinfra.setString(33, generateRandomString(500));
                stmtOrderLineinfra.setString(34, generateRandomString(20));
                stmtOrderLineinfra.setString(35, generateRandomString(32));
                stmtOrderLineinfra.setString(36, generateRandomString(7));
                stmtOrderLineinfra.setString(37, generateRandomString(200));
                stmtOrderLineinfra.setString(38, generateRandomString(3));
                stmtOrderLineinfra.setString(39, generateRandomString(20));
                stmtOrderLineinfra.setString(40, generateRandomString(32));
                stmtOrderLineinfra.setString(41, generateRandomString(10));
                stmtOrderLineinfra.setString(42, generateRandomString(32));
                stmtOrderLineinfra.setString(43, generateRandomString(200));
                stmtOrderLineinfra.setString(44, generateRandomString(14));
                stmtOrderLineinfra.setString(45, generateRandomString(200));
                stmtOrderLineinfra.setString(46, generateRandomString(3));
                stmtOrderLineinfra.setString(47, generateRandomString(1));
                stmtOrderLineinfra.setString(48, generateRandomString(2));
                stmtOrderLineinfra.setString(49, generateRandomString(1));
                stmtOrderLineinfra.setString(50, generateRandomString(8));
                stmtOrderLineinfra.setString(51, generateRandomString(32));
                // trnintftyp,flgaccent,flgclrs,flgclsbook,flgspebook,flgentacc
                stmtOrderLineinfra.setString(52, generateRandomString(1));
                stmtOrderLineinfra.setString(53, generateRandomString(1));
                stmtOrderLineinfra.setString(54, generateRandomString(1));
                stmtOrderLineinfra.setString(55, generateRandomString(1));
                stmtOrderLineinfra.setString(56, generateRandomString(1));
                stmtOrderLineinfra.setString(57, generateRandomString(1));
                // other
                stmtOrderLineinfra.setString(58, generateRandomString(6));
                stmtOrderLineinfra.setString(59, generateRandomString(10));
                stmtOrderLineinfra.setString(60, generateRandomString(200));
                stmtOrderLineinfra.setString(61, generateRandomString(8));
                stmtOrderLineinfra.setString(62, generateRandomString(32));
                stmtOrderLineinfra.setString(63, generateRandomString(8));
                stmtOrderLineinfra.setString(64, generateRandomString(32));
                stmtOrderLineinfra.setString(65, generateRandomString(20));
                stmtOrderLineinfra.setString(66, generateRandomString(200));
                stmtOrderLineinfra.setString(67, generateRandomString(7));
                stmtOrderLineinfra.setString(68, generateRandomString(200));
                stmtOrderLineinfra.setString(69, generateRandomString(8));
                stmtOrderLineinfra.setString(70, generateRandomString(14));
                stmtOrderLineinfra.setString(71, generateRandomString(10));
                // teller,telchk,tleauth
                stmtOrderLineinfra.setString(72, generateRandomString(8));
                stmtOrderLineinfra.setString(73, generateRandomString(8));
                stmtOrderLineinfra.setString(74, generateRandomString(8));
                // bakfld1,bakfld2,bakfld3,bakfld4,bakfld5,bakfld6
                stmtOrderLineinfra.setString(75, generateRandomString(200));
                stmtOrderLineinfra.setString(76, generateRandomString(200));
                stmtOrderLineinfra.setString(77, generateRandomString(200));
                stmtOrderLineinfra.setString(78, generateRandomString(200));
                stmtOrderLineinfra.setString(79, generateRandomString(200));
                stmtOrderLineinfra.setString(80, generateRandomString(200));
                stmtOrderLineinfra.setString(81, generateRandomString(8));
                stmtOrderLineinfra.setString(82, generateRandomString(10));
                stmtOrderLineinfra.setString(83, generateRandomString(8));
                stmtOrderLineinfra.setString(84, generateRandomString(2));
                stmtOrderLineinfra.setTimestamp(85, new java.sql.Timestamp(now));
                stmtOrderLineinfra.setTimestamp(86, new java.sql.Timestamp(now));
                stmtOrderLineinfra.setDouble(87, getDoubleByModuloEight(o_id));
                stmtOrderLineinfra.setString(88, generateRandomString(10));
                stmtOrderLineinfra.setString(89, generateRandomString(10));
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

    private void load_infra_single(int w_id,int connrows) throws SQLException, IOException {
        for (int o_id = 1; o_id <= connrows; o_id++) {
            /*
             * Create the ORDER_LINE rows for this ORDER.
             */
            long now = System.currentTimeMillis();
            // primary bankno  6
            stmtOrderLineinfra.setString(1, getStringByModul(o_id));
            // primary accbooktyp 5
            stmtOrderLineinfra.setString(2, getStringByModSingle(o_id));
            // primary dtacc 2
            stmtOrderLineinfra.setString(3, getStringByModuloEight(o_id));
            stmtOrderLineinfra.setString(4, generateRandomString(10));
            // primary actxnseqno 1
            stmtOrderLineinfra.setString(5, getStringByModul32(o_id) + padRight(padRight(Integer.toString(o_id), 0) + Integer.toString(w_id), 9));
            // primary subseqno  3
            stmtOrderLineinfra.setDouble(6, getDoubleByModuloEight(o_id));
            // primary bookseq 4
            stmtOrderLineinfra.setDouble(7, getDoubleByModuloEight(o_id + 1));
            stmtOrderLineinfra.setString(8, generateRandomString(3));
            stmtOrderLineinfra.setString(9, generateRandomString(20));
            stmtOrderLineinfra.setString(10, generateRandomString(32));
            stmtOrderLineinfra.setString(11, generateRandomString(10));
            stmtOrderLineinfra.setString(12, generateRandomString(5));
            // accnoseq  和 paccnoseq
            String accnoseq = generateRandomString(6);
            stmtOrderLineinfra.setString(13, generateRandomString(6));
            stmtOrderLineinfra.setString(36, generateRandomString(1) + accnoseq);
            // buscode 和 extbuscode
            String extbuscode = generateRandomString(3);
            stmtOrderLineinfra.setString(14, extbuscode);
            stmtOrderLineinfra.setString(15, generateRandomString(7) + extbuscode + generateRandomString(6));
            // flgcr ,flgct,flgdc
            stmtOrderLineinfra.setString(16, generateRandomString(1));
            stmtOrderLineinfra.setString(17, generateRandomString(1));
            stmtOrderLineinfra.setString(18, generateRandomString(1));
            stmtOrderLineinfra.setString(19, generateRandomString(1));
            stmtOrderLineinfra.setString(20, generateRandomString(3));
            // amttran  decimal(20,2)  real 3,2
            stmtOrderLineinfra.setDouble(21, getDoubleByModuloEight(o_id));
            // decimal
            stmtOrderLineinfra.setDouble(22, getDoubleByModuloEight(o_id));
            // curbal  decimal(20,2)  real 3,2
            stmtOrderLineinfra.setDouble(23, getDoubleByModuloEight(o_id));
            //subnotyp 1
            stmtOrderLineinfra.setString(24, getStringByModSingle(o_id));
            //subno 20
            stmtOrderLineinfra.setString(25, generateRandomString(19) + getStringByModSingle(o_id));
            // accfacccode 20
            stmtOrderLineinfra.setString(26, generateRandomString(20));
            // accfacchnm  60
            stmtOrderLineinfra.setString(27, generateRandomString(60));
            // flgprinsub,flgsiso,flgentsub
            stmtOrderLineinfra.setString(28, generateRandomString(1));
            stmtOrderLineinfra.setString(29, generateRandomString(1));
            stmtOrderLineinfra.setString(30, generateRandomString(1));
            // anulcryov
            stmtOrderLineinfra.setString(31, generateRandomString(1));
            stmtOrderLineinfra.setString(32, generateRandomString(10));
            stmtOrderLineinfra.setString(33, generateRandomString(500));
            stmtOrderLineinfra.setString(34, generateRandomString(20));
            stmtOrderLineinfra.setString(35, generateRandomString(32));
            stmtOrderLineinfra.setString(36, generateRandomString(7));
            stmtOrderLineinfra.setString(37, generateRandomString(200));
            stmtOrderLineinfra.setString(38, generateRandomString(3));
            stmtOrderLineinfra.setString(39, generateRandomString(20));
            stmtOrderLineinfra.setString(40, generateRandomString(32));
            stmtOrderLineinfra.setString(41, generateRandomString(10));
            stmtOrderLineinfra.setString(42, generateRandomString(32));
            stmtOrderLineinfra.setString(43, generateRandomString(200));
            stmtOrderLineinfra.setString(44, generateRandomString(14));
            stmtOrderLineinfra.setString(45, generateRandomString(200));
            stmtOrderLineinfra.setString(46, generateRandomString(3));
            stmtOrderLineinfra.setString(47, generateRandomString(1));
            stmtOrderLineinfra.setString(48, generateRandomString(2));
            stmtOrderLineinfra.setString(49, generateRandomString(1));
            stmtOrderLineinfra.setString(50, generateRandomString(8));
            stmtOrderLineinfra.setString(51, generateRandomString(32));
            // trnintftyp,flgaccent,flgclrs,flgclsbook,flgspebook,flgentacc
            stmtOrderLineinfra.setString(52, generateRandomString(1));
            stmtOrderLineinfra.setString(53, generateRandomString(1));
            stmtOrderLineinfra.setString(54, generateRandomString(1));
            stmtOrderLineinfra.setString(55, generateRandomString(1));
            stmtOrderLineinfra.setString(56, generateRandomString(1));
            stmtOrderLineinfra.setString(57, generateRandomString(1));
            // other
            stmtOrderLineinfra.setString(58, generateRandomString(6));
            stmtOrderLineinfra.setString(59, generateRandomString(10));
            stmtOrderLineinfra.setString(60, generateRandomString(200));
            stmtOrderLineinfra.setString(61, generateRandomString(8));
            stmtOrderLineinfra.setString(62, generateRandomString(32));
            stmtOrderLineinfra.setString(63, generateRandomString(8));
            stmtOrderLineinfra.setString(64, generateRandomString(32));
            stmtOrderLineinfra.setString(65, generateRandomString(20));
            stmtOrderLineinfra.setString(66, generateRandomString(200));
            stmtOrderLineinfra.setString(67, generateRandomString(7));
            stmtOrderLineinfra.setString(68, generateRandomString(200));
            stmtOrderLineinfra.setString(69, generateRandomString(8));
            stmtOrderLineinfra.setString(70, generateRandomString(14));
            stmtOrderLineinfra.setString(71, generateRandomString(10));
            // teller,telchk,tleauth
            stmtOrderLineinfra.setString(72, generateRandomString(8));
            stmtOrderLineinfra.setString(73, generateRandomString(8));
            stmtOrderLineinfra.setString(74, generateRandomString(8));
            // bakfld1,bakfld2,bakfld3,bakfld4,bakfld5,bakfld6
            stmtOrderLineinfra.setString(75, generateRandomString(200));
            stmtOrderLineinfra.setString(76, generateRandomString(200));
            stmtOrderLineinfra.setString(77, generateRandomString(200));
            stmtOrderLineinfra.setString(78, generateRandomString(200));
            stmtOrderLineinfra.setString(79, generateRandomString(200));
            stmtOrderLineinfra.setString(80, generateRandomString(200));
            stmtOrderLineinfra.setString(81, generateRandomString(8));
            stmtOrderLineinfra.setString(82, generateRandomString(10));
            stmtOrderLineinfra.setString(83, generateRandomString(8));
            stmtOrderLineinfra.setString(84, generateRandomString(2));
            stmtOrderLineinfra.setTimestamp(85, new java.sql.Timestamp(now));
            stmtOrderLineinfra.setTimestamp(86, new java.sql.Timestamp(now));
            stmtOrderLineinfra.setDouble(87, getDoubleByModuloEight(o_id));
            stmtOrderLineinfra.setString(88, generateRandomString(10));
            stmtOrderLineinfra.setString(89, generateRandomString(10));
            stmtOrderLineinfra.execute();
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
