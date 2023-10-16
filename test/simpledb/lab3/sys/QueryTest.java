package simpledb.lab3.sys;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import simpledb.storage.BufferPool;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.HeapFile;
import simpledb.storage.HeapFileEncoder;
import simpledb.Parser;
import simpledb.optimizer.TableStats;
import simpledb.systemtest.SystemTestUtil;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.Utility;

public class QueryTest {
	
	/**
	 * Given a matrix of tuples from SystemTestUtil.createRandomHeapFile, create an identical HeapFile table
	 * @param tuples Tuples to create a HeapFile from
	 * @param columns Each entry in tuples[] must have "columns == tuples.get(i).size()"
	 * @param colPrefix String to prefix to the column names (the columns are named after their column number by default)
	 * @return a new HeapFile containing the specified tuples
	 * @throws IOException if a temporary file can't be created to hand to HeapFile to open and read its data
	 */
	public static HeapFile createDuplicateHeapFile(List<List<Integer>> tuples, int columns, String colPrefix) throws IOException {
        File temp = File.createTempFile("table", ".dat");
        temp.deleteOnExit();
        HeapFileEncoder.convert(tuples, temp, BufferPool.getPageSize(), columns);
        return Utility.openHeapFile(columns, colPrefix, temp);
	}
	
	@Test(timeout=20000) public void queryTest() throws IOException {
		// This test is intended to approximate the join described in the
		// "Query Planning" section of 2009 Quiz 1,
		// though with some minor variation due to limitations in simpledb
		// and to only test your integer-heuristic code rather than
		// string-heuristic code.		
		final int IO_COST = 101;

		// Create all of the tables, and add them to the catalog
		List<List<Integer>> empTuples = new ArrayList<>();
		HeapFile emp = SystemTestUtil.createRandomHeapFile(6, 100000, null, empTuples, "c");
		Database.getCatalog().addTable(emp, "emp");
		
		List<List<Integer>> deptTuples = new ArrayList<>();
		HeapFile dept = SystemTestUtil.createRandomHeapFile(3, 1000, null, deptTuples, "c");	
		Database.getCatalog().addTable(dept, "dept");
		
		List<List<Integer>> hobbyTuples = new ArrayList<>();
		HeapFile hobby = SystemTestUtil.createRandomHeapFile(6, 1000, null, hobbyTuples, "c");
		Database.getCatalog().addTable(hobby, "hobby");
		
		List<List<Integer>> hobbiesTuples = new ArrayList<>();
		HeapFile hobbies = SystemTestUtil.createRandomHeapFile(2, 200000, null, hobbiesTuples, "c");
		Database.getCatalog().addTable(hobbies, "hobbies");
		
		// Get TableStats objects for each of the tables that we just generated.
		TableStats.setTableStats("emp", new TableStats(Database.getCatalog().getTableId("emp"), IO_COST));
		TableStats.setTableStats("dept", new TableStats(Database.getCatalog().getTableId("dept"), IO_COST));
		TableStats.setTableStats("hobby", new TableStats(Database.getCatalog().getTableId("hobby"), IO_COST));
		TableStats.setTableStats("hobbies", new TableStats(Database.getCatalog().getTableId("hobbies"), IO_COST));

//		Parser.setStatsMap(stats);
		
		Transaction t = new Transaction();
		t.start();
		Parser p = new Parser();
		p.setTransaction(t);
		
		// Each of these should return around 20,000
		// This Parser implementation currently just dumps to stdout, so checking that isn't terribly clean.
		// So, don't bother for now; future TODO.
		// Regardless, each of the following should be optimized to run quickly,
		// even though the worst case takes a very long time.
		p.processNextStatement("SELECT * FROM emp,dept,hobbies,hobby WHERE emp.c1 = dept.c0 AND hobbies.c0 = emp.c2 AND hobbies.c1 = hobby.c0 AND emp.c3 < 1000;");
	}
	
	/*
	  Build a large series of tables; then run the command-line query code and execute a query.
	  The number of tables is large enough that the query will only succeed within the
	  specified time if a join method faster than nested-loops join is available.
	  The tables are also too big for a query to be successful if its query plan isn't reasonably efficient,
	  and there are too many tables for a brute-force search of all possible query plans.
	 */
	// Not required for Lab 4
}
