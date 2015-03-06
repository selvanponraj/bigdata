package com.burberry.customer;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveProperties;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;

@RunWith(StandaloneHiveRunner.class)
public class CustomerTest {

	@HiveProperties
	public Map<String, String> hiveProperties = MapUtils.putAll(new HashMap(),
			new Object[] { "MY.HDFS.DIR", "${hadoop.tmp.dir}", "my.schema",
					"custom_general", });
	@HiveSetupScript
	private String createSchemaScript = "create schema ${hiveconf:my.schema}";

	@HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/hana_customer/data_from_file.csv")
	private File dataFromHanaCust = new File(ClassLoader.getSystemResource(
			"customer/hana_cust_data.csv").getPath());

	@HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/hana_user/data_from_file.csv")
	private File dataFromHanaUser = new File(ClassLoader.getSystemResource(
			"customer/hana_user_data.csv").getPath());
	
	@HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/hana_lead/data_from_file.csv")
	private File dataFromHanaLead = new File(ClassLoader.getSystemResource(
			"customer/hana_lead_data.csv").getPath());
	
	@HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/cust/data_from_file.csv")
	private File dataFromCust = new File(ClassLoader.getSystemResource(
			"customer/output.csv").getPath());
	
	@HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/lead/data_from_file.csv")
	private File dataFromLead = new File(ClassLoader.getSystemResource(
			"customer/output_lead.csv").getPath());
	
	@HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/cust_lead_inter/data_from_file.csv")
	private File dataFromCustLeadInter = new File(ClassLoader.getSystemResource(
			"customer/output_cust_lead_inter.csv").getPath());
	
	@HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/bronze/data_from_file.csv")
	private File dataFromBronze = new File(ClassLoader.getSystemResource(
			"customer/bronze.csv").getPath());
	
	@HiveSQL(files = { "customer/create_table.sql" }, encoding = "UTF-8")
	private HiveShell hiveShell;

	@Test
	public void testTablesCreated() {
		List<String> expected = Arrays.asList("cust", "hana_customer",
				"hana_user", "lead", "cust_lead_inter", "cust_lead", "bronze", "hana_lead");
		List<String> actual = hiveShell.executeQuery("show tables");
		Collections.sort(expected);
		Collections.sort(actual);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testCustTable() throws Exception {
		File select_cust = new File(ClassLoader.getSystemResource(
				"customer/select_cust.sql").getPath());
		String select = new Scanner(select_cust).useDelimiter("\\Z").next();
		//String query = "SELECT hana_customer.firstname, hana_customer.lastname FROM hana_customer left outer join hana_user on (hana_customer.lastmodifiedbyid = hana_user.id)";
		List<String> actual = hiveShell.executeQuery(select);
		
		File outputFile = new File(ClassLoader.getSystemResource(
				"customer/output.csv").getPath());
		String output = new Scanner(outputFile).useDelimiter("\\Z").next();
		output = output.replace(',', '\t');
		String lines[] = output.split("\\r?\\n");
		List<String> expected = Arrays.asList(lines);
		
		Collections.sort(expected);
		Collections.sort(actual);
		
		Assert.assertEquals(expected, actual);
		Assert.assertTrue(actual.containsAll(expected));
	}
	
	@Test
	public void testLeadTable() throws Exception {
		File select_lead = new File(ClassLoader.getSystemResource(
				"customer/select_lead.sql").getPath());
		String select = new Scanner(select_lead).useDelimiter("\\Z").next();
		//String query = "SELECT hana_customer.firstname, hana_customer.lastname FROM hana_customer left outer join hana_user on (hana_customer.lastmodifiedbyid = hana_user.id)";
		List<String> actual = hiveShell.executeQuery(select);
		
		File outputFile = new File(ClassLoader.getSystemResource(
				"customer/output_lead.csv").getPath());
		String output = new Scanner(outputFile).useDelimiter("\\Z").next();
		output = output.replace(',', '\t');
		String lines[] = output.split("\\r?\\n");
		List<String> expected = Arrays.asList(lines);
		Collections.sort(expected);
		Collections.sort(actual);
		Assert.assertEquals(expected, actual);
		Assert.assertTrue(actual.containsAll(expected));
	}

	@Test
	public void testCustLeadInterTable() throws Exception {
		
		File select_cust = new File(ClassLoader.getSystemResource(
				"customer/select_cust_lead_inter.sql").getPath());
		String select = new Scanner(select_cust).useDelimiter("\\Z").next();
		List<String> actual = hiveShell.executeQuery(select);
		/*
		File select_cust = new File(ClassLoader.getSystemResource(
				"customer/select_cust.sql").getPath());
		String select_first = new Scanner(select_cust).useDelimiter("\\Z").next();
		//String query = "SELECT hana_customer.firstname, hana_customer.lastname FROM hana_customer left outer join hana_user on (hana_customer.lastmodifiedbyid = hana_user.id)";
		List<String> actual = hiveShell.executeQuery(select_first);
		
		File select_lead = new File(ClassLoader.getSystemResource(
				"customer/select_lead.sql").getPath());
		String select_second = new Scanner(select_lead).useDelimiter("\\Z").next();
		//String query = "SELECT hana_customer.firstname, hana_customer.lastname FROM hana_customer left outer join hana_user on (hana_customer.lastmodifiedbyid = hana_user.id)";
		List<String> second = hiveShell.executeQuery(select_second);
		actual.addAll(second);
		*/
		File outputFile = new File(ClassLoader.getSystemResource(
				"customer/output_cust_lead_inter.csv").getPath());
		String output = new Scanner(outputFile).useDelimiter("\\Z").next();
		output = output.replace(',', '\t');
		String lines[] = output.split("\\r?\\n");
		List<String> expected = Arrays.asList(lines);
		
		Collections.sort(expected);
		Collections.sort(actual);
		
		Assert.assertEquals(expected, actual);
		Assert.assertTrue(actual.containsAll(expected));
	}
    
	@Test
	public void testCustLeadTable() throws Exception {
		
		File select_cust = new File(ClassLoader.getSystemResource(
				"customer/select_cust_lead.sql").getPath());
		String select = new Scanner(select_cust).useDelimiter("\\Z").next();
		List<String> actual = hiveShell.executeQuery(select);
		/*
		File select_cust = new File(ClassLoader.getSystemResource(
				"customer/select_cust.sql").getPath());
		String select_first = new Scanner(select_cust).useDelimiter("\\Z").next();
		//String query = "SELECT hana_customer.firstname, hana_customer.lastname FROM hana_customer left outer join hana_user on (hana_customer.lastmodifiedbyid = hana_user.id)";
		List<String> actual = hiveShell.executeQuery(select_first);
		
		File select_lead = new File(ClassLoader.getSystemResource(
				"customer/select_lead.sql").getPath());
		String select_second = new Scanner(select_lead).useDelimiter("\\Z").next();
		//String query = "SELECT hana_customer.firstname, hana_customer.lastname FROM hana_customer left outer join hana_user on (hana_customer.lastmodifiedbyid = hana_user.id)";
		List<String> second = hiveShell.executeQuery(select_second);
		actual.addAll(second);
		*/
		File outputFile = new File(ClassLoader.getSystemResource(
				"customer/output_cust_lead.csv").getPath());
		String output = new Scanner(outputFile).useDelimiter("\\Z").next();
		output = output.replace(',', '\t');
		String lines[] = output.split("\\r?\\n");
		List<String> expected = Arrays.asList(lines);
		
		Collections.sort(expected);
		Collections.sort(actual);
		
		Assert.assertEquals(expected, actual);
		Assert.assertTrue(actual.containsAll(expected));
	}
	

}
