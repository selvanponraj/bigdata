package com.burberry.customer;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.collections.MapUtils;
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

	@HiveSQL(files = { "customer/create_table.sql" }, encoding = "UTF-8")
	private HiveShell hiveShell;

	@Test
	public void testTablesCreated() {
		List<String> expected = Arrays.asList("cust", "hana_customer",
				"hana_user");
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
		String query = "SELECT hana_customer.firstname, hana_customer.lastname FROM hana_customer left outer join hana_user on (hana_customer.lastmodifiedbyid = hana_user.id)";
		List<String> actual = hiveShell.executeQuery(select);
		File outputFile = new File(ClassLoader.getSystemResource(
				"customer/output.csv").getPath());
		String output = new Scanner(outputFile).useDelimiter("\\Z").next();
		output = output.replace(',', '\t');
		String lines[] = output.split("\\r?\\n");
		List<String> expected = Arrays.asList(lines);
		Assert.assertEquals(expected, actual);
		Assert.assertTrue(actual.containsAll(expected));
	}

}
