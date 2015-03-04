/*
 * Copyright 2013 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.burberry.customer;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

/**
 * Hive Runner Reference implementation.
 * <p/>
 * All HiveRunner tests should run with the StandaloneHiveRunner
 */
@RunWith(StandaloneHiveRunner.class)
public class HelloHiveRunner {


    /**
     * Cater for all the parameters in the script that we want to test.
     * Note that the "hadoop.tmp.dir" is one of the dirs defined by the test harness
     */
    @HiveProperties
    public Map<String, String> hiveProperties = MapUtils.putAll(new HashMap(), new Object[]{
            "MY.HDFS.DIR", "${hadoop.tmp.dir}",
            "my.schema", "bar",
    });

    /**
     * In this example, the scripts under test expects a schema to be already present in hive so
     * we do that with a setup script.
     * <p/>
     * There may be multiple setup scripts but the order of execution is undefined.
     */
    @HiveSetupScript
    private String createSchemaScript = "create schema ${hiveconf:my.schema}";

    /**
     * Create some data in the target directory. Note that the 'targetFile' references the
     * same dir as the create table statement in the script under test.
     * <p/>
     * This example is for defining the data in line as a string.
     */
    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/foo/data_from_string.csv")
    private String dataFromString = "2,World\n3,!";

    /**
     * Create some data in the target directory. Note that the 'targetFile' references the
     * same dir as the create table statement in the script under test.
     * <p/>
     * This example is for defining the data in in a resource file.
     */
    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/foo/data_from_file.csv")
    private File dataFromFile =
            new File(ClassLoader.getSystemResource("helloHiveRunner/hello_hive_runner.csv").getPath());


    /**
     * Define the script files under test. The files will be loaded in the given order.
     * <p/>
     * The HiveRunner instantiate and inject the HiveShell
     */
    @HiveSQL(files = {
            "helloHiveRunner/create_table.sql",
            "helloHiveRunner/create_ctas.sql"}, encoding = "UTF-8")
    private HiveShell hiveShell;


    @Test
    public void testTablesCreated() {
        List<String> expected = Arrays.asList("foo", "foo_prim");
        List<String> actual = hiveShell.executeQuery("show tables");

        Collections.sort(expected);
        Collections.sort(actual);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSelectFromCtas() {
        List<String> expected = Arrays.asList("Hello", "World", "!");
        List<String> actual = hiveShell.executeQuery("select a.s from (select s, i from foo_prim order by i) a");
        Assert.assertEquals(expected, actual);
    }

}
