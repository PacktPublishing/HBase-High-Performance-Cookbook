/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public interface HbaseCSVConstantsIntf {
    public static final String INPUT_CSVFILE_LOCATON ="/u/HbaseB/WDI_Country.csv";
    public static final String ORACLE_DRIVER_MANAGER ="oracle.jdbc.OracleDriver";
    public static final String THIN_DRIVER_HOST_PORT="jdbc:oracle:thin:@//myoracle.db.com:1521/xe";
    public static final String ORCALE_USER="scott";
    public static final String ORCALE_PASSWORD="tiger";
    public final String JDBC_INSERT_SQL = "INSERT INTO WDI_COUNTRY" +"( "
            + "(COUNTRY_CODE,SHORT,TABLE_NAME,ALPHA_CODE,CURRENCY_UNIT,SPECIAL_NOTES,REGION,)"
            + "(INCOME_GROUP,INTERNATIONAL_MEMBERSHIPS,WB_2_CODE,NATIONAL_ACCOUNTS_BASE_YEAR,)"
            + "(NATIONAL_ACCOUNTS_REF_YEAR,SNA_PRICE_VALUATION,LENDING_CATEGORY,OTHER_GROUPS,)"
            + "(SYSTEM_OF_NATIONAL_ACCOUNTS,ALTERNATIVE_CONVERSION_FACTOR,PPP_SURVEY_YEAR_BALANCE,)"
            + "(EXTERNAL_DEBT_REPORT_STATUS,SYSTEM_OF_TRADE,GOVERNMENT_ACCT_CONCEPT,IMF_DATA_DISSEMINATION_STD,)"
            + "(LATEST_POPULATION_CENSUS,LATEST_HOUSEHOLD_SURVEY,SOURCE_OF_MOST_RECENT_INCOME,VITAL_REGISTRATION_COMPLETE,)"
            + "(LATEST_AGRICULTURAL_CENSUS,LATEST_INDUSTRIAL_DATA.LATEST_TRADE_DATA,LATEST_WATER_WITHDRAWAL_DATA,)"+")"
            + "VALUES"
            + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    

}
