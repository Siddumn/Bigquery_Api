/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.satwic.join;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

public class DIM_LOCATION_LOAD {

	static int maximum=1;
	static boolean b = true;

	static List<String> list_new = new ArrayList<String>();
	
	public interface TemplateOptions extends DataflowPipelineOptions {

		@Description("Path of the file to read from")
		@Default.String("gs://dataproc-b58bb935-2bfc-481a-8065-cb8376915e76-asia-southeast1/Sheet1.csv")
		ValueProvider<String> getInputFile();

		void setInputFile(ValueProvider<String> value);

		@Description("intermediate table")
		@Default.String("dulcet-cable-92410:sampleSheets.table1$20170101")
		ValueProvider<String> getIntTableName();

		void setIntTableName(ValueProvider<String> value);

		@Description("final staging table")
		@Default.String("dulcet-cable-92410:sampleSheets.table4$20170102")
		ValueProvider<String> getTableName();

		void setTableName(ValueProvider<String> value);
	}

	public static void main(String[] args) {

		TemplateOptions options = PipelineOptionsFactory.create().as(TemplateOptions.class);
		options.setProject("dulcet-cable-92410");
		options.setStagingLocation("gs://dataproc-b58bb935-2bfc-481a-8065-cb8376915e76-asia-southeast1/staging");
		options.setTempLocation("gs://dataproc-b58bb935-2bfc-481a-8065-cb8376915e76-asia-southeast1/staging/temp");

		Pipeline pReadAndWrite = Pipeline.create(options);
		
		PCollection<KV<String, String>> RetailerData =  pReadAndWrite
				.apply(BigQueryIO.readTableRows().from("dulcet-cable-92410:SATWIC_POC_TGT.DIM_RETAILER"))
				.apply("ToMap1", ParDo.of(new DoFn<TableRow, KV<String, String>>(){
					@ProcessElement
					public void processElement(ProcessContext c){
						TableRow t = c.element();
						String keyrow = t.get("COUNTRY_CODE")+","+t.get("RETAILER_NUMBER") ;
						String valrow = (String)t.get("RETAILER_ID");
						c.output(KV.of(keyrow, valrow));
					}
				}));
		PCollectionView<Map<String, String>> Retailer_Map = RetailerData.apply("ToView", View.asMap());
		
		PCollection<String> LocationData =  pReadAndWrite
				.apply(BigQueryIO.readTableRows().from("dulcet-cable-92410:SATWIC_POC_STG.DIM_LOCATION"))
				.apply("ToMap2", ParDo.of(new DoFn<TableRow, String>(){
					@ProcessElement
					public void processElement(ProcessContext c){
						TableRow t = c.element();
						String value = t.get("RETAILER_ID") +","+ t.get("LOCATION_NUMBER") +","+ t.get("COUNTRY_CODE");
						c.output(value);
					}
				}));
		PCollectionView<List<String>> LocationList = LocationData.apply("ToView", View.asList());
		
		PCollection<TableRow> ps = pReadAndWrite
				.apply(BigQueryIO.readTableRows().from("dulcet-cable-92410:SATWIC_POC.Location"))
				.apply("ToLines", ParDo.of(new DoFn<TableRow, TableRow>(){
					@ProcessElement
					public void processElement(ProcessContext c) throws Exception{
						
						TableRow row = c.element();
						
						Map<String, String> map = c.sideInput(Retailer_Map);
						List<String> list = c.sideInput(LocationList);
						
						String ret_cntCode = row.get("Country_Code") +","+ row.get("Retailer_Nbr");
						//System.out.println(map+"::"+ret_cntCode);
						if(map.containsKey(ret_cntCode)){
							
							String retailer_id = map.get(ret_cntCode);
							String location_number = (String) row.get("Location_Nbr");
							String country_code = (String) row.get("Country_Code");
							
							String check_unique = retailer_id + ","+ location_number + "," +country_code;
							if(list.contains(check_unique) || list_new.contains(check_unique)){
								
							}else{
								list_new.add(check_unique);
								System.out.println(list_new);
								String address1 = (String) row.get("Address_1");
								String address2 = (String) row.get("Address_2");
								String zip_code = (String) row.get("Postal_Code");
								String state = (String) row.get("State_Region");
								String country_name = (String) row.get("Country_Name");
								c.output(new TableRow().set("RETAILER_ID", retailer_id).set("LOCATION_NUMBER", location_number)
										.set("COUNTRY_CODE", country_code).set("ADDRESS_1", address1).set("ADDRESS_2", address2)
										.set("ZIP_CODE", zip_code).set("STATE", state).set("COUNTRY_NAME", country_name));
							}
						}else{
							
						}
						
					}
				}).withSideInputs(Retailer_Map).withSideInputs(LocationList));
		
		ps.apply(BigQueryIO.writeTableRows().to("dulcet-cable-92410:SATWIC_POC_STG.DIM_LOCATION")
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

		pReadAndWrite.run();
	}
}