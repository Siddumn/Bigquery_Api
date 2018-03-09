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

public class DIM_ITEM_LOAD {

	static int maximum=0;
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
				.apply("ToMap", ParDo.of(new DoFn<TableRow, KV<String, String>>(){
					@ProcessElement
					public void processElement(ProcessContext c){
						TableRow t = c.element();
						String keyrow = (String)t.get("RETAILER_NUMBER");
						String valrow = t.get("COUNTRY_CODE") +","+ t.get("RETAILER_ID");
						c.output(KV.of(keyrow, valrow));
					}
				}));
		PCollectionView<Map<String, String>> Retailer_Map = RetailerData.apply("ToView", View.asMap());
		
		PCollection<String> ItemData =  pReadAndWrite
				.apply(BigQueryIO.readTableRows().from("dulcet-cable-92410:SATWIC_POC_STG.DIM_ITEM"))
				.apply("ToList", ParDo.of(new DoFn<TableRow, String>(){
					@ProcessElement
					public void processElement(ProcessContext c){
						TableRow t = c.element();
						String s = t.get("ITEM_NUMBER") +","+ t.get("RETAILER_ID") +","+ t.get("COUNTRY_CODE");
						String str = (String) t.get("DW_ITEM_ID");
						int b = Integer.parseInt(str);
						if(b>maximum){
							maximum=b;
						}else{
							
						}
						c.output(s);
					}
				}));
		PCollectionView<List<String>> Item_List = ItemData.apply("ToView", View.asList());
		
		
		PCollection<TableRow> ps = pReadAndWrite
				.apply(BigQueryIO.readTableRows().from("dulcet-cable-92410:SATWIC_POC.Retailer_Sales"))
				.apply("ToLines", ParDo.of(new DoFn<TableRow, TableRow>(){
					@ProcessElement
					public void processElement(ProcessContext c) throws Exception{
						TableRow row = c.element();
						
						List<String> list = c.sideInput(Item_List);
						//for(int i=0)
						//System.out.println(list.size());
						
						Map<String, String> map = c.sideInput(Retailer_Map);
						Set<String> retailer_numbers = map.keySet();
						/*for(String retailer_number : retailer_numbers){
							System.out.println(retailer_number);
						}*/
						/*while(retailer_number.iterator().hasNext()){
							String key = retailer_number.iterator().next();
							System.out.println(key);
						}*/
						//System.out.println(retailer_number);
						String rowkey = (String) row.get("Retailer_Number");
						if(retailer_numbers.contains(rowkey)){
							
							//String retailerNumber = rowkey;
							
							//unique
							String countryCode = map.get(rowkey).split(",")[0];
							String retailerID = map.get(rowkey).split(",")[1];
							String productNumber = (String) row.get("Product_Number");
							
							String prodDesc = (String) row.get("Product_Desc");
							Date daterepeat = new Date();
							 DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
							   String daterepeatstring = df.format(daterepeat);
							
							String checkUnique = productNumber+","+retailerID+","+countryCode;
							//System.out.println(list.size());
							if(list.contains(checkUnique) || list_new.contains(checkUnique)){
								//update
							}else{
								list_new.add(checkUnique);
								maximum = maximum + 1;
								c.output(new TableRow().set("DW_ITEM_ID", maximum).set("ITEM_NUMBER", productNumber)
										.set("RETAILER_ID", retailerID).set("COUNTRY_CODE", countryCode)
										.set("ITEM_DESCRIPTION", prodDesc).set("UPC_NUMBER", productNumber)
										.set("INSERT_TS", daterepeatstring));
							}
					
						}
						
					}
				}).withSideInputs(Retailer_Map).withSideInputs(Item_List));
		
		ps.apply(BigQueryIO.writeTableRows().to("dulcet-cable-92410:SATWIC_POC_STG.DIM_ITEM")
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

		pReadAndWrite.run();
	}
}