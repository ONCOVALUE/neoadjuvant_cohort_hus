# HUS Neoadjuvant Cohort Codes
All the codes in the documentation are written with PySpark.
## 1. The base neoadjuvant cohort
The following conditions have been used to collect all the neoadjuvant patients. The diagnosis code is a ICD10 code and procedure codes are [NCSP](http://nomesco-eng.nom-nos.dk/) codes.

* Diagnosis condition: Diagnosis code "C50*"
* Neoadjuvant Procedure condition: Procedure code in ```["WB610", "WD325", "WB321", "WB121"]```
* Surgery condition for Diagnoses:
  1. Retrieve post-diagnosis surgeries after a breast cancer diagnosis, identifying surgeries related to breast cancer (procedure_code like ```"(^HAC.)|(^HAB.)|(^HAF.*)"``` )
  2. Breast Cancer Surgery after the first neoadjuvant procedure
* Patient text condition: Post-breast cancer diagnosis reports should mention ```"(?i)neoadj"``` 

The cohort picking logic can be easily modified to correspond to a specific treatment. In HUS, this happens by selecting the patients that have had the specific medications from the medication table.

### 1.1.	Diagnose condition
```python
# Condition parameters
dg_bc             = '^C50'
start             = '2015-01-01 00:00'
end               = '2030-01-01 00:00'

#Wanted variables
wanted_diagnosis_cols = ["ssn", "diagnosis_code", "reported_start_time", "contact_start_time"]
wanted_patient_cols = ["ssn", "primary_patient_number", "secondary_patient_number", "tertiary_patient_number", "gender_description", "date_of_birth", "date_of_death"]

#Conditions
##Breast cancer diagnoses by ICD10 code
cond_bc = (F.col("diagnosis_code").rlike(dg_bc))
##Diagnose's contact time
cond_dg_time = (F.col("contact_start_time").between(start, end))

#Join diagnose and patient tables and filter the rows by diagnosis code and time
diagnosis_cohort_tmp = (diagnose.select(wanted_dg)
                    .join(patient.select(wanted_pat), on = "ssn", how = "left")
                    .filter(cond_bc & cond_dg_time)
                   )

#Window funtion conditions
##The first reported time for the breast cancer diagnose
wind_first_bc_dg_reported = Window.partitionBy("ssn").orderBy(F.col("reported_start_time").asc_nulls_last())
##The first contact time for the breast cancer diagnose
wind_first_bc_dg_contact = Window.partitionBy("ssn").orderBy(F.col("contact_start_time").asc_nulls_last())

#Select the first reported breast cancer diagnose time for each patient
first_bc_dg_hus_ilmoitettu = (diagnosis_cohort_tmp
                              .withColumn("row", F.row_number().over(wind_first_bc_dg_reported))
                              .filter(F.col("row") == F.lit(1))
                              .select("ssn", "reported_start_time", "diagnosis_code"))

#Select the first contact time for breast cancer diagnose time for each patient
first_bc_dg_hus_kontakti = (diagnosis_cohort_tmp
                            .withColumn("row", F.row_number().over(wind_first_bc_dg_contact))
                            .filter(F.col("row") == F.lit(1))
                            .select("ssn", "contact_start_time", "diagnosis_code"))

diagnosis_cohort = (first_bc_dg_hus_ilmoitettu
                    .join(first_bc_dg_hus_kontakti, on = ["ssn", "diagnosis_code"], how = "full")
                    .selectExpr("ssn", "reported_start_time as first_reported_start_time", "contact_start_time as first_contact_start_time", "diagnosis_code as first_diagnosis_code"))

```

### 1.2.	Neoadjuvant procedure condition
```python
#Neoadjuvant procedure codes
neoadj_proc = ["WB610", "WD325", "WB321", "WB121"]

wanted_dg_cols = ["ssn", "procedure_code", "procedure_description", "procedure_time"]
#Condition for neoadjuvan procedure
cond_neoadj_proc = F.col("procedure_code").isin(neoadj_proc)

#Window function definition for the first neoadjuvant procedure time
wind_neoadj = Window.partitionBy("ssn").orderBy(F.col('procedure_time').asc())

#Select the neoadjuvant procedure rows
neoadj_proc_tmp = (procedures
                    .select(wanted_dg_cols)
                    .filter(cond_neoadj_proc)
                    )

#Select the first neoadjuvant procedure and join with the rest neoadjuvat procedures
neoadj_proc_cohort = (neoadj_proc_tmp
                            .withColumn("row_num", F.row_number().over(wind_neoadj))
                            .filter(F.col("row_num")==1)
                            .selectExpr("ssn", “procedure_time as first_neoadj_procedure_time”)
                            .join(neoadj_proc_tmp, "ssn")
                            )
```
### 1.3.	Combine the diagnose and neoadjuvant cohorts

```python
#Condition for the procedure date: neoadjuvant procedure has to be after the breast cancer diagnosis
cond_neoadj_post_bc_dg = F.to_date("procedure_time") >= F.to_date("first_bc_start_time")

#Primarly the reported diagnosis date is use, the contact date for the diagnosis is the secondary option 
cohort_bc_neoadj_all = (diagnosis_cohort
                        .join(neoadj_proc_cohort , on = "ssn")
                        .withColumn("first_bc_start_time", 
                            F.when((F.col("first_reported_start_time") <= F.col("first_contact_start_time")) & (F.col("first_reported_start_time").isNotNull()), (F.col("first_reported_start_time"))).otherwise(F.col("first_contact_start_time"))
                        )
                        .filter(cond_neoadj_post_bc_dg)
                    )
```
### 1.4.	Surgery condition

```python
#Operation columns
operation_wanted = ["ssn", "main_procedure", "operation_date", "operation_id", "procedure_description", "procedure_exta_codes", "procedure_laterality_description", "procedure_code"]

#Condition for operation time: operation time has to be after the first breas cancer diagnosis
cond_operation_time = F.to_date("operation_date") >= F.to_date("first_bc_start_time")
#The operation has to be breast cancer operation
cond_bc_operation_code = F.col("procedure_code").rlike("(^HAC.*)|(^HAB.*)|(^HAF.*)")

#Join the operation table with cohort and use the time filtering
operatio_tmp = (operation
                .select(operation_wanted)
                .join(neoadj_cohort_sis_kesken.select("ssn", "first_bc_start_time").distinct(), on = "ssn", how = "inner")
                .withColumn("bc_operation", F.when(cond_bc_operation_code, True).otherwise(False))
                .filter(cond_operation_time)                
                )
```
### 1.5.	Combine the surgeries and the neoadjuvant cohort

```python
#Conditions for filtering out the patients who have not had the breast surgery after the first neoadjuvant procedure. More detailed filtering is done later.
cond_neoadj_pre_op = F.to_date("operation_date") >= F.to_date("first_neoadj_procedure_time")
cond_bc_op = F.col("bc_operation") == True

#Filtering off the patients that have not had a breast surgery after the neoadjuvant treatment
cohort_bc_neoadj = (cohort_bc_neoadj_all
                 .join((operatio_tmp
                       .filter(cond_bc_op)
                       .select("ssn", "operation_date")
                       .distinct()) , "ssn", "inner"
                 )
                 .filter(cond_neoadj_pre_op)
                 )
```

The code blocks 1.-5. give us the basic neoadjuvant cohort. However, at least in HUS, this cohort still includes some patients that do not have any mention about the neoadjuvant treatments in their patient texts or the preoperative pathological results are missing. In this point we decided that we excluded all the patient cases where there was no mention about the neoadjuvant treatment in the patient’s texts (exclusion in code block 6.) and we manually investigated the cases where the preoperative pathology was missing. The number of these unusual patients was relatively small. 

### 1.6.	Patient text condition
```python
#condition for the event time: text is written after six months before the first breast cancer diagnosis
cond_text_time = F.to_date("event_time") >= F.add_months("first_bc_start_time",-6)
#The text's length is more than zero
cond_text_not_null = F.col("text").isNotNull()
cond_neoadjuvant_sentences = F.size("neoadjuvant_sentences") > 0

wanted_text_cols = ["ssn", "view", "event_time", "update_time_text", "text"]

#Join the patient texts (incl. texts from visits, radiology, pathology, labs and general texts) and the cohort
## Clean the texts and filter the rows including string (case-insenitive match) "neoadj" that have written after the first breast cancer diagnosis
texts_cohort = (pat_texts
                .select(wanted_text_cols)
                .join(neoadj_cohort_sis_adj.select("ssn", "first_bc_start_time").distinct(), on = "ssn", how = "inner")
                .withColumn('text',F.regexp_replace('text','\\\\n','\n'))
                .withColumn('text',F.regexp_replace('text','([^\n])(\n)([^\n])',r'$1 $3'))
                .withColumn("neoadjuvant_sentences", extract_sentences(F.col("text"), "(?i)neoadj"))
                .filter(cond_text_time & cond_text_not_null)
                )

#Select the patients in the cohort that have "neoadj" mentioned in their textrs
neoadj_cohort = (cohort_bc_neoadj
                 .join(pat_texts
                       .filter(cond_neoadjuvant_sentences)
                       , "ssn"
                       , "leftsemi"
                       )
                 )
```
## 2. Join the cohort to the relevant data
I have not explicitly written here the codes for the joins between the wanted datasources and the cohort because these are system specific. In each case, I have done ```leftsemi```-join with respect to the social security number, filtered out the data that have date before the first breast cancer diagnsosis, and selected the relevant variables. Here is the list from the tables:
```python
#Cohort table created above
neoadj_cohort
#Demography
patient_cohort
#Surgeries
surgery_cohort
#Patient texts
patient_text_cohort
#Pahtology diagnosis
pathology_dg_cohort
#Medicines
medicine_cohort
#Labs
lab_cohort
#Procedures
procedure_cohort
#Diagnosis
diagnosis_cohort
```

## 3. Logic for pre-operational variables
The logic for the pre-operational data pick has the following outline:

1.	Do the left join between the pathology diagnosis of the cohort and the neoadjuvant procedures and the breast cancer operations. Do the following steps for finding the preoperative pathology samplenumbers:
    * Both for the breasts and lymph nodes
    * Remove the surgical specimens
    * Select the cases where the pathological sample has arrived before some neoadjuvant treatment that are before some breast surgery
2. Extract the needed data from the pathological texts, including sample numbers, grade, and tumor histologies.
3. Join the pathology diagnosis to the cohort that refer to the original pathological sample.
   * In many cases the sample identifier can change between the examinations even if the original sample would be the same. However, in these cases, the original sample identifier is mentioned in the text. For that reason we are extracting the sample numbers from a text and use tose in the join. 
   * Set a side for a manual examination those cases where the preoperative diagnosis cannot be found.
 4. Create timeline for each patient, including the pathological results, neoadjuvant treatments, and  breast cancer operations. These events are used to create a neoadjuvant treatment episode that starts from the core needle biopsy, incldes neoadjuvant treatments, and ends to a breast surgery. Here the groupping variabes are the social security number for a patient and the breast (left or right). 
    * This episode identifier is then used for collecting the relevant pre-operational variables.
    * Filter out all the episodes that do not include neoadjuvant treatments.
5. Collect the relevant variables.

### 3.1. Preop pathology samplenumbers

Before using these codes, check that these regexes work with your data. In HUS, these texts are originally in Finnish, but I have changed them in English so that the codes are easier to understand.
```python
#Organ condition: breast
cond_organ_breast = F.col("organ").rlike("(?i)breast")
#Organ condition: lymph node
cond_organ_lymph = F.col("organ").rlike("(?i)lymph")
#Diagnosis condition: pathology diagnose is carsinoma
cond_dg_carcinoma = F.col("diagnose").rlike("(?i)carcinoma")
#Condition for surgical specimen: this is used to remove surgical specimens
cond_surgical_specimen = F.col("examinations").rlike("PAD-[345]")
#Breast cancer operation condition
cond_bc_operation = F.col("bc_operation") == True
#Neoadjuvant treatment order condition: this is an initial filtering for selecting the cases 
#where the pathological sample has arrived before some neoadjuvant treatment that is before some breast cancer treatment
# The more detailed episode filtering will be done below with the timeline
cond_neoadj_order = ((F.to_date("arrived") <= F.to_date("procedure_time")) 
                     & (F.to_date("procedure_time") <= F.to_date("operation_date")))

#This dataframe consists of surgical specimens that filtered out from pre-operational pathologies
surgical_specimen_pathology_samplenumber = (pathology_dg_cohort.filter(cond_surgical_specimen))

#Remove surgical specimens from pathology diagnosis with leftanti
#Filter the rows that include "(?i)carcinoma" in their diagnosis and the organ is breast
#Join the procedure and surgery dataframes so that it is possible to filter the cohort so that
#the pathological sample has arrived before some neoadjuvant treatment that is before some breast cancer treatment
breast_preop_pad_pathology_samplenumber = (pathology_dg_cohort
                                           .join(surgical_specimen_pathology_samplenumber, "arrived", "leftanti")
                                           .filter(cond_dg_carcinoma & cond_organ_breast)
                                           .join(procedure_cohort
                                                 .filter(cond_neoadj_proc)
                                                 , "ssn"
                                                 , "left"
                                                 )
                                           .join(surgery_cohort
                                                 .filter(cond_bc_operation)
                                                 , "ssn"
                                                 , "left"
                                                 )
                                           .filter(cond_neoadj_order)
                                           .select("ssn", "pathology_samplenumber")
                                          )

#The same logic as for breasts above but now for lymph nodes
lymph_preop_pad_pathology_samplenumber = (pathology_dg_cohort
                                          .join(surgical_specimen_pathology_samplenumber, "arrived", "leftanti")
                                          .filter(cond_dg_carcinoma & cond_organ_lymph)
                                          .join(procedure_cohort
                                                .filter(cond_neoadj_proc)
                                                , "ssn"
                                                , "left"
                                                )
                                          .join(surgery_cohort
                                                .filter(cond_bc_operation)
                                                , "ssn"
                                                , "left"
                                                )
                                          .filter(cond_neoadj_order)
                                          .select("ssn", "pathology_samplenumber")
                                          )

```
Some part of the patients in HUS that were part of the original cohort, did not have preoperative pathology diagnosis that would have been found with this logic: those patients were ecluded from the cohort in this point and moved to a cohort that is manually qualified later.  

### 3.2. Preop pathology text extractions
The following sentences are extracted in this  point:
* samplenumbers
* tumor grade
* histologies
* multifocality

These extractions are done to the main patology diagnosis table so that they can be used both for breasts and lymph nodes. In the cases of ductal, lobular and mulifocality also the negation sentences are tried to be recognized and extracted. 

Samplenumbers are extracted for collecting all the pathology reports that are refetting to the original sample: this is because the samplenumber can change between the studies, even if the examined sample is the same. 

```python
#The multifocality has been written in a standard way in HUS already before the form has been stucturized. The conditions for multifocality are written in the following way. 
cond_multifoc_true =  F.col("statement").rlike("Multifocality\s+Yes")
cond_multifoc_false =  F.col("statement").rlike("Multifocality\s+No")

#extracted_samplenumbers: regex that maches to the pathology system's sample number structure
#extracted_grade_text: Regex for picking the grade from pathology texts. Mind that this works just in Finnish. In the next row the distinct values are collected.
#extracted_grade_dg: Regex for picking the grade from pathology diagnosis. Mind that this works just in Finnish. In the next row the distinct values are collected.
#Regexes for different histologies that are partly collected from the pathology texts. "extract_sentences" is our own code for picking the whole sentence(s) around the
#regex phrase, but in this case this is done for only validation purpouses. These new columns are used only as indicators that tell if the word in search is mentioned.
#Notion that in "histology_senteneces_dukt_neg" and "histology_senteneces_lobul_neg" we try to catch if the sentence is negative.
#The list of histologies might shrink (or expand) in the future.
#Multifocality indicated in the texts are extracted also here. 
pathology_dg_cohort = (pathology_dg_cohort
                        .withColumn("extracted_samplenumbers", F.expr(r"regexp_extract_all(statement, '((\\w{3}\\d{9})|(\\w{2}\\d{2}\\-\\d{3,5}))', 1)"))
                        .withColumn("extracted_grade_text", F.expr(r"regexp_extract_all(statement, '(?i)(grade|erilaistu\\S+|\\W+gr\\s+|\\W+g)\\s*([1-3](\\-g?[1-3])?)', 2)"))
                        .withColumn("extracted_grade_text", F.array_distinct(F.col("extracted_grade_text")))
                        .withColumn("extracted_grade_dg", F.expr(r"regexp_extract_all(diagnosesuffix, '(?i)(grade|erilaistu\\S+|gr\\s+|g)\\s*([1-3](\\-g?[1-3])?)', 2)"))
                        .withColumn("extracted_grade_dg", F.array_distinct(F.col("extracted_grade_dg")))
                        .withColumn("histology_senteneces_dukt", extract_sentences(F.col("statement"), "(?i)du[kc]t[^u]"))
                        .withColumn("histology_senteneces_lobul", extract_sentences(F.col("statement"), "(?i)lobul[^u]"))
                        .withColumn("histology_senteneces_dukt_neg", extract_sentences(F.col("statement"), "(?i)ei\s+(\S+\s+)?(\S+\s+)?du[kc]t[^u]"))
                        .withColumn("histology_senteneces_lobul_neg", extract_sentences(F.col("statement"), "(?i)ei\s+(\S+\s+)?(\S+\s+)?lobul[^u]"))
                        .withColumn("histology_senteneces_musinoot", extract_sentences(F.col("statement"), "(?i)(?<!\bin situ\s)mu[sc]ino(?<!\bin situ\b)"))
                        .withColumn("histology_senteneces_micropapill", extract_sentences(F.col("statement"), "(?i)micropapill"))
                        .withColumn("histology_senteneces_tubulaar", extract_sentences(F.col("statement"), "(?i)tubula(a)?r"))
                        .withColumn("histology_senteneces_metaplast", extract_sentences(F.col("statement"), "(?i)metaplast"))
                        .withColumn("histology_senteneces_tubulolob", extract_sentences(F.col("statement"), "(?i)tubulolob"))
                        .withColumn("histology_senteneces_kribriform", extract_sentences(F.col("statement"), "(?i)[kc]ribriform"))
                        .withColumn("histology_senteneces_neuroend", extract_sentences(F.col("statement"), "(?i)neuroend"))
                        .withColumn("histology_senteneces_apocrin", extract_sentences(F.col("statement"), "(?i)apo[ck]ri(i)?n"))
                        .withColumn("histology_senteneces_inflamm", extract_sentences(F.col("statement"), "(?i)inflam(m)?ato"))
                        .withColumn("histology_senteneces_dukt_in_situ", extract_sentences(F.col("statement"), "((?i)du[kc]t[^u]*\s+in\situ)|DCIS"))
                        .withColumn("histology_senteneces_lobul_in_situ", extract_sentences(F.col("statement"), "((?i)lobul[^u]*\s+in\situ)|LCIS"))
                        .withColumn("multifoc_text_tmp", F.when(cond_multifoc_true, True).when(cond_multifoc_false, False).otherwise(None))
                      )


```

### 3.3. Join the pathology diagnosis to the cohort that refer to the original pathological sample.

```python
#Condition for the leftsemi-join (leftsemi leaves the matching rows in the left table)
#Either the SSNs and pathology samplenumbers directly match 
#OR SSN matches AND the samplenumber is referred in the patient text (these samplenumbers are extracted from the text above)
cond_pad_preop_breast_join = (((pathology_dg_cohort.ssn == breast_preop_pad_pathology_samplenumber.ssn)
                              & (pathology_dg_cohort.pathology_samplenumber == breast_preop_pad_pathology_samplenumber.pathology_samplenumber)
                              )
                              | ((pathology_dg_cohort.ssn == breast_preop_pad_pathology_samplenumber.sn)
                                 & (F.array_contains(pathology_dg_cohort.extracted_samplenumbers, breast_preop_pad_pathology_samplenumber.pathology_samplenumber))
                                 )
                             )

#The same logic for the lymph nodes
cond_pad_preop_lymph_join = (((pathology_dg_cohort.ssn == imusolmuke_preop_pad_pathology_samplenumber.ssn)
                                   & (pathology_dg_cohort.pathology_samplenumber == imusolmuke_preop_pad_pathology_samplenumber.pathology_samplenumber)
                                   )
                                   | ((pathology_dg_cohort.ssn == imusolmuke_preop_pad_pathology_samplenumber.ssn)
                                      & (F.array_contains(pathology_dg_cohort.extracted_samplenumbers, imusolmuke_preop_pad_pathology_samplenumber.pathology_samplenumber))
                                      )
                                  )

#Join itself
#Remove non-breast rows
pad_preop_breast = (pathology_dg_cohort
                  .join(breast_preop_pad_pathology_samplenumber, cond_pad_preop_breast_join, "leftsemi")
                  .filter(cond_organ_breast)
                  )

#Join itself
#Remove non-lymph node rows
pad_preop_lymp_node = (pathology_dg_cohort
                        .join(lymph_preop_pad_pathology_samplenumber, cond_pad_preop_lymph_join, "leftsemi")
                        .filter(cond_organ_lymph)
                        )

```
### 3.4. Timeline for each patient: Creatinng the episode identifier
Timeline created for each patient in the cohort. Events included in the timeline: 
* pre-operational  breast and lymphnode pathologies
* neoadjuvant procedures
* breast cancer operations.

Here we also create the episode identifier that starts from a biopsy and ends to a breast cancer surgery.

```python
#Codition that a list of events contains at least one neoadjuvant procedure
cond_contains_neoadj_episode = F.array_contains(F.col("events"), "neoadjuvant_procedure")
#Codition that a event is a neoadjuvant procedure
cond_neoadj_event = F.col("event")=="neoadjuvant_procedure"
#Codition that a event is a breast cancer surgery
cond_bc_op_event = F.col("event")=="breast_cancer_operation"

#This window definition is used to order the events by their dates, partitioned by ssn
wind_cumsum = Window().partitionBy("ssn").orderBy(F.col("date").asc())
#This window definition is used to find the start date for each episode
wind_episode = Window().partitionBy("ssn","episode_id").orderBy(F.col("date").asc())

#Pathology events both from breast and lymph node to event timeline
time_pat = (pad_preop_breast
            .union(pad_preop_lymp_node)
            .withColumn("event", F.lit("pathology"))
            .withColumn("date", F.to_date("arrived"))
            .selectExpr("ssn", "date", "event", "pathology_samplenumber as id", "diagnose as other")
            .distinct()
            )

#Breast cancer surgeries to event timeline
time_bc_op = (surgery_cohort
              .filter(cond_bc_operation)
              .withColumn("event", F.lit("breast_cancer_operation"))
              .withColumn("other", F.lit(None))
              .selectExpr("ssn", "operation_date as date", "event", "operation_id as id", "other")
              .distinct()
              )

#Neoadjuvant procedures to event timeline
time_neoadj_proc = (procedure_cohort
                   .filter(cond_neoadj_proc)
                   .withColumn("event", F.lit("neoadjuvant_procedure"))
                   .withColumn("date", F.to_date("procedure_time"))
                   .selectExpr("ssn", "date", "event", "procedure_id as id", "toimenpide_description as other")
                   .distinct()
                   )

#Union the pathology, surgery and neoadjuvant dataframes
#Create a column "value", that first gets value 1 if the event is breast cacner surgery, and otherwise 0
#Column value is then used for creating a new column "episode_id"
#"episode_id" presents an episode starting from biopsy and ending to breast cancer surgery
#This variable is created by cumming over the "value" column so that in each breast cancer surgery the id increases by 1
#In the end, we decrease each breast cancer surgery rows by one so that they are included in the previous episode
timeline_pat_neoadj_op_tmp = (time_pat
                              .union(time_bc_op)
                              .union(time_neoadj_proc)
                              .withColumn("value", F.when(cond_bc_op_event, F.lit(1)).otherwise(F.lit(0)))
                              .withColumn("episode_id", F.sum("value").over(wind_cumsum))
                              .withColumn("episode_id", F.when(cond_bc_op_event, F.col("episode_id")-1).otherwise(F.col("episode_id")))
                              )

#Here we collect all the events in each episode. With those, we check which of the episodes include neoadjuvant events (variable "neoadjuvant_episode")
neoadjuvant_episodes = (timeline_pat_neoadj_op_tmp
                        .groupBy("ssn", "episode_id")
                        .agg(F.collect_set("event").alias("events"))
                        .withColumn("neoadjuvant_episode", F.when(cond_contains_neoadj_episode, True).otherwise(False))
                        .select("ssn", "episode_id", "neoadjuvant_episode")
                        )

#Here we collect the starting days to each episode
episode_start_date = (timeline_pat_neoadj_op_tmp
                  .withColumn("rownum", F.row_number().over(wind_episode))
                  .filter(F.col("rownum")==F.lit(1))
                  .selectExpr("ssn","episode_id","date as episode_start_date")
                  )
#Here we collect the ending days to each episode (by definition each episode ends to a breast cancer operation)
episode_end_date = (timeline_pat_neoadj_op_tmp
                  .filter(cond_bc_op_event)
                  .selectExpr("ssn","episode_id","date as episode_end_date")
                 )

#Here we collect the first neoadjuvant event date in each episode
neoadj_start_date = (timeline_pat_neoadj_op_tmp
                  .filter(cond_neoadj_event)
                  .withColumn("rownum", F.row_number().over(wind_episode))
                  .filter(F.col("rownum")==F.lit(1))
                  .selectExpr("ssn","event_id","date as neoadj_start_date")
                  )

#Combine the start and end dates
episode_start_end_date = (episode_start_date
                         .join(episode_end_date, ["ssn", "event_id"], "left")
                         )

#Join all the dateframes created above to the timeline
timeline_pat_neoadj_op = (timeline_pat_neoadj_op_tmp
                          .join(neoadjuvant_episodes, ["ssn", "event_id"])
                          .join(episode_start_end_date, ["ssn", "event_id"],"left")
                          .join(neoadj_start_date, ["ssn", "event_id"],"left")
                          .sort("ssn", F.col("date").asc())
                          )

```

### 3.4. Join the neoadjuvant episode information to the pathology diagnosis

Join is separately done both for the breast and lymph node pathologies. Here we also filter out the non-neoadjuvant treatment episodes.

```python
#Condition for filtering the neoadjuvant episodes
cond_neoadj_episode = F.col("neoadjuvant_episode")==True
#Condition for filtering the pathologies that are taken before the begining of neoadjuvant treatments
cond_preop_pat_before_neoadj = F.to_date("arrived") < F.to_date("neoadj_start_date")

#Join between pre-operational pathologies and timeline variables
#Filtering only cases that are neoadjuvant episodes and where the biopsy has arrived before start of neoadjuvant treatments
pad_preop_breast = (pad_preop_breast
                   .join(timeline_pat_neoadj_op
                         .filter(cond_neoadj_episode)
                         .selectExpr("ssn", "id as pathology_samplenumber", "episode_id", "episode_start_date", "episode_end_date", "neoadj_start_date")
                         .distinct()
                         , ["ssn", "pathology_samplenumber"]
                         , "inner")
                   .filter(cond_preop_pat_before_neoadj)
                   )

#Same joins and filtering for the lymph nodes 
pad_preop_lymp_node = (pad_preop_lymp_node
                   .join(timeline_pat_neoadj_op
                         .filter(cond_neoadj_episode)
                         .selectExpr("ssn", "id as pathology_samplenumber", "episode_id", "episode_start_date", "episode_end_date", "neoadj_start_date")
                         .distinct()
                         , ["ssn", "pathology_samplenumber"]
                         , "inner")
                   .filter(cond_preop_pat_before_neoadj)
                   )

```
In this point we have created the base for the variable collection: we have the neoadjuvant episodes and pre-treatment pathologies. In the following paragraphs the idea is to collect indepentent dataframes that consist of the wanted variables that can be joined in the end with respect to the ```["ssn", "organ", "episode_id"]```. This combined dataframe then represents the pre-operative neoadjuvant variables. 

### 3.5. Pre-treatment histology
In the case of HUS, we do not have one complete and realiable source for histologies, so we have to combine information from multiple sources: pathology diagnosis (extracted in 3.2), pathology statements (extracted in 3.2) and ICD10 diagnosis codes (extracted here). 

At firts we extract the histology from ICD10 diagnosis code. The second decimal number gives the histology. In this part, we are only interested if the diagnosis code is 1 (ductal) or 2 (lobular).

The following function can be used for collecting the extracted histology:

```python
#String list column of potential histologies are given as an input.
def lists_collect_histology(str_list):

#String list must have some elements
  if len(str_list)>0:
    #We are pnöy interested here if the code is 1 (ductal) or 2 (lobular)  
    hist_list = [hist for hist in str_list if hist in ["1","2"]]

    #Move forward only if there are some histologies  
    if len(hist_list)>0:
      #Distinct elements in the list
      hist_list = list(set(hist_list))
      return hist_list
      #Otherwise the funtion returns "None"
    else:
      return None

  else:
    return None

#PySpark spesific UDF specification
lists_collect_histology_udf = F.udf(lists_collect_histology, ArrayType(StringType()))
```
In the following snippet the diagnosis are collected.

```python
#Pre-operative pathology data is joined with diagnosis
#Filter the rows where the contact has started after or on the same date as episode has started (pathology sample has arrived)
#and filter the rows where the contact has started before the episode's neoadjuvant treatments have began
#Filter the breast cancer diagnosis
#Extract the second secimal from the ICD10 code 
#Collect the set of the diagnosis codes
#Collect the histolofies by using the lists_collect_histology -function
histology_diagnosis = (pad_preop_breast
                        .join(diagnosis_cohort, "ssn", "left")
                        .filter((F.to_date("contact_start_time") >= F.to_date("episode_start_date")) & (F.to_date("contact_start_time") <= F.to_date("neoadj_start_date")) & (F.col("diagnosis_code").rlike("^C50")))
                        .withColumn("histology_diagnosis_tmp", F.regexp_extract("diagnosis_code", "C50\.\d{1}(\d{1})", 1))
                        .groupBy("ssn", "episode_id", "organ")
                        .agg(F.collect_set("histology_diagnosis_tmp").alias("histology_diagnosis_tmp"))
                        .withColumn("histology_diagnosis", lists_collect_histology_udf("histology_diagnosis_tmp"))
                        .select("ssn", "episode_id", "organ", "histology_diagnosis")
                        )

```
In the following snippet we collect the histology columns (extracted in 3.2) from the pathology diagnosis dataframe. 

```python
#wanted columns
wanted_pad_preop_breast_histologia = ["ssn"
                                     , "arrived"
                                     , "pathology_samplenumber"
                                     , "extracted_samplenumbers"
                                     , "diagnose"
                                     , "organ"
                                     , "episode_id"
                                     , "histology_sentences_dukt"
                                     , "histology_sentences_lobul"
                                     , "histology_sentences_dukt_neg"
                                     , "histology_sentences_lobul_neg"
                                     , 'histology_sentences_musinoot'
                                     , 'histology_sentences_papill'
                                     , 'histology_sentences_tubulaar'
                                     , 'histology_sentences_metaplast'
                                     , 'histology_sentences_tubulolob'
                                     , 'histology_sentences_kribriform'
                                     , 'histology_sentences_neuroend'
                                     , 'histology_sentences_apocrin'
                                     , 'histology_sentences_inflamm'
                                     , 'histology_sentences_dukt_in_situ'
                                     , 'histology_sentences_lobul_in_situ']

#Filtering the breast carsinoma diagnosis rows (these rows include the histology if it it mentioned) 
pad_preop_breast_histologia = (pad_preop_breast
                        .filter(cond_dg_carcinoma & cond_organ_breast)
                        .select(wanted_pad_preop_breast_histologia)
                        .distinct()
                        )

```
The following function has been used for parsing the "list of lists" -type column to a one set of sentences.

```python
#"list of lists" type column as an input
#This function is used to loop through all the lists in a list and return a set of number
def list_of_lists2set(list_of_lists_col):
  
  list_tmp = []
  
  for i in list_of_lists_col:
    for j in i:
      list_tmp.append(j)

  #Returns distinct sentences    
  return list(set(list_tmp))

#PySpark specific transform to UDF
list_of_lists2set_udf = F.udf(list_of_lists2set, ArrayType(StringType()))
```
In the following snippet the idea is to collect all the histologies as sets with respect to each combination of ```["ssn", "organ" and ."episode_id"]```. At first, the histologies are collected from the ```"diagnose"``` column, and after that all the columns with extracted histology sentences are collected. The list of histologies that we are collecting can still change from this.
```python
#Collected histologies from pahthology's "diagnose" column.
#Join the previous histology_diagnosis dataframe
pad_preop_breast_histology_collected_tmp = (pad_preop_breast_histology
                                        .groupBy("ssn", "organ", "episode_id")
                                        .agg(F.collect_set("diagnose").alias("Carcinoma_histology"))
                                        .join(histology_diagnosis, ["ssn", "organ", "episode_id"], "left")
                                        )

#Correspondingly here we collect all the extracted sentences and tranfer them to sets
pad_preop_breast_histology_sentences_collected = (pad_preop_breast_histology
                                        .groupBy("ssn", "organ", "episode_id")
                                        .agg(F.collect_set("histology_sentences_dukt").alias("histology_sentences_dukt")
                                             , F.collect_set("histology_sentences_lobul").alias("histology_sentences_lobul")
                                             , F.collect_set("histology_sentences_dukt_neg").alias("histology_sentences_dukt_neg")
                                             , F.collect_set("histology_sentences_lobul_neg").alias("histology_sentences_lobul_neg")
                                             , F.collect_set("histology_sentences_musinoot").alias("histology_sentences_musinoot")
                                             , F.collect_set("histology_sentences_papill").alias("histology_sentences_papill")
                                             , F.collect_set("histology_sentences_tubulaar").alias("histology_sentences_tubulaar")
                                             , F.collect_set("histology_sentences_metaplast").alias("histology_sentences_metaplast")
                                             , F.collect_set("histology_sentences_tubulolob").alias("histology_sentences_tubulolob")
                                             , F.collect_set("histology_sentences_kribriform").alias("histology_sentences_kribriform")
                                             , F.collect_set("histology_sentences_neuroend").alias("histology_sentences_neuroend")
                                             , F.collect_set("histology_sentences_apocrin").alias("histology_sentences_apocrin")
                                             , F.collect_set("histology_sentences_inflamm").alias("histology_sentences_inflamm")
                                             , F.collect_set("histology_sentences_dukt_in_situ").alias("histology_sentences_dukt_in_situ")
                                             , F.collect_set("histology_sentences_lobul_in_situ").alias("histology_sentences_lobul_in_situ")
                                             )
                                        .withColumn("histology_sentences_dukt", list_of_lists2set_udf("histology_sentences_dukt"))
                                        .withColumn("histology_sentences_lobul", list_of_lists2set_udf("histology_sentences_lobul"))
                                        .withColumn("histology_sentences_dukt_neg", list_of_lists2set_udf("histology_sentences_dukt_neg"))
                                        .withColumn("histology_sentences_lobul_neg", list_of_lists2set_udf("histology_sentences_lobul_neg"))
                                        .withColumn("histology_sentences_musinoot", list_of_lists2set_udf("histology_sentences_musinoot"))
                                        .withColumn("histology_sentences_papill", list_of_lists2set_udf("histology_sentences_papill"))
                                        .withColumn("histology_sentences_tubulaar", list_of_lists2set_udf("histology_sentences_tubulaar"))
                                        .withColumn("histology_sentences_metaplast", list_of_lists2set_udf("histology_sentences_metaplast"))
                                        .withColumn("histology_sentences_kribriform", list_of_lists2set_udf("histology_sentences_kribriform"))
                                        .withColumn("histology_sentences_neuroend", list_of_lists2set_udf("histology_sentences_neuroend"))
                                        .withColumn("histology_sentences_apocrin", list_of_lists2set_udf("histology_sentences_apocrin"))
                                        .withColumn("histology_sentences_inflamm", list_of_lists2set_udf("histology_sentences_inflamm"))
                                        .withColumn("histology_sentences_dukt_in_situ", list_of_lists2set_udf("histology_sentences_dukt_in_situ"))
                                        .withColumn("histology_sentences_lobul_in_situ", list_of_lists2set_udf("histology_sentences_lobul_in_situ"))
                                        )

#Join the sentences to the histology dataframe
pad_preop_breast_histology_collected_tmp = (pad_preop_breast_histology_collected_tmp
                                          .join(pad_preop_breast_histology_sentences_collected
                                          , ["ssn", "organ", "episode_id"]
                                          , "left")
                                          )


```
The actual logic for derivating the pre-operational histology status is described in the following snippet. _This logic is HUS spesific and cannot be directly applied elsewhere_. Here we had to use this more complex logic because there were no single reliable and complete data source: however, some sources were more reliable than others (e.g. pathology diagnose is more realiable and granual source for pre-treatment histology status than ICD10 diagnose code). The reported histologies can still change.

```python
#Is duktal mentioned in some text
cond_duct_text = F.size("histology_lauseet_dukt")>0
#Is lobular mentioned in some text
cond_lobul_text = F.size("histology_lauseet_lobul")>0
#Is duktal mentioned in negation in some text
cond_duct_neg_text = F.size("histology_lauseet_dukt_neg")>0
#Is lobular mentioned in negation in some text
cond_lobul_neg_text = F.size("histology_lauseet_lobul_neg")>0
#Is some other (listed) histology mentioned in some text 
cond_muut_text = F.size("histology_lauseet_muut")>0
#Is duktal mentioned in some diagnosis code
cond_duct_dg = F.array_contains(F.col("histology_diagnoosi"),"1")
#Is lobular mentioned in some diagnosis code
cond_lobul_dg = F.array_contains(F.col("histology_diagnoosi"),"2")
#Is duktal mentioned in some pathology diagnosis
cond_duct_pat = F.array_contains(F.col("Carcinoma_histology"),"Carcinoma ductale")
#Is lobular mentioned in some pathology diagnosis
cond_lobul_pat = F.array_contains(F.col("Carcinoma_histology"),"Carcinoma lobulare")
#Is some non-specific carsinoma mentioned in some pathology diagnosis
cond_nonspecific_pat = F.array_contains(F.col("Carcinoma_histology"),"Carcinoma")

#Here is a list of conditions of other possible pathologies that could have been mentioned in the pathology diagnosis
cond_papillare_pat = F.array_contains(F.col("Carcinoma_histology"),"Carcinoma papillare")
cond_metaplasticum_pat = F.array_contains(F.col("Carcinoma_histology"),"Carcinoma metaplasticum")
cond_tubulare_pat = F.array_contains(F.col("Carcinoma_histology"),"Carcinoma tubulare")
cond_apocrinum_pat = F.array_contains(F.col("Carcinoma_histology"),"Carcinoma apocrinum")
cond_inflammatoricum_pat = F.array_contains(F.col("Carcinoma_histology"),"Carcinoma inflammatoricum")
cond_micropapillare_invasivum_pat = F.array_contains(F.col("Carcinoma_histology"),"Carcinoma micropapillare invasivum")
cond_neuroendocrinum_pat = F.array_contains(F.col("Carcinoma_histology"),"Carcinoma neuroendocrinum")
cond_mucinosum_pat = F.array_contains(F.col("Carcinoma_histology"),"Carcinoma mucinosum")
cond_ductale_in_situ_pat = F.array_contains(F.col("Carcinoma_histology"),"Carcinoma ductale in situ")
cond_lobulare_in_situ_pat = F.array_contains(F.col("Carcinoma_histology"),"Carcinoma lobulare in situ")

#Here is a list of conditions of other possible pathologies that could have been mentioned in the pathology texts
cond_musinoot_text = F.size("histology_lauseet_musinoot")>0
cond_papill_text = F.size("histology_lauseet_papill")>0
cond_tubulaar_text = F.size("histology_lauseet_tubulaar")>0
cond_metaplast_text = F.size("histology_lauseet_metaplast")>0
cond_kribriform_text = F.size("histology_lauseet_kribriform")>0
cond_neuroend_text = F.size("histology_lauseet_neuroend")>0
cond_apocrin_text = F.size("histology_lauseet_apocrin")>0
cond_inflamm_text = F.size("histology_lauseet_inflamm")>0
cond_duct_in_situ_text = F.size("histology_lauseet_dukt_in_situ")>0
cond_lobul_in_situ_text = F.size("histology_lauseet_lobul_in_situ")>0

#For ductal carsinoma we are using multiple conditions
#Ductal carsinoma is diagnosed if
##ductal carsinoma is diagnosed both in pathology and diagnose code
##ductal carsinoma is diagnosed both pathology, mentioned in pathology texts and not mentioned in pathology texts in negation
##pathology diagnosis is a non-specific carsinoma , ductal carsinoma is mentioned in pathology texts and not mentioned in pathology texts in negation
##pathology diagnosis is a non-specific carsinoma , ductal carsinoma is diven in diagnosis code and not mentioned in pathology texts in negation
##ductal carsinoma is in pathology diagnosis and lobular not mentioned in texts 
cond_duct_multi = ((cond_duct_pat & cond_duct_dg) 
                    | (cond_duct_pat & cond_duct_text & (~cond_duct_neg_text)) 
                    | (cond_nonspecific_pat & cond_duct_text & (~cond_duct_neg_text)) 
                    | (cond_nonspecific_pat & cond_duct_dg & (~cond_lobul_text)) 
                    | (cond_duct_pat & (~cond_lobul_text))
                    )

#For lobular carsinoma we are using the same multiple conditions as with ductal but the opposite
cond_lobul_multi = ((cond_lobul_pat & cond_lobul_dg) 
                     | (cond_lobul_pat & cond_lobul_text & (~cond_lobul_neg_text)) 
                     | (cond_nonspecific_pat & cond_lobul_text & (~cond_lobul_neg_text)) 
                     | (cond_nonspecific_pat & cond_lobul_dg & (~cond_duct_text)) 
                     | (cond_lobul_pat & (~cond_duct_text))
                     )



#Different histology columns are defined befor concatiating
#Ductal and lobular conditions from above
#For other histologies look if they are mentioden either in pathology texts in pathology diagnosis
#Concat the histology columns in the final step
pad_preop_breast_histology_collected = (pad_preop_breast_histology_collected_tmp
                                        .withColumn("histology_ductal", 
                                        F.when(cond_duct_multi, "Carsinoma ductale").otherwise(None)
                                        )
                                        .withColumn("histology_lobular", 
                                        F.when(cond_lobul_multi, "Carsinoma lobulare").otherwise(None)
                                        )
                                        .withColumn("histology_papillare", 
                                        F.when((cond_papillare_pat | cond_papill_text), "Carsinoma papillare").otherwise(None)
                                        )
                                        .withColumn("histology_metaplasticum", 
                                        F.when((cond_metaplasticum_pat | cond_metaplast_text), "Carsinoma metaplasticum").otherwise(None)
                                        )
                                        .withColumn("histology_tubulare", 
                                        F.when((cond_tubulare_pat | cond_tubulaar_text), "Carsinoma tubulare").otherwise(None)
                                        )
                                        .withColumn("histology_apocrinum", 
                                        F.when((cond_apocrinum_pat | cond_apocrin_text), "Carsinoma apocrinum").otherwise(None)
                                        )
                                        .withColumn("histology_inflammatoricum", 
                                        F.when((cond_inflammatoricum_pat | cond_inflamm_text), "Carsinoma inflammatoricum").otherwise(None)
                                        )
                                        .withColumn("histology_neuroendocrinum", 
                                        F.when((cond_neuroendocrinum_pat | cond_neuroend_text), "Carsinoma neuroendocrinum").otherwise(None)
                                        )
                                        .withColumn("histology_mucinosum", 
                                        F.when((cond_mucinosum_pat | cond_musinoot_text), "Carsinoma mucinosum").otherwise(None)
                                        )
                                        .withColumn("histology_kribriform", 
                                        F.when(cond_kribriform_text, "Carsinoma cribriform").otherwise(None)
                                        )
                                        .withColumn("histology_ductale_in_situ", 
                                        F.when((cond_duct_in_situ_text | cond_ductale_in_situ_pat), "Carsinoma ductale in situ").otherwise(None)
                                        )
                                        .withColumn("histology_lobul_in_situ", 
                                        F.when((cond_lobulare_in_situ_pat | cond_lobul_in_situ_text), "Carsinoma lobulare in situ").otherwise(None)
                                        )
                                        .withColumn("histology", F.concat_ws(", ", 'histology_ductal',
                                                                              'histology_lobular',
                                                                              'histology_papillare',
                                                                              'histology_metaplasticum',
                                                                              'histology_tubulare',
                                                                              'histology_apocrinum',
                                                                              'histology_inflammatoricum',
                                                                              'histology_neuroendocrinum',
                                                                              'histology_mucinosum',
                                                                              'histology_kribriform',
                                                                              'histology_ductale_in_situ',
                                                                              'histology_lobul_in_situ'
                                                                              )
                                                    )
                                          )
```

### 3.6. Pre-treatment ER
Pre-operative estrogen receptor status is extracted from pathological diagnosis. This is done by collecting all the ER measurement values with respect to ```["ssn", "organ", "episode_id"]```. Both the positive/negative status and the percentage are collected. Unfortunately the ER percentage column is incompletly filled in our cohort for the patients with negative response. The threshold for positive result is ```ER% > 9%'```. 

```python
#Regex for ER
er_regex = "((?i)estrogen)|(^\s*ER\W*$)"

#Let's collect the rows considering ER from preop pathologies
pad_preop_breast_er = (pad_preop_breast
                        .filter(F.col("diagnose").rlike(er_regex))
                        .select("ssn", "pathology_samplenumber", "organ", "diagnose", "diagnosesuffix", "episode_id")
                        .distinct()
                      )

#Next group the rows w.r.t. ["ssn", "organ", "episode_id"] and collect a set of ER diagnosis. 
#Concat the possible ERs
pad_preop_breast_er_collected_tmp = (pad_preop_breast_er
                                        .groupBy("ssn", "organ", "episode_id")
                                        .agg(F.collect_set("diagnose").alias("ER_tmp"))
                                        .withColumn("ER", F.concat_ws(";", F.col("ER_tmp")))
                                        )

#Similarly as in the previous stepm, we can collect the ER% from the diagnosesuffix column
#Clean the values my removing all the other characters but numbers, "%" and ";".
pad_preop_breast_er_prop_collected_tmp = (pad_preop_breast_er
                                        .groupBy("ssn", "organ", "episode_id")
                                        .agg(F.collect_set("diagnosesuffix").alias("ER_prop_tmp"))
                                        .withColumn("ER_prop_tmp", F.concat_ws(";", F.col("ER_prop_tmp")))
                                        .withColumn("ER_prop", F.regexp_replace(F.col("ER_prop_tmp"), "[^0-9%;]", ""))
                                        )

#Join the two dataframes
pad_preop_breast_er_collected = (pad_preop_breast_er_collected_tmp
                                .select("ssn", "organ", "episode_id","ER")
                                .join(pad_preop_breast_er_prop_collected_tmp.select("ssn", "organ", "episode_id","ER_prop")
                                      , on = ["ssn", "organ", "episode_id"]
                                      , how = "left"
                                      )
                                )
```
### 3.7. Pre-treatment PR
Exactly the same steps as for collecting ER are done for collecting PR.

```python
#PR regex
pr_regex = "((?i)progest)|(^\s*PR\W*$)"

#Let's collect the rows considering PR from preop pathologies
pad_preop_breast_pr = (pad_preop_breast
                        .filter(F.col("diagnose").rlike(pr_regex))
                        .select("ssn", "pathology_samplenumber", "organ", "diagnose", "diagnosesuffix", "episode_id")
                        .distinct()
                      )

#Next group the rows w.r.t. ["ssn", "organ", "episode_id"] and collect a set of PR diagnosis. 
#Concat the possible PRs
pad_preop_breast_pr_collected_tmp = (pad_preop_breast_pr
                                        .groupBy("ssn", "organ", "episode_id")
                                        .agg(F.collect_set("diagnose").alias("pr_tmp"))
                                        .withColumn("PR", F.concat_ws(";", F.col("pr_tmp")))
                                        )

#Similarly as in the previous stepm, we can collect the PR% from the diagnosesuffix column
#Clean the values my removing all the other characters but numbers, "%" and ";".
pad_preop_breast_pr_prop_collected_tmp = (pad_preop_breast_pr
                                        .groupBy("ssn", "organ", "episode_id")
                                        .agg(F.collect_set("diagnosesuffix").alias("pr_prop_tmp"))
                                        .withColumn("pr_prop_tmp", F.concat_ws(";", F.col("pr_prop_tmp")))
                                        .withColumn("pr_prop", F.regexp_replace(F.col("pr_prop_tmp"), "[^0-9%;]", ""))
                                        )

#Join the two dataframes
pad_preop_breast_pr_collected = (pad_preop_breast_pr_collected_tmp
                                .select("ssn", "organ", "episode_id","PR")
                                .join(pad_preop_breast_pr_prop_collected_tmp.select("ssn", "organ", "episode_id","pr_prop")
                                      , on = ["ssn", "organ", "episode_id"]
                                      , how = "left"
                                      )
                                )
```
### 3.8. Pre-treatment IHC
Similarly the pre-treatment HER2+ (c-erbB2) IHC diagnose is collected with respect to ```["ssn", "organ", "episode_id"]```.

```python
#This refers to HER2+ test
her2_regex = "(?i)c-erbB2"
 #This refers to HER2+ ISH test
ish_regex = "(?i)ish"

#Select the HER2+ test rows from the pre-treatment pathology diagnosis dataframe
#In our system, if the HER2+ (c-erbB2) test diagnosesuffix does not mention ISH, it is then IHC test
pad_preop_beast_her2_ihc = (pad_preop_breast
                              .filter((F.col("diagnose").rlike(her2_regex)) & (~(F.col("diagnosesuffix").rlike(ish_regex))))
                              .select("ssn", "pathology_samplenumber", "organ", "diagnose", "diagnosesuffix", "episode_id")
                              .distinct()
                              )

#Similarly as in the earlier steps, all the IHC diagnosis from the neoadjuvant period are collected as a set w.r.t. ["ssn", "organ", "episode_id"]
pad_preop_breast_her2_ihc_collected = (pad_preop_breast_her2_ihc
                                        .groupBy("ssn", "organ", "episode_id")
                                        .agg(F.collect_set("diagnose").alias("ihc_diagnose_tmp"), F.collect_set("diagnosesuffix").alias("ihc_result_tmp"))
                                        .withColumn("ihc_diagnose", F.concat_ws(";", F.col("ihc_diagnose_tmp")))
                                        .withColumn("ihc_result", F.concat_ws(";", F.col("ihc_result_tmp")))
                                        )
```
### 3.8. Pre-treatment ISH
The HER2+ ISH is collected very much in the same way as IHC. From ISH we do not have more inforation but the positive/negative result.

```python
#This has the same picking logic as the IHC results but now we include instead of exclude the rows that mention "ISH" 
pad_preop_breast_her2_ish = (pad_preop_breast
                              .filter((F.col("diagnose").rlike(her2_regex)) & (F.col("diagnosesuffix").rlike(ish_regex)))
                              .select("ssn", "pathology_samplenumber", "organ", "diagnose", "diagnosesuffix", "episode_id")
                              .distinct()
                              )

#As earlier, we collect the diagnosis as a set w.r.t. ["ssn", "organ", "episode_id"]
pad_preop_breast_her2_ish_collected = (pad_preop_breast_her2_ish
                                        .groupBy("ssn", "organ", "episode_id")
                                        .agg(F.collect_set("diagnose").alias("ish_diagnose_tmp"), F.collect_set("diagnosesuffix").alias("ish_result_tmp"))
                                        .withColumn("ish_diagnose", F.concat_ws(";", F.col("ish_diagnose_tmp")))
                                        .withColumn("ish_result", F.concat_ws(";", F.col("ish_result_tmp")))
                                        )
```

### 3.8. Pre-treatment tumor grade
Tumor grade data was already extracted in step _3.2. Preop pathology text extractions_. In this step, all the tumor grade diagnosis are collected as the other pathological variables with respect to  ```["ssn", "organ", "episode_id"]```. In our system we premilinary collect the data from pathology diagnosis and secondarly from pathological texts. If there are different tumor grades from different sub-examinations, we will select the highest grade. 

Besides the ```list_of_lists2set_udf``` funtion that we already defined and used in _3.5. Pre-treatment histology_, we will collect the tumor grades from a input set with the following function:

```python
#This function is used to loop through all the numbers in a string list and concat the values if the list has multiple values
def lists_collect_numbers(str_list):
  #Empty list
  list_tmp = []
  #Regex pattern for tumor grades
  pattern = r'[1-4]'

  #Loop through the list of strings
  for string in str_list:
    #Find matches
    matches = re.findall(pattern, string)
    #Extend the list with matches
    list_tmp.extend(matches)

  #If thhe list is not empty, select the max tumor grade
  if len(list_tmp)>0:
    return_value = max(list_tmp)
  else:
    return_value = None

  return return_value

#Pyspark spesific UDF modification
lists_collect_numbers_udf = F.udf(lists_collect_numbers, StringType())

```
Here is the code for collecting the extracted pre-treatment tumor grades.

```python
#Condition for collecting the tumor grade primarly from pathology diagnosis and only secondarly from pathology texts
cond_grade = ((F.col("extracted_grade_dg").isNull()) 
                 | (F.size("extracted_grade_dg")==0)
                 )
                 & ((F.col("extracted_grade_text").isNotNull()) 
                 | (F.size("extracted_grade_text")!=0)
                 )

#Select the columns by using the condition above
#Then collect the values w.r.t ["ssn", "organ", "episode_id"] as done previously
#Select the max tumor grade for each patient
pad_preop_breast_grade_collected = (pad_preop_breast
                                    .withColumn("extracted_grade", F.when(cond_grade
                                                , F.col("extracted_grade_text")
                                                )
                                                .otherwise(F.col("extracted_grade_dg"))
                                                )
                                    .groupBy("ssn", "organ", "episode_id")
                                    .agg(F.collect_set("extracted_grade").alias("grade_tmp"))
                                    .withColumn("grade_tmp", list_of_lists2set_udf("grade_tmp"))
                                    .withColumn("grade", lists_collect_numbers_udf("grade_tmp"))
                                    .select("ssn", "organ", "episode_id", "grade")
                                    )
```

### 3.x. Pre-treatment combined dataframe
In this point we are missing some pre-treatment variables such as ECOG (or WHO), earlier breast cancer, MiB (or KI-67) and lymph node cytology (TNM class and tumor stage were in the list, but it seems that we will not get those). These variables will be included in this documentation when they are picked. In this point here we can demonstrate how the variables that are collected in this point can be joined together.  

```python
#Join the dataframes cteated in the previous sub-paragraphs to a combined dataframe
pad_preop_breat_master = (pad_preop_breast_histology_collected
                          .select("ssn", "organ", "episode_id", "histologia")
                          .join(pad_preop_breast_er_collected
                                .select("ssn", "organ", "episode_id", "ER","ER_prop")
                                , ["ssn", "organ", "episode_id"]
                                , "left"
                          )
                          .join(pad_preop_breast_pr_collected
                                .select("ssn", "organ", "episode_id", "PR","PR_prop")
                                , ["ssn", "organ", "episode_id"]
                                , "left"
                          ).join(pad_preop_breast_grade_collected
                                .select("ssn", "organ", "episode_id", "gradus")
                                , ["ssn", "organ", "episode_id"]
                                , "left"
                          )
                          .join(pad_preop_breast_her2_ihc_collected
                                .select("ssn", "organ", "episode_id", "ihc_diagnose", "ihc_result")
                                , ["ssn", "organ", "episode_id"]
                                , "left"
                          )
                          .join(pad_preop_breast_her2_ish_collected
                                .select("ssn", "organ", "episode_id", "ish_diagnose")
                                , ["ssn", "organ", "episode_id"]
                                , "left"
                          )
                        )

```


```python


```


```python


```

