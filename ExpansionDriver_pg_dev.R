# Runs expansions on specified catch-sample strata. Inputs are run year, fishery code, and species code.
# There are many parts to this script, which was transformed from multiple
# SQL Server stored procs. The SQL Server stored procs were based on row by row cursor operations, whereas this script
# relies on set-based operations.


# Clear workspace
rm(list=ls(all=TRUE))

# Libraries
library(RCurl)
library(dplyr)
library(dplR)
library(DBI)
library(odbc)
library(glue)
library(stringi)
library(RODBC)
library(lubridate)
library(data.table)
library(tidyr)
library(pool)

# functions
source("GlobalFnx.R")

#=====================================================================================
# Function to get user for database
pg_user <- function(user_label) {
  Sys.getenv(user_label)
}

# Function to get pw for database
pg_pw <- function(pwd_label) {
  Sys.getenv(pwd_label)
}

# Function to get pw for database
pg_host <- function(host_label) {
  Sys.getenv(host_label)
}

# Function to get pw for database
pg_port <- function(port_label) {
  Sys.getenv(port_label)
}

# Function to connect to postgres local spi1, which is the staging schema prior to stripping
# out old pk\fk and copying to local spi .

pg_con_local_cwts = function(dbname) {
  dbPool(
    RPostgres::Postgres(),
    host = "localhost",
    dbname = dbname,
    user = pg_user("pg_user_pg"),
    password = pg_pw("pg_pwd_local"),
    port = 5432,
    options="-c search_path=cwts") 
}

pg_con_Test = function(dbname) {
  dbPool (
    RPostgreSQL::PostgreSQL(),
    host = pg_host("AWSHostTest"), 
    dbname = dbname,
    user = pg_user("AWSUser"),
    password = pg_pw("AWSPassword"),
    port = pg_port("AWSPort"),
    options="-c search_path=cwts") 
}

pg_con_Prod = function(dbname) {
  dbPool(
    RPostgreSQL::PostgreSQL(),
    host =pg_host("AWSHost"), 
    dbname = dbname,
    user = pg_user("AWSUser"),
    password = pg_pw("AWSPassword"),
    port = pg_port("AWSPort"),
    options="-c search_path=cwts")
}

# Function to convert column names to LC
lowerCN2 <- function(x) {
  colnames(x) <- tolower(colnames(x))
  x
}

# Set global for date. Used for naming csv output tables
out_date = format(Sys.Date(), "%Y-%m-%d %H:%M:%S %Z")

# Set globals 
runyr = 2018     # for target run_year
fsh = "\'45\'"       # fishery_lut.code
spc = "\'1\'"        # species_lut.cwt_species_code; chin=1;coho=4;stlhd=6;pink=3;sockeye=5;chum=2

# Set the connection pool
db_con = pg_con_Prod(dbname = "FISH")

# get expansion data for selected year/fishery/species
qry = glue::glue(
  "select e.id as expansion_id,pooled_expansion_id,run_year,e.fishery_id,",
  "e.species_id,run_id,time_period_number,time_period_type_id,",
  "expansion_location_code,maturity_id,sampled_mark_id,expansion_category_id,",
  "detect_method_id,sampled_length_range_number,expansion_level_number,selective_fishery_type_id,",  
  "sample_agency_id,catch_count,catch_coefficient_of_variation_qty,tag_checked_count,mark_checked_count,",
  "snout_count,awareness_qty,expansion_factor_qty,expansion_factor_coefficient_of_variation_qty,pooled_data_indicator,",  
  "data_completeness_id,sample_pst_indicator,sample_pst_datetime,comment_txt ",      
  "from expansion e ",
  "inner join fishery_lut f on e.fishery_id = f.id ",
  "inner join species_lut s on e.species_id = s.id ",
  "where run_year = {runyr} and f.code = {fsh} and s.cwt_species_code = {spc} ",
  "and expansion_category_id in('455b511c-d352-44f8-b5f8-14eed966644d','c1644971-08d0-428b-a76c-6a8e06517adc')") # expansion category codes 1 and 2 only

catsam = DBI::dbGetQuery(db_con, qry)

# Create a table to hold existing records, for comparison at the end of the procedure
qry = glue::glue(
  "select e.id as expansion_id,run_year,e.fishery_id,e.species_id,catch_count,tag_checked_count,snout_count,mark_checked_count,",
  "expansion_factor_qty,awareness_qty,expansion_category_id ",
  "into compare ",
  "from expansion e ",
  "inner join fishery_lut f on e.fishery_id = f.id ",
  "inner join species_lut s on e.species_id = s.id ",
  "where run_year = {runyr} and f.code = {fsh} and s.cwt_species_code = {spc}"
  )

dbExecute(db_con, qry, immediate = TRUE)

qry = glue::glue("alter table compare ADD COLUMN adj8 numeric(7,3), ADD COLUMN adj3 numeric(7,3), ADD COLUMN adj4 numeric(7,3)")
dbExecute(db_con, qry, immediate = TRUE)

# no snouts
qry = glue::glue(
  "update compare c set adj8 = a.adjustment_qty ",
  "from expansion_adjustment a where a.expansion_id = c.expansion_id ",
  "and a.result_type_id = '3d7f6258-9d1b-4391-8b0c-ce37df987d0e'")
dbExecute(db_con, qry, immediate = TRUE)
# lost tags
qry = glue::glue(
  "update compare c set adj3 = a.adjustment_qty ",
  "from expansion_adjustment a where a.expansion_id = c.expansion_id ",
  "and a.result_type_id = 'ab45f5b4-9cbe-41e0-b3eb-37a90c8ec6c1'")
dbExecute(db_con, qry, immediate = TRUE)
# unreadable tags
qry = glue::glue(
  "update compare c set adj4 = a.adjustment_qty ",
  "from expansion_adjustment a where a.expansion_id = c.expansion_id ",
  "and a.result_type_id = '6874f544-e75c-492b-8a31-df1613dcd0e0'")
dbExecute(db_con, qry, immediate = TRUE)
compare = dbReadTable(db_con,"compare")
dbExecute(db_con,"drop table compare") 

# =====================================================================================================================================
# Update snout counts
# =====================================================================================================================================
# Update snout counts for this year/fishery/species. Create temporary table for checking snout count changes.
# Load data from this year/fishery/species,before snout count changes.Make a placeholder for every cwt result type,even if not present.

qry = glue::glue(
  "select e.id as expansion_id, e.expansion_category_id, r.id as result_type_id ",
  "into sntcnt from expansion e ",
  "inner join fishery_lut f on e.fishery_id = f.id ",
  "inner join species_lut s on e.species_id = s.id ",
  "CROSS JOIN result_type_lut r ",
  "where f.code = {fsh} and s.cwt_species_code = {spc} and e.run_year = {runyr}")
dbExecute(db_con, qry, immediate = TRUE)

qry = glue::glue(
  "alter table sntcnt add column old_count integer, add column new_count integer;"
)
dbExecute(db_con, qry, immediate = TRUE)

qry = glue::glue(
  "update sntcnt set old_count = 0, new_count = 0;")
dbExecute(db_con, qry, immediate = TRUE)

qry = glue::glue(
  "update sntcnt s set old_count = r.recovery_count ",
  "from result r where s.result_type_id = r.result_type_id ",
  "AND s.expansion_id = r.expansion_id")
dbExecute(db_con, qry, immediate = TRUE)
sntcnt = dbReadTable(db_con,"sntcnt")
dbExecute(db_con,"drop table sntcnt")

#===============================================================================================================================
# Call the stored procedure that drives the counting and recording of recoveries associated with the records in the target year,
# fishery and species. sp_expnsn_sntcountdriver; called from sp_expnsn_driver
#===============================================================================================================================

# Start of snout count driver section 

  # get data for expansion records,for selected year/fishery/species
  qry = glue::glue(
    "select e.id as csid, detect_method_id ",      
    "from expansion e ",
    "inner join fishery_lut f on e.fishery_id = f.id ",
    "inner join species_lut s on e.species_id = s.id ",
    "where run_year = {runyr} and f.code = {fsh} and s.cwt_species_code = {spc}"
    )
  
  catsam_csr = DBI::dbGetQuery(db_con, qry)
  
#=========================================================================================================================================
#  Call procedure to count snouts for this run year\fishery\species. Procedure returns counts of the associated recoveries, by tag result. 
#  sp_expnsn_snoutcnt; called from sp_expnsn_sntcountdriver
#  Input is the expansion_id for the record. Output is the count of the snouts, one for each cwt_result_type_LUT.
#=========================================================================================================================================
  
  catsam_csr = catsam_csr %>%
    mutate(st1 = 0L,st2 = 0L,st3 = 0L,st4 = 0L,st7 = 0L,st8 = 0L,st9 = 0L) %>% # Initialize the counts to zero
    select(csid,st1,st2,st3,st4,st7,st8,st9)
  # Find matching records and count snouts. Create a temp.table to hold snout counts, by result type 
  qry = glue::glue(
    "select expansion_id,result_type_id,count(*) as snts ",      
    "into rsnts ",
    "from cwts.recovery_fish f right join catsam_csr c on f.expansion_id = c.csid::uuid ", # changed from inner join
    "group by expansion_id,result_type_id"
    )
  
  dbWriteTable(db_con,"catsam_csr",catsam_csr)
  dbExecute(db_con, qry, immediate = TRUE)
 
  qry = glue::glue(
    "update catsam_csr ",      
    "set st1 = snts ",
    "from rsnts where expansion_id = csid::uuid ",
    "and result_type_id = '9f89f4f0-0f71-4702-8139-28f0ac1143b2'")   # decoded tags
  dbExecute(db_con, qry, immediate = TRUE)
  qry = glue::glue(
    "update catsam_csr ",      
    "set st2 = snts ",
    "from rsnts where expansion_id = csid::uuid ",
    "and result_type_id = '096d9e3d-0522-46e0-a3f0-1bb98d3d8bd9'")   # no tags
  dbExecute(db_con, qry, immediate = TRUE)
  qry = glue::glue(
    "update catsam_csr ",      
    "set st3 = snts ",
    "from rsnts where expansion_id = csid::uuid ",
    "and result_type_id = 'ab45f5b4-9cbe-41e0-b3eb-37a90c8ec6c1'")   # lost tags
  dbExecute(db_con, qry, immediate = TRUE)
  qry = glue::glue(
    "update catsam_csr ",      
    "set st4 = snts ",
    "from rsnts where expansion_id = csid::uuid ",
    "and result_type_id = '6874f544-e75c-492b-8a31-df1613dcd0e0'")   # unreadable tags
  dbExecute(db_con, qry, immediate = TRUE)
  qry = glue::glue(
    "update catsam_csr ",      
    "set st7 = snts ",
    "from rsnts where expansion_id = csid::uuid ",
    "and result_type_id = '82067922-4274-436f-a579-c64409340f6c'")   # discrepancy tags
  dbExecute(db_con, qry, immediate = TRUE)
  qry = glue::glue(
    "update catsam_csr ",      
    "set st8 = snts ",
    "from rsnts where expansion_id = csid::uuid ",
    "and result_type_id = '3d7f6258-9d1b-4391-8b0c-ce37df987d0e'")   # no snout tags
  dbExecute(db_con, qry, immediate = TRUE)
  qry = glue::glue(
    "update catsam_csr ",      
    "set st9 = snts ",
    "from rsnts where expansion_id = csid::uuid ",
    "and result_type_id = 'cb01d653-5979-43df-9e40-836c991b2036'")   # blank wire tags
  dbExecute(db_con, qry, immediate = TRUE)
  
  rtn = dbReadTable(db_con,"catsam_csr")
  dbExecute(db_con,"drop table rsnts")
  dbExecute(db_con,"drop table catsam_csr")

  #===================================================================================================================
  # Call procedure to update records with snout counts. sp_expnsn_set_cwtresult; called from sp_expnsn_sntcountdriver
  #===================================================================================================================
  
  # Step through the result types:if record exists,update it. If not,insert. Create direction table:
  cwtr = rtn %>%
    mutate(expansion_id = csid) %>%
    pivot_longer(.,cols = c("st1","st2","st3","st4","st7","st8","st9"),
                 names_to = "result_type_id",values_to = "recovery_count") %>%
    mutate(cwtresult_id = case_when(result_type_id == "st1" ~ '9f89f4f0-0f71-4702-8139-28f0ac1143b2',
                                    result_type_id == "st2" ~ '096d9e3d-0522-46e0-a3f0-1bb98d3d8bd9',
                                    result_type_id == "st3" ~ 'ab45f5b4-9cbe-41e0-b3eb-37a90c8ec6c1',
                                    result_type_id == "st4" ~ '6874f544-e75c-492b-8a31-df1613dcd0e0',
                                    result_type_id == "st7" ~ '82067922-4274-436f-a579-c64409340f6c',
                                    result_type_id == "st8" ~ '3d7f6258-9d1b-4391-8b0c-ce37df987d0e',
                                    result_type_id == "st9" ~ 'cb01d653-5979-43df-9e40-836c991b2036',
                                    TRUE ~ NA_character_))
                                    
  # cwt_result record already exists, but new count is zero. sp_cwt_result_delete; called from sp_expnsn_set_cwtresult
  qry = glue::glue(
    "DELETE FROM result f ",
    "Using cwtr ",
    "where f.expansion_id = cwtr.expansion_id::uuid and f.result_type_id = cwtr.cwtresult_id::uuid ",
    "and cwtr.recovery_count = 0 and f.recovery_count > 0")
  
  dbWriteTable(db_con,"cwtr",cwtr)
  dbExecute(db_con, qry, immediate = TRUE)
  
  # Update the existing record if recovery_count values are different. sp_cwt_result_update; called from sp_expnsn_set_cwtresult
  qry = glue::glue(
    "UPDATE result f set recovery_count = c.recovery_count ",
    "from cwtr c where f.expansion_id = c.expansion_id::uuid and f.result_type_id = c.cwtresult_id::uuid ",
    "and c.recovery_count <> 0 and c.recovery_count <> f.recovery_count")
  dbExecute(db_con, qry, immediate = TRUE)
  
  # Create a new record, if none exists. Using EXCEPT method here. sp_cwt_result_insert; called from sp_expnsn_set_cwtresult
  qry = glue::glue(
    "Insert Into result (expansion_id,result_type_id,recovery_count) ",
    "Select expansion_id::uuid,cwtresult_id::uuid,recovery_count from cwtr c ",
    "Where c.recovery_count > 0 ",
    "EXCEPT Select expansion_id,result_type_id,recovery_count from result")
  dbExecute(db_con, qry, immediate = TRUE)
  dbExecute(db_con,"drop table cwtr")
  
  #===================================================================================================================================
  # Call procedure to count four mark groupings for associated tagged fish. sp_expnsn_tagmarkcnt; called from sp_expnsn_sntcountdriver
  #===================================================================================================================================
  
  # Initialize counts to zero
  rtns = rtn %>% 
    distinct(csid) %>%
    mutate(adclip_tags = 0L,noadclip_tags = 0L,
           undt_tags = 0L,unkn_tags = 0L) %>%
    select(csid,adclip_tags,noadclip_tags,undt_tags,unkn_tags)
  
  # Make temporary table of associated recoveries by mark code
  qry = glue::glue(
    "select c.csid as expansion_id,csid,adclip_tags,noadclip_tags,undt_tags,unkn_tags,mrk.code as mark_code, count(*) as cwt_cnt ",
    "from recovery_fish fsh INNER JOIN mark_lut mrk ON fsh.external_mark_id = mrk.id ",
    "right join rtns c on fsh.expansion_id = c.csid::uuid group by c.csid,csid,adclip_tags,noadclip_tags,undt_tags,unkn_tags,mrk.code") # changed from inner join
  
  dbWriteTable(db_con,"rtns",rtns)  
  rtn = DBI::dbGetQuery(db_con, qry)
  dbExecute(db_con,"drop table rtns") 
  
  # Set four categories of recovery counts, by adipose clip status
  rtn = rtn %>%
    mutate(adclip_tags = if_else(mark_code %like% "^5",cwt_cnt,adclip_tags)) %>%
    mutate(noadclip_tags = if_else(mark_code %like% "^0",cwt_cnt,noadclip_tags)) %>%
    mutate(undt_tags = if_else(mark_code == "UNDT",cwt_cnt,undt_tags)) %>%
    mutate(unkn_tags = if_else(mark_code %like% "^9",cwt_cnt,unkn_tags)) %>%
    # Reset null values to zero
    mutate(adclip_tags = if_else(is.na(adclip_tags),0L,adclip_tags)) %>%
    mutate(noadclip_tags = if_else(is.na(noadclip_tags),0L,noadclip_tags)) %>%
    mutate(undt_tags = if_else(is.na(undt_tags),0L,undt_tags)) %>%
    mutate(unkn_tags = if_else(is.na(unkn_tags),0L,unkn_tags)) %>%
    group_by(expansion_id) %>%
    summarise(adclip_tags=sum(adclip_tags),noadclip_tags=sum(noadclip_tags),
              undt_tags=sum(undt_tags),unkn_tags=sum(unkn_tags)) %>%
    select(expansion_id,adclip_tags,noadclip_tags,undt_tags,unkn_tags) %>%
    ungroup()
  
  rtn = rtn %>%
    mutate(new_count_sum = adclip_tags+noadclip_tags+undt_tags+unkn_tags)
  
#==================================================================================================================================================================
#  Call procedure to update this record with tagged mark counts. sp_expnsn_settagmark; called from sp_expnsn_sntcountdriver
#==================================================================================================================================================================
  
#  Stored procedure for calling the update, delete or insert procedures for tables checked_tag and checked_mark to set the correct counts, for one expansion record.
#  Input: expansion_id and counts of recoveries, in the four mark categories that correspond with values in the
#          adipose_clip_status_LUT table: adiipose clipped, no adipose clip, unknown adipose clip, and undetermineable adipose clip.
#  Determine if no changes are needed where incoming counts are zero and there are no existing records
  
#==================================================================================================================================================================
#  If incoming snouts are zero and there are existing record(s). Delete the checked_tag record, which will also delete the related checked_mark records.
#  sp_checked_tag_delete; called from sp_expnsn_settagmark
#  Filter for just records where all 4 summed mark categories have no tags. This will also delete the related checked_mark record due to "cascade delete" setting.

  notags = rtn %>%    
    filter(new_count_sum==0) %>%
    select(expansion_id)
  
  if(nrow(notags)>0)  {
    qry = glue::glue(
      "DELETE From checked_tag c using notags n ",
      "where c.expansion_id = n.expansion_id::uuid ",
      "and c.detected_id = '3b02c2fe-23a1-4a82-a708-c2051979d8f0'")
    
    dbWriteTable(db_con,"notags",notags)
    dbExecute(db_con, qry, immediate = TRUE)
    dbExecute(db_con,"drop table notags")
  }

# Create empty temp. tables to hold marks counts for existing data and new data. This logic is based upon the assumption that the only adipose_clip_status_id
# values possible will be 1 - 4, and those values retain the same meaning as when this procedure was written:
 
#       adipose_clip_status_id     adipose_clip_status_desc
# --    -------------------------- --------------------------
# --    0                          Not Applicable
# --    1                          Adipose Fin Clip
# --    2                          Adipose Fin Not Clipped
# --    3                          Unknown Clip Status
# --    4                          Adipose Fin Clip Undetermineable
# --    5                          Fish Not Checked for Adipose Fin Clip 
  
  current_marks = rtn  %>% 
    # filter out rows where all 4 summed mark categories have no tags; >>>>>>>>should this be done later in the script?<<<<<<<<<<<<<<<<
    filter(new_count_sum>0) %>%
    pivot_longer(.,cols = c("adclip_tags","noadclip_tags","undt_tags","unkn_tags"),
                 names_to = "adclipstatuslut_id",values_to = "recovery_count") %>%
    mutate(adipose_clip_status_id = case_when(adclipstatuslut_id == "adclip_tags" ~ "cf56e8ad-da71-4d62-82d5-95b439ab62a9",
                                              adclipstatuslut_id == "noadclip_tags" ~ "e09b4838-54a8-4f52-92d8-d47baddd1d80",
                                              adclipstatuslut_id == "unkn_tags" ~ "60936df9-b94d-4116-baad-415ac9ab6fb2",
                                              adclipstatuslut_id == "undt_tags" ~ "d5cfedf3-a212-4615-9519-00ab63ae2a97",
                                              TRUE ~ NA_character_)) %>%
    mutate(checked_tag_id = NA_character_,checked_mark_id = NA_character_) %>%
    select(checked_tag_id,expansion_id,checked_mark_id,adipose_clip_status_id,recovery_count)
  
  # if table does not update with checked_tag_id it has no existing tagged recoveries 
  qry = glue::glue(
    "update cwts.current_marks set checked_tag_id = ",
    "(select id from cwts.checked_tag tag ",
    "where tag.expansion_id = cwts.current_marks.expansion_id::uuid and tag.detected_id = '3b02c2fe-23a1-4a82-a708-c2051979d8f0')")
  
  dbWriteTable(db_con,"current_marks",current_marks)
  dbExecute(db_con, qry, immediate = TRUE)
  
  qry = glue::glue(
    "update current_marks c set checked_mark_id = mrk.id ",
    "from checked_mark mrk where c.adipose_clip_status_id::uuid = mrk.adipose_clip_status_id ",
    "and mrk.checked_tag_id = c.checked_tag_id::uuid")
  
  dbExecute(db_con, qry, immediate = TRUE)
  current_marks = dbReadTable(db_con,"current_marks")
  dbExecute(db_con,"drop table current_marks")
  
  # mark existing tag records as current or new
  current_marks = current_marks %>%
    mutate(current_flag = NA_integer_) %>%
    mutate(current_flag = if_else(!is.na(checked_tag_id) & !is.na(checked_mark_id),1L, 0L))  # 1=current, 0=new
                                  
  #=================================================================================================
  # Check if no existing tag records. If so, insert a record into checked_tag table, detected_id = 1

  # filter for new tag records only
  new_marks = current_marks %>%
    filter(current_flag==0 & recovery_count>0) %>%
    distinct(expansion_id) %>%
    mutate(detected_id = "3b02c2fe-23a1-4a82-a708-c2051979d8f0")
  
  if(nrow(new_marks)>0)  {
    ## sp_checked_tag_insert; called from sp_expnsn_settagmark
    qry = glue::glue(
      "INSERT INTO checked_tag ",
      "(expansion_id,detected_id) ",
      "Select expansion_id,detected_id from new_marks")
    
    dbWriteTable(db_con,"new_marks",new_marks)
    dbExecute(db_con, qry, immediate = TRUE)
    
    ## get the new checked_tag_id
    qry = glue::glue("alter table new_marks add column checked_tag_id integer")
    dbExecute(db_con, qry, immediate = TRUE)
    
    qry = glue::glue(
      "update c set c.checked_tag_id = a.checked_tag_id ",
      "from checked_tag a where a.expansion_id = c.expansion_id ",
      "and a.detected_id = '3b02c2fe-23a1-4a82-a708-c2051979d8f0'")
    dbExecute(db_con, qry, immediate = TRUE)
    
    new_marks = dbReadTable(db_con,"new_marks")
    dbExecute(db_con,"drop table new_marks")
  }
  
  #=============================================================================================================================================================
  # Program logic for following inserts, updates and deletions:
  # Handles inserts, updates, and deletes in 3 separate sections. For each section, reads through the temporary table, by adipose_clip_status_id value (1-4) and
  # determines if the compared values meet the criteria for the section. If so takes action.
  ## update temp table with new tag id if needed
  current_marks = current_marks %>%
    mutate(new_checked_tag_id = NA_character_) %>%
    select(checked_tag_id,new_checked_tag_id,expansion_id,checked_mark_id,adipose_clip_status_id,recovery_count,current_flag)
  
  ## update temp table with new tag id if needed
  if (nrow(new_marks)>0) {
    current_marks = current_marks %>%
      left_join(.,new_marks,by = c("expansion_id"), na_matches = "never") %>%
      mutate(new_checked_tag_id = checked_tag_id.y) %>%
      select(checked_tag_id,new_checked_tag_id,expansion_id,checked_mark_id,adipose_clip_status_id,recovery_count,current_flag)
    
    ## Perform needed inserts. sp_checked_mark_insert; called from sp_expnsn_settagmark
    ##==================================================================================
    qry = glue::glue(
      "INSERT INTO checked_mark ",
      "(checked_tag_ID,adipose_clip_status_id,Fish_Cnt) ",
      "Select new_checked_tag_id,adipose_clip_status_id,recovery_count from current_marks c ",
      "Where c.new_checked_tag_id is not null and c.recovery_count > 0")
    
    dbWriteTable(db_con,"current_marks",current_marks)
    dbExecute(db_con, qry, immediate = TRUE)
    
    current_marks = dbReadTable(db_con,"current_marks")
    dbExecute(db_con,"drop table current_marks")
  }
 
  # add an update_flag; marks records that need to be updated
  current_marks = current_marks %>%
    mutate(update_flag = NA) %>%
    select(checked_tag_id,new_checked_tag_id,expansion_id,checked_mark_id,adipose_clip_status_id,recovery_count,current_flag,update_flag)
  
  ##  Perform needed updates. sp_checked_mark_update; called from sp_expnsn_settagmark
  ##==================================================================================
  ## Check for any updates. Flag record if updates needed.
  qry = glue::glue(
    "UPDATE cwts.current_marks c set ",
    "update_flag = TRUE ",
    "From cwts.checked_mark m where c.checked_mark_id::uuid = m.id ",
    "and c.new_checked_tag_id is null and c.recovery_count > 0 and ",
    "(c.checked_tag_id::uuid <> m.checked_tag_id or c.recovery_count <> m.fish_count ",
    "or c.adipose_clip_status_id::uuid <> m.adipose_clip_status_id)")
  
  dbWriteTable(db_con,"current_marks",current_marks)
  dbExecute(db_con, qry, immediate = TRUE)
  
  current_marks = dbReadTable(db_con,"current_marks")
  current_marks_ud = current_marks %>% 
    filter(update_flag == TRUE)
  current_marks_del = current_marks %>%
    filter(recovery_count == 0 & current_flag == 1)
  
  ## Perform needed updates. Write to history table before update. sp_checked_mark_H_insert; called from sp_checked_mark_update
  ## ==========================================================================================================================

  if (nrow(current_marks_ud) > 0) {
    qry = glue::glue(
      "insert into cwts.checked_mark_history ",
      "(checked_mark_id,checked_tag_id,adipose_clip_status_id,fish_count, ",
      "create_datetime,create_user,modify_datetime,modify_user,create_history_datetime ) ",
      "Select m.id,m.checked_tag_id,m.adipose_clip_status_id,fish_count, ",
      "create_datetime,create_user,modify_datetime,modify_user,current_timestamp ",
      "From cwts.checked_mark m inner join cwts.current_marks c on m.id = c.checked_mark_id::uuid where c.update_flag = TRUE")
    
    dbExecute(db_con, qry, immediate = TRUE)
    
    ## Perform updates
    qry = glue::glue(
      "UPDATE cwts.checked_mark m set ",
      "checked_tag_id = c.checked_tag_id::uuid,",
      "adipose_clip_status_id = c.adipose_clip_status_id::uuid,",
      "fish_count = c.recovery_count ",
      "From current_marks c where c.checked_mark_id::uuid = m.id ",
      "and c.new_checked_tag_id is null and c.recovery_count > 0 ",
      "and c.update_flag = TRUE")
    
    dbExecute(db_con, qry, immediate = TRUE)
  }  
  ## Perform needed deletions. sp_checked_mark_delete; called from sp_expnsn_settagmark.
  ## ===================================================================================
  ## checked_mark contains a trigger that auto-inserts into checked_mark_H table.
  if (nrow(current_marks_del) > 0) {
  qry = glue::glue(
    "DELETE from cwts.checked_mark m using cwts.current_marks c ",
    "where m.id = c.checked_mark_id::uuid ",
    "and c.recovery_count = 0 and c.current_flag = 1")
  
  dbExecute(db_con, qry, immediate = TRUE)
  
  current_marks = dbReadTable(db_con,"current_marks")
  #dbExecute(db_con,"drop table current_marks")
  }
  dbExecute(db_con,"drop table current_marks")
  #return(current_marks)
  
# end snout count driver section
# =============================================================================================================  

# Load revised file into comparison table and print any changes
qry = glue::glue(
  "update cwts.sntcnt s set new_count = r.recovery_count ",
  "from cwts.result r where s.result_type_id::uuid = r.result_type_id ",
  "AND s.expansion_id::uuid = r.expansion_id")
dbWriteTable(db_con,"sntcnt",sntcnt)
dbExecute(db_con, qry, immediate = TRUE)

# Reset nulls to zero, for comparison
qry = glue::glue(
  "update cwts.sntcnt set new_count = 0 where new_count is null")
dbExecute(db_con, qry, immediate = TRUE)

# list out expansion records with snout count changes, if changes exist

sntcnt = dbReadTable(db_con,"sntcnt")
dbExecute(db_con,"drop table sntcnt")

sntcntdiff = sntcnt %>%
  filter(old_count != new_count) 

if(nrow(sntcntdiff)>0) {
  print(sntcntdiff)
} else {
  cat("\nNo changes in snout counts\n\n")
}

#================================================================================================
#                     Update CWT_POOLED_EXPANSION
#================================================================================================
#  Update CWT_POOLED_EXPANSION for this year/fishery/species.
#  This assumes that the records (with or without data) to hold data in CWT_POOLED_EXPANSION have
#  already been created. Reads through the expansion table to determine which pooled records are
#  referenced.  Calls the update procedure for the CWT_POOLED_EXPANSION table.
# 
# Input:  Run Year, fishery and species
# Output: Updated records in CWT_POOLED_EXPANSION
#  sp_expnsn_setpooled; called from sp_expnsn_driver

#===================================================
# start set pooled function. sp_expnsn_setpooled; called from sp_expnsn_driver
  # If no expansion record exists without a pooled indicator, then return with no change

  qry = glue::glue(
    "select pooled_expansion_id,pooled_data_indicator,run_year,fishery_id,species_id ",      
    "from cwts.expansion e ",
    "inner join cwts.fishery_lut f on e.fishery_id = f.id ",
    "inner join cwts.species_lut s on e.species_id = s.id ",
    "where (pooled_data_indicator = TRUE or pooled_expansion_id is not null) ",
    "and e.run_year = {runyr} and f.code = {fsh} and s.cwt_species_code = {spc}"
    )
  
  pooled = DBI::dbGetQuery(db_con, qry)
  
  if(nrow(pooled)>0)  {   # pooled records exist
    # Check for expansion records where the pooled_expansion_id is blank
    # but the indicator is set "on" or reverse. Return error.
    qry = glue::glue(
      "select pooled_expansion_id,pooled_data_indicator,run_year,fishery_id,species_id ",
      "from cwts.expansion e ",
      "inner join cwts.fishery_lut f on e.fishery_id = f.id ",
      "inner join cwts.species_lut s on e.species_id = s.id ",
      "where (pooled_data_indicator = true and e.run_year = {runyr} and f.code = {fsh} ",
      "and s.cwt_species_code = {spc} and pooled_expansion_id is null) or ",
      "(pooled_data_indicator = false and e.run_year = {runyr} and f.code = {fsh} ",
      "and s.cwt_species_code = {spc} and pooled_expansion_id is not null)")

    poolederr = DBI::dbGetQuery(db_con, qry)
    
    # pooled records exist and there are no errors
    if (nrow(poolederr)==0) {
      # -------------------------------------------------------------------------
      # Create a temporary table to hold the expansion records requiring a pooled record,plus any existing pooled_expansion data
      qry = glue::glue(
        "select min(time_period_number) as period_begin_number,max(time_period_number) as period_end_number, ",
        "sum(catch_count) as pooled_catch_count, sum(tag_checked_count) as pooled_tag_checked_count, ",
        "pooled_expansion_id as pldexpnsn_id ",      
        "into pooled ",
        "from cwts.expansion e ",
        "inner join cwts.fishery_lut f on e.fishery_id = f.id ",
        "inner join cwts.species_lut s on e.species_id = s.id ",
        "where pooled_data_indicator = TRUE and run_year = {runyr} and f.code = {fsh} ",
        "and s.cwt_species_code = {spc} ",
        "group by pooled_expansion_id")
      
      dbExecute(db_con, qry, immediate = TRUE)
      
      qry = glue::glue("alter table cwts.pooled add column pooled_catch_coefficient_of_variation_qty numeric(5,2)")
      dbExecute(db_con, qry, immediate = TRUE)
      
      # Check for existence of the pooled ID -- if any missing, error return
      qry = glue::glue(
        "select pldexpnsn_id::uuid from cwts.pooled ",
        "where pldexpnsn_id not in(select id from cwts.pooled_expansion)")
      
      errpoolexp = DBI::dbGetQuery(db_con, qry)
      # if missing pool id
      if (nrow(errpoolexp)>0) { 
        dbExecute(db_con,"drop table pooled")
        cat("\nPooled_expansion missing IDs present in expansion\n\n")
      } else {                  # if not missing pool id
        
        # Read through temporary table, calling the update for pooled_expansion. sp_CWT_POOLED_EXPANSION_update; called from sp_expnsn_setpooled   
        # Check for any updates. Return if no changes.
        
        # Check for any updates. Flag record if updates needed.
        qry = glue::glue(
          "select c.period_begin_number,c.period_end_number,c.pooled_catch_count,c.pooled_catch_coefficient_of_variation_qty,c.pooled_tag_checked_count,pldexpnsn_id ", 
          "into poolededits ",  
          "From cwts.pooled c INNER JOIN cwts.pooled_expansion m on c.pldexpnsn_id::uuid = m.id ", 
          "Where c.period_begin_number <> m.period_begin_number or ",
          "c.period_end_number <> m.period_end_number or ", 
          "c.pooled_catch_count <> m.pooled_catch_count or ",
          "c.pooled_catch_coefficient_of_variation_qty <> m.pooled_catch_coefficient_of_variation_qty or ", 
          "c.pooled_tag_checked_count <> m.pooled_tag_checked_count"
        ) 
        
        dbExecute(db_con, qry, immediate = TRUE)
        poolededits = dbReadTable(db_con,"poolededits")
        # Perform updates if needed
        if (nrow(poolededits)==0) { # if updates not needed
          dbExecute(db_con,"drop table poolededits")
          pooled = dbReadTable(db_con,"pooled")
          dbExecute(db_con,"drop table pooled")
          cat("\nNo changes in pooled records\n\n")
        } else {                  # if updates needed
          qry = glue::glue(
            "UPDATE pooled_expansion m ",
            "SET m.period_end_number = c.period_end_number, ",
            "m.pooled_catch_count = c.pooled_catch_count, ",
            "m.pooled_catch_coefficient_of_variation_qty = c.pooled_catch_coefficient_of_variation_qty, ",
            "m.pooled_tag_checked_count = c.pooled_tag_checked_count ",
              "FROM poolededits c ",
              "WHERE c.pldexpnsn_id::uuid = m.id ",
              "AND (c.period_begin_number <> m.period_begin_number OR ",
                  "c.period_end_number <> m.period_end_number OR ",
                  "c.pooled_catch_count <> m.pooled_catch_count OR ",
                  "c.pooled_catch_coefficient_of_variation_qty <> m.pooled_catch_coefficient_of_variation_qty OR ",
                  "c.pooled_tag_checked_count <> m.pooled_tag_checked_count)"
          )
          dbExecute(db_con, qry, immediate = TRUE)
          pooled = dbReadTable(db_con,"poolededits")
          dbExecute(db_con,"drop table poolededits")
          dbExecute(db_con,"drop table pooled")

        } # end of if updates needed
      }  # end of if pooled records exist and there are no errors and not missing pool id
    } # end of if pooled records exist and there are no errors
  }  else { # pooled records do not exist; return with no change
    cat("\nNo pooled records. Proceed!\n\n") 
 }

# ==========================================================================
#             Calculate Awareness for this Year/Fishery/Species
# ==========================================================================
# Process only sport fisheries where awareness is used (PS marine, statewide freshwater)
# sp_expnsn_awarenessdriver; called from sp_expnsn_driver
# Not yet developed!!<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# ========================================================================================
#        Process expansion records to set tag adjustments and calculate expansion factor
# ========================================================================================
# ----------------------------------------------------------------------------
#                         Calculate tag adjustments
# ----------------------------------------------------------------------------

# Check for pooled records
catsam_pooled = catsam %>%
  filter(!is.na(pooled_expansion_id)) %>%
  select(expansion_id,pooled_expansion_id)

# if pooled stratum exists call expnsn_settagadjpooled function to set pooled tag adjustments  
if (nrow(catsam_pooled)>0) {

  # ================================================================================================================================
  # For records in the pooled_expansion table, pulls the combined snout counts, 
  # calculates the tag adjustment(s),if any,and calls the insert or update procedure for
  # table pooled_adjustment. If a pooled_adjustment record(s) exists for the ID and no adjustment is needed the record is deleted.
  # 
  # Input:  Pooled expansion ID.  Should be from a pooled_expansion record that is a candidate for expansion.
  # Output: Return value  1 = no action, no record   0 = update or insert or delete OK  -1 = error
  # sp_expnsn_settagadjpooled; called from sp_expnsn_driver

    #  Pull the snout counts from cwt_result table. If the result record does not exist, delete any existing adjustment records

    qry = glue::glue(
      "select distinct e.pooled_expansion_id,e.id as expid,r.expansion_id as rexpid ",
      "into rsltnotexist ",
      "from cwts.expansion e left JOIN cwts.result r ",
      "ON e.id = r.expansion_id ",
      "where e.pooled_expansion_id in(select pooled_expansion_id::uuid from catsam_pooled) ",
      "and r.expansion_id is null"
    )
    
    dbWriteTable(db_con,"catsam_pooled",catsam_pooled)
    dbExecute(db_con, qry, immediate = TRUE)
    rsltnotexist = dbReadTable(db_con,"rsltnotexist")
    
    if (nrow(rsltnotexist)>0) { # if result
      qry = glue::glue(
        "DELETE from cwts.pooled_adjustment m using cwts.rsltnotexist c ",
        "where m.pooled_expansion_id = c.pooled_expansion_id::uuid")
      
      dbExecute(db_con, qry, immediate = TRUE)
      
    }
    dbExecute(db_con,"drop table rsltnotexist")

    # Make temporary table to obtain the recovery counts for each result type
    
    qry = glue::glue(
      "select e.pooled_expansion_id,r.result_type_id, sum(recovery_count) as recov_cnt ",
      "into pull_recovs ",
      "from cwts.expansion e INNER JOIN cwts.result r ON e.id = r.expansion_id ",
      "where e.pooled_expansion_id in(select pooled_expansion_id::uuid from catsam_pooled) ",
      "group by e.pooled_expansion_id,r.result_type_id")
    
    dbExecute(db_con, qry, immediate = TRUE)
    pull_recovs = dbReadTable(db_con,"pull_recovs")
    
    dbExecute(db_con,"drop table pull_recovs")
    
    pull_recovs = pull_recovs %>%
      # Initialize the counts to zero
      mutate(t1 = 0L,t2 = 0L,t3 = 0L,t4 = 0L,t7 = 0L,t8 = 0L,t9 = 0L) %>%
      mutate(t1 = if_else(result_type_id=="9f89f4f0-0f71-4702-8139-28f0ac1143b2",recov_cnt,t1)) %>% # decoded
      mutate(t2 = if_else(result_type_id=="096d9e3d-0522-46e0-a3f0-1bb98d3d8bd9",recov_cnt,t2)) %>% # no tag
      mutate(t3 = if_else(result_type_id=="ab45f5b4-9cbe-41e0-b3eb-37a90c8ec6c1",recov_cnt,t3)) %>% # lost tag
      mutate(t4 = if_else(result_type_id=="6874f544-e75c-492b-8a31-df1613dcd0e0",recov_cnt,t4)) %>% # unreadable
      mutate(t7 = if_else(result_type_id=="82067922-4274-436f-a579-c64409340f6c",recov_cnt,t7)) %>% # Discrepancy tags
      mutate(t8 = if_else(result_type_id=="3d7f6258-9d1b-4391-8b0c-ce37df987d0e",recov_cnt,t8)) %>% # no snout
      mutate(t9 = if_else(result_type_id=="cb01d653-5979-43df-9e40-836c991b2036",recov_cnt,t9)) %>% # blank wire
      select(pooled_expansion_id,result_type_id,recov_cnt,t1,t2,t3,t4,t7,t8,t9)
    
    pull_recovs = pull_recovs %>%
      group_by(pooled_expansion_id) %>%
      summarise(t1=sum(t1),t2=sum(t2),t3=sum(t3),t4=sum(t4),t7=sum(t7),
                t8=sum(t8),t9=sum(t9)) %>%
      select(pooled_expansion_id,t1,t2,t3,t4,t7,t8,t9) %>%
      ungroup()
    
    # Check for whether any new adjustments will exist. If not, delete existing records if they exist

    qry = glue::glue(
      "delete from cwts.pooled_adjustment c ",
      "using cwts.pull_recovs r ",
      "where c.pooled_expansion_id = r.pooled_expansion_id::uuid ",
      "and t3+t4+t8=0"
    )
    
    dbWriteTable(db_con,"pull_recovs",pull_recovs)
    dbExecute(db_con, qry, immediate = TRUE)
    dbExecute(db_con,"drop table pull_recovs")
    
    # Calculate the adjustments
    pull_recovs = pull_recovs %>%
      # Initialize the adjustments to 1.000
      mutate(adj3 = format(1.000,nsmall=3),adj4 = format(1.000,nsmall=3),adj8 = format(1.000,nsmall=3)) %>%
      # no snouts
      mutate(adj8 = if_else(t8 > 0 & (t1 + t7 + t4 + t3 + t2 + t9) > 0,
                            format(round((t1 + t7 + t4 + t3 + t2 + t9 + t8) / (t1 + t7 + t4 + t3 + t2 + t9),3),
                                   nsmall = 3),adj8)) %>%
      # Lost tags
      mutate(adj3 = if_else(t3 > 0 & (t1 + t7 + t4 + t9) > 0,
                            format(round((t1 + t7 + t4 + t3 + t9) / (t1 + t7 + t4 + t9),3),
                                   nsmall = 3),adj3)) %>%
      # Unreadable tags
      mutate(adj4 = if_else(t4 > 0 & (t1 + t7 + t9) > 0,
                            format(round((t1 + t7 + t4 + t9) / (t1 + t7 + t9),3),
                                   nsmall = 3),adj4))
    
    # Call the update or insert procedure for adjustments where > 1.000. First, error flag if any calculated value is less than one
    ErrAdjFlg = pull_recovs %>%
      filter(adj3 < 1 | adj4 < 1 | adj8 < 1)
    
    if (nrow(ErrAdjFlg)>0) { # if any adjustments are less than one
      cat("\nPooled_adjustment records exist where adjustment is less than one\n\n")
    } 
    
    # Make current and new temporary tables to hold the calculations
    # current pooled adjustment
    qry = glue::glue(
      "select result_type_id,id as pooled_adjustment_id,",
      "pooled_expansion_id,adjustment_qty ",
      "into current_adj ",
      "from cwts.pooled_adjustment ",
      "where pooled_expansion_id in(select distinct pooled_expansion_id::uuid from cwts.catsam_pooled)",
    )
    
    dbExecute(db_con, qry, immediate = TRUE)
    current_adj = dbReadTable(db_con,"current_adj")
    
    current_adj = current_adj %>%
      mutate(CurrAdj_Qty = format(adjustment_qty,nsmall=3)) %>%
      mutate(CurrAdj_Qty = as.numeric(CurrAdj_Qty)) %>%
      select(result_type_id,pooled_adjustment_id,pooled_expansion_id,CurrAdj_Qty)
    
    # new adjustment records
    new_adj = pull_recovs %>%
      pivot_longer(.,cols = c("adj3","adj4","adj8"),
                   names_to = "cwtresult_id",values_to = "NewAdj_Qty") %>%
      mutate(result_type_id = case_when(cwtresult_id=="adj8" ~ "3d7f6258-9d1b-4391-8b0c-ce37df987d0e",
                                        cwtresult_id=="adj4" ~ "6874f544-e75c-492b-8a31-df1613dcd0e0",
                                        cwtresult_id=="adj3" ~ "ab45f5b4-9cbe-41e0-b3eb-37a90c8ec6c1")) %>%
      mutate(NewAdj_Qty = as.numeric(NewAdj_Qty)) %>%
      select(pooled_expansion_id,result_type_id,NewAdj_Qty)
    
    # combine and compare
    comb_adj = new_adj %>%
      merge(.,current_adj,by=c("pooled_expansion_id","result_type_id"), all = TRUE) %>%
      filter(result_type_id %in% c("3d7f6258-9d1b-4391-8b0c-ce37df987d0e","6874f544-e75c-492b-8a31-df1613dcd0e0","ab45f5b4-9cbe-41e0-b3eb-37a90c8ec6c1"))
    
    # Do inserts
    ins_adj = comb_adj %>%
      filter(is.na(CurrAdj_Qty) & NewAdj_Qty > 1.000)
    
    # sp_CWT_POOLED_ADJUSTMENT_insert; called from sp_expnsn_settagadjpooled
    if (nrow(ins_adj)>0) {
      qry = glue::glue(
        "insert into cwts.pooled_adjustment ",
        "(pooled_expansion_id,result_type_id,adjustment_qty) ",
        "Select pooled_expansion_id::uuid,result_type_id::uuid,NewAdj_Qty ",
        "From cwts.ins_adj")
      
      dbWriteTable(db_con,"ins_adj",ins_adj)
      dbExecute(db_con, qry, immediate = TRUE)
      dbExecute(db_con,"drop table ins_adj")
    }
    
    # Do updates
    upd_adj = comb_adj %>%
      filter(CurrAdj_Qty > 1.000 & NewAdj_Qty > 1.000 & (CurrAdj_Qty != NewAdj_Qty))
    
    # sp_CWT_POOLED_ADJUSTMENT_update; called from sp_expnsn_settagadjpooled
    if (nrow(upd_adj)>0) {
      qry = glue::glue(
        "UPDATE cwts.pooled_adjustment c set ",
        "pooled_expansion_id = u.pooled_expansion_id::uuid, ",
        "result_type_id = u.result_type_id::uuid, ",
        "adjustment_qty = u.NewAdj_Qty ",
        "From cwts.upd_adj u ",
        "where c.id  = u.pooled_adjustment_id::uuid"
      )
      
      dbWriteTable(db_con,"upd_adj",upd_adj)
      dbExecute(db_con, qry, immediate = TRUE)
      dbExecute(db_con,"drop table upd_adj")
    }
    
    # Do deletions
    del_adj = comb_adj %>%
      filter(CurrAdj_Qty > 1.000 & NewAdj_Qty==1.000)
    
    # sp_CWT_POOLED_ADJUSTMENT_delete; called from sp_expnsn_settagadjpooled
    if (nrow(del_adj)>0) {
      qry = glue::glue(
        "delete from cwts.pooled_adjustment c ",
        "using cwts.del_adj r ",
        "where c.id = r.pooled_adjustment_id::uuid",
      )
      
      dbWriteTable(db_con,"del_adj",del_adj)
      dbExecute(db_con, qry, immediate = TRUE)
      dbExecute(db_con,"drop table del_adj")
    }
    
    dbExecute(db_con,"drop table catsam_pooled")
    dbExecute(db_con,"drop table current_adj")
    # end of if pooled stratum exists
  }      
  
# No pooled stratum, call procedures for tag adjustment calc
catsam_nopooled = catsam %>%
  filter(is.na(pooled_expansion_id)) %>%
  select(expansion_id)
#rtnsss = expnsn_settagadjexp(catsam_nopooled)

# Pulls the snout counts,calculates the tag adjustment(s),if any, and calls the insert or update procedure for table expansion_ADJUSTMENT.
# If a expansion_ADJUSTMENT record(s) exists for the ID and no adjustment is needed, the record is deleted.
# If voluntary sport snouts exist for this stratum, then the adjustments reflect both snout count data combined.

# Input:  Expansion ID.  Should be from a expansion record that is a candidate for expansion.
# Output: Return value  1 = no action, no record   0 = update or insert or delete OK  -1 = error
#expnsn_settagadjexp = function(catsam_nopooled) {
  # Check the expansion category, catch value and sample value for the record
  qry = glue::glue(
    "select id,expansion_category_id,catch_count,tag_checked_count ",
    "from cwts.expansion ",
    "where id in(select expansion_id::uuid from cwts.catsam_nopooled)"
  )
  
  dbWriteTable(db_con,"catsam_nopooled",catsam_nopooled)
  catsam_nopooled = DBI::dbGetQuery(db_con, qry)
  dbExecute(db_con,"drop table catsam_nopooled")
  
  # filter out records if expansion category is code 5, 3, or 4 or if catch or sample does not exist with an exp.category code 1 record
  catsam_nopooled = catsam_nopooled %>%
    filter(!expansion_category_id %in% c("6ff1f00b-0446-4bda-a764-87bcebb99e9b","f530a945-402d-4d28-af03-388a460afdfa","dbadacfa-2bce-47b3-ae93-e1851e05fd71") |   # exp cat codes 5,3,4
             (expansion_category_id != "c1644971-08d0-428b-a76c-6a8e06517adc" & (catch_count > 0 |  tag_checked_count > 0) )) # exp cat code <> 2 
                
  # *********THIS SECTION NOT YET DEVELOPED! ONLY USED FOR CATCH\SAMPLE WITH sport fishery AWARENESS FACTORS!***********
  # Determine if a companion expansion category 1 or 2 record exists.  If so, its snout counts
  # must be pulled for the tag adjustment calculation
  # if exists (select 1 from expansion where expansion_id = @expansion_id_input
  #            and fishery_id between 29 and 33  -- PS and FW sport fisheries only
  #            and awareness_qty > 0.000)
  # begin
  # -- Call function to pull the expansion ID for the companion expansion  record 
  # select @companion_id = cwts_dev.dbo.fn_find_sportvol_expansion(@expansion_id_input)
  # if @companion_id <> 0 select @ecat2_flag = 'y'
  
  # Make temporary table to obtain the recovery counts for each result type
  
  qry = glue::glue(
    "select c.expansion_id,c.result_type_id, sum(c.recovery_count) as rcvs ",
    "into cwts.results ",
    "from cwts.result c ",
    "inner join cwts.catsam_nopooled s on c.expansion_id = s.id::uuid ",
    "where c.expansion_id in(select id::uuid from cwts.catsam_nopooled) ",
    "group by c.expansion_id,c.result_type_id")
  
  dbWriteTable(db_con,"catsam_nopooled",catsam_nopooled)
  dbExecute(db_con, qry, immediate = TRUE)
  results = dbReadTable(db_con,"results")
  dbExecute(db_con,"drop table results")
  
  results = results %>%
    # Initialize the counts of each result type to zero
    mutate(t1 = 0L,t2 = 0L,t3 = 0L,t4 = 0L,t7 = 0L,t8 = 0L,t9 = 0L) %>%
    mutate(t1 = if_else(result_type_id=="9f89f4f0-0f71-4702-8139-28f0ac1143b2",rcvs,t1)) %>% # decoded
    mutate(t2 = if_else(result_type_id=="096d9e3d-0522-46e0-a3f0-1bb98d3d8bd9",rcvs,t2)) %>% # no tag                                    
    mutate(t3 = if_else(result_type_id=="ab45f5b4-9cbe-41e0-b3eb-37a90c8ec6c1",rcvs,t3)) %>% # lost tag
    mutate(t4 = if_else(result_type_id=="6874f544-e75c-492b-8a31-df1613dcd0e0",rcvs,t4)) %>% # unreadable
    mutate(t7 = if_else(result_type_id=="82067922-4274-436f-a579-c64409340f6c",rcvs,t7)) %>% # tag discrepancy
    mutate(t8 = if_else(result_type_id=="3d7f6258-9d1b-4391-8b0c-ce37df987d0e",rcvs,t8)) %>% # no snout
    mutate(t9 = if_else(result_type_id=="cb01d653-5979-43df-9e40-836c991b2036",rcvs,t9)) %>% # blank wire
    select(expansion_id,result_type_id,rcvs,t1,t2,t3,t4,t7,t8,t9)
  
 results = results %>%
    group_by(expansion_id) %>%
    summarise(t1=sum(t1),t2=sum(t2),t3=sum(t3),t4=sum(t4),t7=sum(t7),
              t8=sum(t8),t9=sum(t9)) %>%
    select(expansion_id,t1,t2,t3,t4,t7,t8,t9) %>%
    ungroup()
  # Check for no counts; delete all cwt_result records with this exp. id
  # sp_cwt_result_delete; called from sp_expnsn_settagadjexp
  qry = glue::glue(
    "delete from cwts.result c ",
    "using cwts.results r ",
    "where c.expansion_id = r.expansion_id::uuid ",
    "and t1+t2+t3+t4+t7+t8+t9=0"
  )
  
  dbWriteTable(db_con,"results",results)
  dbExecute(db_con, qry, immediate = TRUE)
  
  # else -- Continue with the procedure. Check for whether any new adjustments exist. If not, delete existing records if they exist

  qry = glue::glue(
    "delete from cwts.expansion_adjustment c ",
    "using cwts.results r ",
    "where c.expansion_id = r.expansion_id::uuid ",
    "and t3+t4+t8=0"
  )
  
  dbExecute(db_con, qry, immediate = TRUE)
  dbExecute(db_con,"drop table results")
  
  # Calculate the adjustments
  results = results %>%
    # Initialize the adjustments to 1.000
    mutate(adj3 = format(1.000,nsmall=3),adj4 = format(1.000,nsmall=3),adj8 = format(1.000,nsmall=3)) %>%
    # no snouts
    mutate(adj8 = if_else(t8 > 0 & (t1 + t7 + t4 + t3 + t2 + t9) > 0,
                          format(round((t1 + t7 + t4 + t3 + t2 + t9 + t8) / (t1 + t7 + t4 + t3 + t2 + t9),3),
                                 nsmall = 3),adj8)) %>%
    # Lost tags
    mutate(adj3 = if_else(t3 > 0 & (t1 + t7 + t4 + t9) > 0,
                          format(round((t1 + t7 + t4 + t3 + t9) / (t1 + t7 + t4 + t9),3),
                                 nsmall = 3),adj3)) %>%
    # Unreadable tags
    mutate(adj4 = if_else(t4 > 0 & (t1 + t7 + t9) > 0,
                          format(round((t1 + t7 + t4 + t9) / (t1 + t7 + t9),3),
                                 nsmall = 3),adj4)) %>%
    mutate(adj3 = as.numeric(adj3), adj4 = as.numeric(adj4), adj8 = as.numeric(adj8))
  
  # Call the update or insert procedure for adjustments where > 1.000. First, error flag if any calculated value is less than one
  ErrAdjFlg = results %>%
    filter(adj3 < 1 | adj4 < 1 | adj8 < 1)
  
  if (nrow(ErrAdjFlg)>0) { # if any adjustments are less than one
    cat("\nexpansion_adjustment records exist where adjustment is less than one\n\n")
  } 
  
  # Make current and new temporary tables to hold the calculations
  # current expansion adjustment
  qry = glue::glue(
    "select result_type_id,id as expansion_adjustment_id,",
    "expansion_id,adjustment_qty ",
    "into cwts.current_adj ",
    "from cwts.expansion_adjustment ",
    "where expansion_id in(select distinct expansion_id::uuid from cwts.catsam_nopooled)"
  )
  
  dbExecute(db_con, qry, immediate = TRUE)
  current_adj = dbReadTable(db_con,"current_adj")
  dbExecute(db_con,"drop table catsam_nopooled")
  
  current_adj = current_adj %>%
    mutate(curradj_qty = format(adjustment_qty,nsmall=3)) %>%
    mutate(curradj_qty = as.numeric(curradj_qty)) %>%
    select(expansion_adjustment_id,result_type_id,expansion_id,curradj_qty)
  
  # new adjustment records
  new_adj = results %>%
    pivot_longer(.,cols = c("adj3","adj4","adj8"),
                 names_to = "cwtresult_id",values_to = "newadj_qty") %>%
    mutate(result_type_id = case_when(cwtresult_id=="adj8" ~ "3d7f6258-9d1b-4391-8b0c-ce37df987d0e",       # no snout
                                      cwtresult_id=="adj4" ~ "6874f544-e75c-492b-8a31-df1613dcd0e0",       # unreadable
                                      cwtresult_id=="adj3" ~ "ab45f5b4-9cbe-41e0-b3eb-37a90c8ec6c1")) %>%  # lost tag
    mutate(newadj_qty = as.numeric(newadj_qty)) %>%
    select(expansion_id,result_type_id,newadj_qty)
  
  # combine and compare
  comb_adj = new_adj %>%
    merge(.,current_adj,by=c("expansion_id","result_type_id"), all = TRUE) %>%
    filter(result_type_id %in% c("3d7f6258-9d1b-4391-8b0c-ce37df987d0e","6874f544-e75c-492b-8a31-df1613dcd0e0","ab45f5b4-9cbe-41e0-b3eb-37a90c8ec6c1"))
  
  # Do inserts
  ins_adj = comb_adj %>%
    filter(is.na(curradj_qty) & newadj_qty > 1.000)
  
  # sp_expansion_ADJUSTMENT_insert; called from sp_expnsn_settagadjexp
  if (nrow(ins_adj)>0) {
    qry = glue::glue(
      "insert into cwts.expansion_adjustment ",
      "(expansion_id,result_type_id,adjustment_qty) ",
      "Select expansion_id,result_type_id,newadj_qty ",
      "From cwts.ins_adj")
    
    dbWriteTable(db_con,"ins_adj",ins_adj)
    dbExecute(db_con, qry, immediate = TRUE)
    dbExecute(db_con,"drop table ins_adj")
  }
  
  # Do updates
  upd_adj = comb_adj %>%
    filter(curradj_qty > 1.000 & newadj_qty > 1.000 & (curradj_qty != newadj_qty))
  
  # sp_expansion_ADJUSTMENT_update; called from sp_expnsn_settagadjexp
  if (nrow(upd_adj)>0) {
    qry = glue::glue(
      "UPDATE cwts.expansion_adjustment c set ",
      "expansion_id = u.expansion_id::uuid, ",
      "result_type_id = u.result_type_id::uuid, ",
      "adjustment_qty = u.newadj_qty ",
      "from cwts.upd_adj u where ",
      "c.id  = u.expansion_adjustment_id::uuid"
    )
    
    dbWriteTable(db_con,"upd_adj",upd_adj)
    dbExecute(db_con, qry, immediate = TRUE)
    dbExecute(db_con,"drop table upd_adj")
  }
  
  # Do deletions
  del_adj = comb_adj %>%
    filter(curradj_qty > 1.000 & newadj_qty==1.000)
  
  # sp_expansion_ADJUSTMENT_delete; called from sp_expnsn_settagadjexp
  if (nrow(del_adj)>0) {
    qry = glue::glue(
      "delete from cwts.expansion_adjustment c ",
      "using cwts.del_adj r ",
      "where c.id = r.expansion_adjustment_id::uuid",
    )
    
    dbWriteTable(db_con,"del_adj",del_adj)
    dbExecute(db_con, qry, immediate = TRUE)
    dbExecute(db_con,"drop table del_adj")
  }
  
  dbExecute(db_con,"drop table current_adj")

#-------------------------------------------------------------------------------------------------------
#     Calculate Expansion Factor and Update expansion (except exp.cat.2)
#-------------------------------------------------------------------------------------------------------

catsamnot2 = catsam %>%
  filter(expansion_category_id != "c1644971-08d0-428b-a76c-6a8e06517adc") %>%  # exclude voluntary recoveries
  select(expansion_id)

#===================================================================================================
# Expansion factor calculation
# Gathers the catch,number of tags checked, awareness(if applicable),pooled and tag adjustment data
# associated with each expansion record and computes the expansion factor.
# Input:  expansion_id for each record.
# Output: expansion factor for each record
#===================================================================================================

  # Pull data for the expansion record, pull from pooled data and retrieve adjustments
  qry = glue::glue(
    "select distinct e.id,p.id as pooled_expansion_id,p.pooled_catch_count,p.pooled_tag_checked_count,",
    "e.awareness_qty,c.adjustment_qty,c.result_type_id ",
    "from cwts.expansion e INNER JOIN cwts.pooled_expansion p ",
    "on e.pooled_expansion_id = p.id LEFT JOIN cwts.pooled_adjustment c ",
    "on p.id = c.pooled_expansion_id ",
    "where e.id in(select distinct expansion_id::uuid from cwts.catsamnot2) and ",
    "pooled_data_indicator = TRUE"
  )
  
  dbWriteTable(db_con,"catsamnot2",catsamnot2)
  catsamnot2pooled = DBI::dbGetQuery(db_con, qry)
  
  # Return if catch or sample is zero or null (should not be getting exp.cat.2 records)
  Err = catsamnot2pooled %>%
    filter(is.na(pooled_catch_count) | pooled_catch_count <= 0 | pooled_tag_checked_count <= 0)
  
  if (nrow(Err)>0) { 
    cat("\nCalc expf result: exp records exist with no catch or sample\n\n")
    
  }
  # Error return if awareness exists with pooled
  Err = catsamnot2pooled %>%
    filter(!is.na(awareness_qty))
  if (nrow(Err)>0) { 
    cat("\nExp. factor calc: Awareness exists with pooled record\n\n")
  }
  
  catsamnot2pooled = catsamnot2pooled %>%
    select(expansion_id=id,catch_count=pooled_catch_count,tag_checked_count=pooled_tag_checked_count,
           awareness_qty,result_type_id,adjustment_qty)
  
  # Pull from regular data and check for adjustments
  qry = glue::glue(
    "select distinct e.id,e.pooled_expansion_id,catch_count,tag_checked_count,",
    "awareness_qty,c.adjustment_qty,c.result_type_id ",
    "from cwts.expansion e left JOIN cwts.expansion_adjustment c ",
    "on e.id = c.expansion_id ",
    "where e.id in(select distinct expansion_id::uuid from cwts.catsamnot2) and ",
    "pooled_data_indicator = FALSE and (catch_count > 0) and ",
    "(tag_checked_count > 0)"
  )
  
  catsamnot2nopool = DBI::dbGetQuery(db_con, qry)
  dbExecute(db_con,"drop table catsamnot2")
  
  # Error return if Pooled ID present where ind = 0
  Err = catsamnot2nopool %>%
    filter(!is.na(pooled_expansion_id))
  if (nrow(Err)>0) { 
    cat("\nExp. factor calc: Pooled ID present where ind = 0\n\n")
  }
  # Return if catch or sample is zero or null (should not be getting exp.cat.2 records)
  # Err = catsamnot2nopool %>%
  #   filter(is.na(catch_count) | catch_count <= 0 | tag_checked_count <= 0)
  # 
  # if (nrow(Err)>0) { 
  #   return(cat("\nCalc expf result: exp records exist with no catch or sample\n\n"))
  #   
  # }
  # 
  catsamnot2nopool = catsamnot2nopool %>%
    select(expansion_id=id,catch_count,tag_checked_count,awareness_qty,result_type_id,adjustment_qty)
  
  # combine pooled and not pooled records
  catsamnot2final = rbind(catsamnot2pooled,catsamnot2nopool)
  
  # setup for potential missing column names in pivot below  
  cols = c("adj3","adj4","adj8")
  
  catsamnot2final = catsamnot2final %>%
    mutate(adjustment_qty = if_else(is.na(adjustment_qty),1.000,adjustment_qty)) %>%
    mutate(adjtype = case_when(result_type_id=="3d7f6258-9d1b-4391-8b0c-ce37df987d0e" ~ "adj8",       # no snout
                               result_type_id=="6874f544-e75c-492b-8a31-df1613dcd0e0" ~ "adj4",       # unreadable
                               result_type_id=="ab45f5b4-9cbe-41e0-b3eb-37a90c8ec6c1" ~ "adj3")) %>%  # lost tag
    pivot_wider(id_cols=c("expansion_id","catch_count","tag_checked_count","awareness_qty"),names_from = "adjtype", values_from = "adjustment_qty")
  
  # add any missing adj columns as a result of pivot; needed for expansion function 
  catsamnot2final[cols[!(cols %in% colnames(catsamnot2final))]] = 1.000 #NA_real_
  
  catsamnot2final = catsamnot2final %>%
    select(expansion_id,catch_count,tag_checked_count,awareness_qty,adj8,adj3,adj4)
  
  # Call function to calculate the expansion factor
  # fn_calculate_expfactor
  # Function uses the following formula to calculate a recovery expansion factor:
  # ( (Catch) / (Sample + ((Catch-Sample) x Awareness ) ) x Adj No Snts x Adj Lost Tags x Adj Unreadable Tags
  #   
  #   Where values do not exist, the following defaults apply:
  #     Awareness = 0.000
  #   Adjustments = 1.000
  
  catsamnot2final = catsamnot2final %>%
    mutate(awareness_qty = if_else(is.na(awareness_qty),0.000,awareness_qty)) %>%
    mutate(adj8 = if_else(is.na(adj8),1.000,adj8)) %>%
    mutate(adj3 = if_else(is.na(adj3),1.000,adj3)) %>%
    mutate(adj4 = if_else(is.na(adj4),1.000,adj4)) %>%
    mutate(expnsn_factor = format(round(adj8 * adj3 * adj4 *(catch_count/(tag_checked_count +
                                                                            ((catch_count - tag_checked_count) * awareness_qty))),2))) %>%
    select(expansion_id,expnsn_factor)
  
# If expansion factor is greater than 9999.00, revise to 9999.00
catsamnot2final$expnsn_factor[catsamnot2final$expnsn_factor > 9999.00] = 9999.00

# Make a straight update transaction (this will affect the modification fields, but not write to history)
# Only update if the existing number does not match the new number
catsam1 = catsam %>%
  left_join(.,catsamnot2final,by=c("expansion_id"))

qry = glue::glue(
  "UPDATE cwts.expansion c set ",
  "expansion_factor_qty = u.expnsn_factor::numeric ",
  "from cwts.catsamnot2final u ",
  "where c.id  = u.expansion_id::uuid ",
  "and (c.expansion_factor_qty <> u.expnsn_factor::numeric or ",
  "(c.expansion_factor_qty is null and u.expnsn_factor is not null) or ",
  "(c.expansion_factor_qty is not null and u.expnsn_factor is null))"
)

dbWriteTable(db_con,"catsamnot2final",catsamnot2final)
dbExecute(db_con, qry, immediate = TRUE)
dbExecute(db_con,"drop table catsamnot2final")

# ===========================================================================
#  Set Expansion Factor for Sport Expansion Category 2
#  and Exp.Cat. 1 with no catch
#  THIS SECTION NOT YET DEVELOPED!!!!!!<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#  NO EXPANSION RECORDS EXIST IN THIS CATEGORY AFTER 2000
#  sp_expnsn_setsportexpansion; called from sp_expnsn_driver

#=======================================================================================
#  Check for changed records
#=======================================================================================
# Make a temporary table of the updated records
qry = glue::glue(
  "select e.id as expansion_id,catch_count,tag_checked_count,snout_count,mark_checked_count,",
  "expansion_factor_qty,awareness_qty,expansion_category_id ", #adj8 = NULL, adj3 = NULL, adj4 = NULL,
  "from cwts.expansion e ",
  "inner join cwts.fishery_lut f on e.fishery_id = f.id ",
  "inner join cwts.species_lut s on e.species_id = s.id ",
  "where run_year = {runyr} and f.code = {fsh} and s.cwt_species_code = {spc}"
)

new_expansion = DBI::dbGetQuery(db_con, qry)

qry = glue::glue(
  "select expansion_id,sum(recovery_count) as snt_count ",
  "from cwts.result ", #adj8 = NULL, adj3 = NULL, adj4 = NULL,
  "where expansion_id in(select distinct expansion_id::uuid from compare) ",
  "group by expansion_id"
)
dbWriteTable(db_con,"compare",compare)
new_results = DBI::dbGetQuery(db_con, qry)

new_expansion = new_expansion %>%
  left_join(.,new_results,by=c("expansion_id")) %>%
  mutate(snout_count = snt_count) %>%
  mutate(adj8 = NA_real_,adj3 = NA_real_,adj4 = NA_real_)

qry = glue::glue(
  "update cwts.new_expansion c set adj8 = adjustment_qty ",
  "from cwts.expansion_adjustment a where a.expansion_id = c.expansion_id::uuid ",
  "and a.result_type_id = '3d7f6258-9d1b-4391-8b0c-ce37df987d0e'"   # no snout
)
dbWriteTable(db_con,"new_expansion",new_expansion)
dbExecute(db_con, qry, immediate = TRUE)
qry = glue::glue(
  "update cwts.new_expansion c set adj3 = adjustment_qty ",
  "from cwts.expansion_adjustment a where a.expansion_id = c.expansion_id::uuid ",
  "and a.result_type_id = 'ab45f5b4-9cbe-41e0-b3eb-37a90c8ec6c1'"   # lost tag
)
dbExecute(db_con, qry, immediate = TRUE)
qry = glue::glue(
  "update cwts.new_expansion c set adj4 = adjustment_qty ",
  "from cwts.expansion_adjustment a where a.expansion_id = c.expansion_id::uuid ",
  "and a.result_type_id = '6874f544-e75c-492b-8a31-df1613dcd0e0'"   # unreadable tag
)
dbExecute(db_con, qry, immediate = TRUE)
new_expansion = dbReadTable(db_con,"new_expansion")

# Write IDs of changed records to temp.table
qry = glue::glue(
  "select c.expansion_id into changed ",
  "from cwts.compare c INNER JOIN cwts.new_expansion e ON c.expansion_id::uuid = e.expansion_id::uuid ",
  "where (c.tag_checked_count <> e.tag_checked_count) ",
  "or (c.snout_count <> e.snout_count) ",
  "or (c.mark_checked_count <> e.mark_checked_count) ",
  "or (c.expansion_factor_qty <> e.expansion_factor_qty) ",
  "or (c.expansion_factor_qty is null and e.expansion_factor_qty is not null) ",
  "or (c.expansion_factor_qty is not null and e.expansion_factor_qty is null) ",
  "or (c.awareness_qty <> e.awareness_qty) ",
  "or (c.awareness_qty is null and  e.awareness_qty is not null) ",
  "or (c.awareness_qty is not null and  e.awareness_qty is null) ",
  "or (c.adj8 <> e.adj8) ",
  "or (c.adj8 is null and e.adj8 is not null) or (c.adj8 is not null and e.adj8 is null) ",
  "or (c.adj3 <> e.adj3) ",
  "or (c.adj3 is null and e.adj3 is not null) or (c.adj3 is not null and e.adj3 is null) ",
  "or (c.adj4 <> e.adj4) ",
  "or (c.adj4 is null and e.adj4 is not null) or (c.adj4 is not null and e.adj4 is null)" 
)

dbExecute(db_con, qry, immediate = TRUE)
changed = dbReadTable(db_con,"changed")

if(nrow(changed)>0) {
  qry = glue::glue(
    "select c.expansion_id,run_year,fishery_id,species_id,c.expansion_category_id as ecat,",
    "e.catch_count as oldnewcatch,",
    "c.tag_checked_count as oldsampl, e.tag_checked_count as newsampl,",
    "c.snout_count as oldsnts, e.snout_count as newsnts,",
    "c.expansion_factor_qty as oldexpf,e.expansion_factor_qty as newexpf,",
    "c.adj8 as oldadj8, e.adj8 as newadj8,",
    "c.adj3 as oldadj3, e.adj3 as newadj3,",
    "c.adj4 as oldadj4, e.adj4 as newadj4,",
    "c.awareness_qty as oldawar, e.awareness_qty as newawar ",
    "from cwts.compare c INNER JOIN cwts.new_expansion e ON c.expansion_id::uuid = e.expansion_id::uuid ",
    "where c.expansion_id::uuid in (select expansion_id::uuid from changed)" 
  )
  changes = DBI::dbGetQuery(db_con, qry)
  cat("\nExpansion records with changes\n\n")
  print(changes)
  dbExecute(db_con,"drop table new_expansion")
  dbExecute(db_con,"drop table compare")
  dbExecute(db_con,"drop table changed")
  
  # Need to implement sp_maint_driver_cwtexpansion here to update snout_count in cwt_expansion!!  sp_maint_driver_cwtexpansion 
  changesntcnt = changes %>%
    filter(oldsnts != newsnts) %>%
    select(expansion_id,oldsnts,newsnts)
  
  if (nrow(changesntcnt)>0) {
    qry = glue::glue(
      "update cwts.cwt_expansion a set snout_count = c.newsnts ",
      "from cwts.changesntcnt c where a.id= c.expansion_id::uuid"
    )
    
    dbWriteTable(db_con,"changesntcnt",changesntcnt)
    dbExecute(db_con, qry, immediate = TRUE) 
    dbExecute(db_con,"drop table changesntcnt")
    cat("\nSnout counts updated\n\n")
  } else {
    cat("\nNo snout count changes\n\n")
  }
  
} else {
  cat("\nNo expansion records with changes\n\n")
  dbExecute(db_con,"drop table new_expansion")
  dbExecute(db_con,"drop table compare") 
  dbExecute(db_con,"drop table changed")
}
 poolClose(db_con) 


