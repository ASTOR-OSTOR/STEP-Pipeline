library(tidyverse)
library(openssl)
library(DBI)
library(RPostgreSQL)
library(lubridate)

# Reads data from the local postgres DB and outputs it into CSV for app consumption

getData <- function() {
  pg <- dbDriver("PostgreSQL")
  con <- dbConnect(pg,
                   user="postgres",
                   password="",
                   host="localhost",
                   port=5432,
                   dbname="stepdb")
  
  # Pull tables from DB
  patients <- dbReadTable(con, "SARCOMA_REGISTRY_INTAKE_FORM")
  initialstaging <- dbReadTable(con, "INITIAL_STAGING_FORM")
  treatments <- dbReadTable(con, "CHEMOTHERAPY_TREATMENT_EVENTS")
  followup <- dbReadTable(con, "FOLLOW_UP")
  
  variantReports <- dbReadTable(con, "SCARLET_VARIANT_REPORTS")
  cnas <- dbReadTable(con, "SCARLET_CNAS")
  alterations <- dbReadTable(con, "SCARLET_ALTERATIONS")
  genes <- dbReadTable(con, "SCARLET_GENES")
  reports <- dbReadTable(con, "SCARLET_REPORTS")
  variantProperties <- dbReadTable(con, "SCARLET_VARIANT_PROPERTY")
  people <- dbReadTable(con, "SCARLET_PEOPLE")
  
  
  # At this point we no longer need the connection
  dbDisconnect(con)
  
  # The variant properties table has info about about VUS status, so let's join it
  reports
  
  # Table of patient data (one row per patient)
  patients <- patients %>% 
    inner_join(people, by=c("mrn"="person_source_value")) %>% 
    inner_join(initialstaging) %>% 
    inner_join(followup)
  
  person_to_study_ids <- patients %>% select(person_id, study_id) %>% distinct()
  
  
  treatments <- treatments %>% 
    left_join(followup %>% # For patients still on therapy, we need their date of last followup
                select(study_id, date_lost_followup)) %>%
    mutate(days_on_drug = if_else(discontinued == "Yes",
                                     (ymd(stop_date) - ymd(chemo_start)),
                                     (ymd(date_lost_followup) - ymd(chemo_start)))
              )
  print(paste("Dropping ", nrow(treatments %>% filter(is.na(days_on_drug))), " treatment records with NA survival"))
  print(paste("Dropping ", nrow(treatments %>% filter(days_on_drug < 0)), " treatment records with negative survival"))
  treatments <- treatments %>% filter(days_on_drug > 0)

  treatments <- treatments %>% inner_join(person_to_study_ids)

  # Table of mutations
  mutations <- patients %>%
    select(person_id) %>% 
    inner_join(reports, by="person_id") %>% 
    inner_join(genes, by="final_report_id") %>% 
    inner_join(alterations, by="gene_id", suffix=c("_gene", "_alteration")) %>% 
    # left_join(variantProperties, by=c("name_gene"="gene_name", "name_alteration"="variant_name")) %>%
    filter(!(name_gene %in% c("Tumor Mutation Burden", "Microsatellite status"))) %>% 
    filter(!(name_alteration %in% c("amplification", "loss")))

  # Table of CNAs
  copynumbers <- patients %>% 
    select(person_id) %>% 
    inner_join(reports, by="person_id") %>% 
    inner_join(variantReports, by=c('final_report_id' = 'test_request')) %>% 
    filter(quality_control_status != "Fail") %>% 
    inner_join(cnas, by='variant_report_id') %>% 
    left_join(variantProperties, by=c("gene"="gene_name", "type"="variant_name")) %>% 
    filter(!is.na("gene"))
  
  # Cleaning and separating other data
  # other_data <- mutations %>% 
  #   filter((name_gene %in% c("Tumor Mutation Burden", "Microsatellite status"))) %>% 
  #   select(person_id, name_gene, name_alteration) %>% 
  #   distinct() %>% 
  #   spread(name_gene, name_alteration)
  
  # Convert the date of birth to an age
  patients$age <- floor((Sys.Date() - as.Date(patients$dob)) / 365)
  
  randomSeed = sample(1:99999999999999, 1)
  
  # Get just the columns we need. We can save this and upload it.
  patients <<- patients %>%
    mutate(person_id = sha256(paste(person_id, randomSeed))) %>% 
    select(person_id, sex, tumor_histology, age, dod, cancer_stage_new, following_patient, date_lost_followup, date_diagnosis, following_patient)
  
  mutations <<- mutations %>%
    mutate(person_id = sha256(paste(person_id, randomSeed)))
  
  copynumbers <<- copynumbers %>%
    mutate(person_id = sha256(paste(person_id, randomSeed)))
  
  treatments <<- treatments %>%
    mutate(person_id = sha256(paste(person_id, randomSeed)))
  
}

getData()
dir.create(file.path('.', 'data'))
write_csv(mutations, "./data/deidentified_mutations.csv")
write_csv(copynumbers, ".//data/deidentified_copynumbers.csv")
write_csv(patients, "./data/deidentified_patients.csv")
write_csv(treatments, "./data/deidentified_treatments.csv")
