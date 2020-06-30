import os, sys
import pandas as pd
import numpy as np
import redcap
import synapseclient
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, Numeric, String, Date, MetaData, ForeignKey
from sqlalchemy import inspect

# Import data from redcap
from redcap import Project
api_url = os.environ['REDCAP_API_URL']
api_key = os.environ['REDCAP_API_KEY']
project = Project(api_url, api_key)

data = project.export_records(format='df', raw_or_label='label', export_checkbox_labels=True) 

metadata = MetaData()

# Create the database
engine = sqlalchemy.create_engine("postgres://postgres:3gq9luzciG6ZRxKhNW7p@/stepdb")
metadata.create_all(engine)
conn = engine.connect()
conn.execute("commit")

intake_form = data[['registry_enrollment_date', 'initial_office_visit', 'mrn', 'dob', 'dod', 'sex', 'race', 'ethnicity']]
intake_form = intake_form.dropna(how="all")
intake_form['study_id'] = [x[0] for x in intake_form.index]
intake_form.to_sql("SARCOMA_REGISTRY_INTAKE_FORM", con=engine, if_exists="replace", index=False)


initial_staging_form = data[['date_diagnosis', 'date_of_diagnosis_act', 'tumor_histology', 'histology_other', 'bone_or_soft', 'primary_site1', 'primary_subsite', 'primary_lat', 'tumor_size', 'tumor_depth', 'dx_mets', 'mets_num_sit', 'single_mets', 'symptoms', 'sym_dur', 'ajcc_version', 'ajcc_other', 'staging_type_new', 'sts_ajcc8', 'bone_ajcc8', 'date_int_stage', 't_stage_new', 'n_stage_new', 'm_stage_new', 'g_stage_new', 'cancer_stage_new']]
initial_staging_form = initial_staging_form.dropna(how="all")
initial_staging_form['study_id'] = [x[0] for x in initial_staging_form.index]
initial_staging_form.to_sql("INITIAL_STAGING_FORM", con=engine, if_exists="replace", index=False)

follow_up = data[['current_date_of_assessment', 'missing_dates', 'following_patient', 'date_lost_followup']]
follow_up = follow_up.dropna(how="all")
follow_up['study_id'] = [x[0] for x in follow_up.index]
follow_up.to_sql("FOLLOW_UP", con=engine, if_exists="replace", index=False)

# Pathology events, this will actually need to be cleaned to be useful
pathology_events = data[['path_event_acq_date1', 'path_spec_type1', 'path_other_spec_type1', 'path_sample_type', 'specimen_source1', 'spec_source_other1', 'path_source', 'path_accession_num1', 'path_accession_outside', 'path_accession_outside_dept', 'path_consensus1', 'path_diag_comment', 'path_review_change1', 'synoptic_report1', 'uterine_sr', 'sr_multifocality1', 'fnclcc_grade1', 'sr_lymph_invasion1', 'sr_vas_invasion1', 'sr_perineural_invasion1', 'sr_tumor_size1', 'sr_percent_necrosis1', 'sr_mitotic_activity1', 'sr_depth1', 'sr_surgical_margins1', 'sr_lymph_nodes1', 'ptnm1', 'sr_u_tumor_location', 'sr_u_mitotic_rate', 'sr_u_spec_integ', 'sr_u_tumor_size', 'sr_u_hist_grade', 'sr_u_endocerv_invovlment', 'sr_u_myometrial_invasion', 'sr_u_depth_invasion', 'sr_u_myometrial_thick', 'sr_u_serosa_involve', 'sr_u_lower_uterine_inv', 'sr_u_cervical_stromal_inv', 'sr_u_vaginal_involve', 'sr_u_right_ovary_involve', 'sr_u_left_ovary_involve', 'sr_u_rt_fallopian_invove', 'sr_u_lt_fallopian_invove', 'sr_u_other_sites', 'sr_u_margins', 'sr_u_lymph_nodes', 'sr_u_lymphvasc', 'sr_u_path_staging', 'ihc_on_sample1', 'pos_ae13_1', 'pos_beta_catenin_1', 'cam5_2_1', 'cd3_1', 'cd20_1', 'pos_cd31_1', 'pos_cd34_1', 'pos_cd45_1', 'pos_cd99_1', 'cd117_1', 'cd138_1', 'pos_cdk4_1', 'cdx2_1', 'ck5_6_1', 'pos_claudin1_1', 'pos_desmin_1', 'pos_dog1_1', 'pos_ema_1', 'pos_er', 'pos_erg_1', 'pos_fli1_1', 'pos_gfap_1', 'pos_hcaldesmon_1', 'pos_hmb45_1', 'pos_ini1_1', 'pos_ki67_1', 'pos_mdm2_1', 'melan_a_1', 'pos_mnf116_1', 'pos_muc4_1', 'myod1_1', 'pos_myogenin_1', 'pos_nyeso1_1', 'p63_1', 'pos_pd1_1', 'pos_pdl1_1', 'pos_pr', 'pos_s100_1', 'pos_satb2_1', 'pos_sma_1', 'pos_sox10_1', 'pos_sox9_1', 'pos_stat6_1', 'pos_tfe_3_1', 'pos_tle1_1', 'pos_vimentin_1', 'pos_wt1_1', 'date_path_update']]

pathology_events = pathology_events.dropna(how="all")
pathology_events['study_id'] = [x[0] for x in pathology_events.index]
pathology_events.to_sql("PATHOLOGY_EVENTS", con=engine, if_exists="replace", index=False)

# Let's get drug events
drug_events = data[['drug1_chemo_start', 'num_chem_drug', 'drug1', 'chemo_drug1_other', 'drug2', 'chemo_drug2_other', 'drug3', 'chemo_drug3_other', 'drug4', 'chemo_drug4_other', 'drug5', 'chemo_drug5_other', 'drug6', 'chemo_drug6_other', 'drug7', 'chemo_drug7_other', 'drug8', 'chemo_drug8_other', 'drug9', 'chemo_drug9_other', 'drug10', 'chemo_drug10_other', 'chemo_disc', 'rsn_chem_disc___1', 'rsn_chem_disc___2', 'rsn_chem_disc___3', 'rsn_chem_disc___4', 'rsn_chem_disc___5', 'rsn_chem_disc___6', 'chemo_end_date_known', 'drug1_chemo_stop', 'chemo_end_date_2', 'best_response']]
drug_events = drug_events.dropna(how="all")
drug_events['study_id'] = [x[0] for x in drug_events.index]


# This should be converted to a relational format before we save it
def drugEventConvertToRelational(row):
    drug_no = row['num_chem_drug']
    if np.isnan(drug_no):
        return(None)
    drug_rowname = "drug" + str(int(drug_no))
    if row[drug_rowname] != "Other":
        drug = row[drug_rowname]
    else: 
        drug = row["chemo_" + drug_rowname + "_other"]
    return(drug_no, drug, row['drug1_chemo_start'], row['chemo_disc'], row['rsn_chem_disc___1'], row['rsn_chem_disc___2'], row['rsn_chem_disc___3'], row['rsn_chem_disc___4'], row['rsn_chem_disc___5'], row['rsn_chem_disc___6'], row['chemo_end_date_known'], row['drug1_chemo_stop'], row['best_response'])
    
colnames = ["drug_number", "drug_name", "chemo_start", "discontinued", "disc_reason-1", "disc_reason-2", "disc_reason-3", "disc_reason-4", "disc_reason-5", "disc_reason-6", "end_date_known", "stop_date", "response"]

drug_events = drug_events.apply(drugEventConvertToRelational, axis=1, result_type='expand')
drug_events.columns = colnames
drug_events['study_id'] = [x[0] for x in drug_events.index]

drug_events.to_sql("CHEMOTHERAPY_TREATMENT_EVENTS", con=engine, if_exists="replace", index=False)


# Pull data from SCARLET (Synapse cached)
syn = synapseclient.Synapse()
syn.login()

scarlet_people = pd.read_csv(syn.get("syn19166903").path)
scarlet_people.to_sql("SCARLET_PEOPLE", con=engine, if_exists="replace", index=False)

scarlet_reports = pd.read_csv(syn.get("syn19166901").path)
scarlet_reports.to_sql("SCARLET_REPORTS", con=engine, if_exists="replace", index=False)

scarlet_genes = pd.read_csv(syn.get("syn19166902").path)
scarlet_genes.to_sql("SCARLET_GENES", con=engine, if_exists="replace", index=False)

scarlet_cnas = pd.read_csv(syn.get("syn19166612").path)
scarlet_cnas.to_sql("SCARLET_CNAS", con=engine, if_exists="replace", index=False)

scarlet_alterations = pd.read_csv(syn.get("syn20504231").path, encoding="ISO-8859-1")
scarlet_alterations.to_sql("SCARLET_ALTERATIONS", con=engine, if_exists="replace", index=False)

scarlet_variant_reports = pd.read_csv(syn.get("syn20632383").path, encoding="ISO-8859-1")
scarlet_variant_reports.to_sql("SCARLET_VARIANT_REPORTS", con=engine, if_exists="replace", index=False)

scarlet_rearrangement = pd.read_csv(syn.get("syn20812274").path, encoding="ISO-8859-1")
scarlet_rearrangement.to_sql("SCARLET_REARRANGEMENT", con=engine, if_exists="replace", index=False)

scarlet_rearrangement = pd.read_csv(syn.get("syn20826645").path, encoding="ISO-8859-1")
scarlet_rearrangement.to_sql("SCARLET_VARIANT_PROPERTY", con=engine, if_exists="replace", index=False)
