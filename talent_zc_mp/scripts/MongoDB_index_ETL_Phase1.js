
/* Collections Involved - candidate , requisition, candiate_resume_text, requisition_candidate, vendor, requisition_candidate_division, client, configuration_settings

*/


// ### Collection: candidate ###
// indx_name = "candidate_CandidateId_UK1"
db.candidate.createIndex( 
{ 
  "candidate_id" : 1
}, 
{ 
  "name" : "candidate_CandidateId_UK1", 
  "unique" : true, 
  "background" : true
}
);

// indx_name = "candidate_MasterSupplierId_IDX1"
db.candidate.createIndex( 
{ 
  "master_supplier_id" : 1
}, 
{ 
  "name" : "candidate_MasterSupplierId_IDX1", 
  "background" : true
}
);

// indx_name = "candidate_LoadedDate_IDX2"
db.candidate.createIndex( 
{ 
  "loaded_date" : -1
}, 
{ 
  "name" : "candidate_LoadedDate_IDX2", 
  "background" : true
}
);

// indx_name = "candidate_UpdateDate_IDX3"


db.candidate.createIndex( 
{ 
  "update_date" : -1
}, 
{ 
  "name" : "candidate_UpdateDate_IDX3", 
  "background" : true
}
);

// indx_name = "candidate_EtlProcessFlag_IDX4"
db.candidate.createIndex( 
{ 
  "etl_process_flag" : 1
}, 
{ 
  "name" : "candidate_EtlProcessFlag_IDX4", 
  "background" : true
}
);

// ### Collection: requisition ###
// indx_name="requisition_RequisitionId_UK1"
db.requisition.createIndex( 
{ 
  "requisition_id" : 1 
}, 
{ 
  "name" : "requisition_RequisitionId_UK1", 
  "unique" : true,
  "background" : true
}
);

// indx_name="requisition_GlobalJobCategoryId_RequisitionId_IDX1"
db.requisition.createIndex( 
{ 
  "new_global_job_category_id" : 1 ,
  "requisition_id" : 1
}, 
{ 
  "name" : "requisition_GlobalJobCategoryId_RequisitionId_IDX1", 
  "background" : true
}
);

// indx_name="requisition_RequisitionId_StatusID_PreReq_IDX2"
db.requisition.createIndex( 
{ 
  "requisition_id" : 1,
  "status_id" : 1,
  "pre_identified_req" : 1
}, 
{ 
  "name" : "requisition_RequisitionId_StatusID_PreReq_IDX2", 
  "background" : true
}
);

// indx_name = "requisition_LoadedDate_IDX3"
db.requisition.createIndex( 
{ 
  "loaded_date" : -1
}, 
{ 
  "name" : "requisition_LoadedDate_IDX3", 
  "background" : true
}
);

// indx_name = "requisition_UpdateDate_IDX4"
db.requisition.createIndex( 
{ 
  "update_date" : -1
}, 
{ 
  "name" : "requisition_UpdateDate_IDX4", 
  "background" : true
}
);

// indx_name = "requisition_EtlProcessFlag_IDX5"
db.requisition.createIndex( 
{ 
  "etl_process_flag" : 1
}, 
{ 
  "name" : "requisition_EtlProcessFlag_IDX5", 
  "background" : true
}
);


// ### Collection: candiate_resume_text ###
// indx_name = "candidate_resume_text_CandidateID_UK1"
db.candidate_resume_text.createIndex( 
{ 
  "candidate_id" : 1
}, 
{ 
  "name" : "candidate_resume_text_CandidateID_UK1", 
  "unique" : true, 
  "background" : true
}
);

// indx_name = "candidate_resume_text_LoadedDate_IDX1"
db.candidate_resume_text.createIndex( 
{ 
  "loaded_date" : -1
}, 
{ 
  "name" : "candidate_resume_text_LoadedDate_IDX1", 
  "background" : true
}
);

// indx_name = "candidate_resume_text_UpdateDate_IDX2"
db.candidate_resume_text.createIndex( 
{ 
  "update_date" : -1
}, 
{ 
  "name" : "candidate_resume_text_UpdateDate_IDX2", 
  "background" : true
}
);

// indx_name = "candidate_resume_text_EtlProcessFlag_IDX3"
db.candidate_resume_text.createIndex( 
{ 
  "etl_process_flag" : 1
}, 
{ 
  "name" : "candidate_resume_text_EtlProcessFlag_IDX3", 
  "background" : true
}
);


// ### Collection: requisition_candidate ###
// indx_name="requisition_candidate_RequisitionId_CandidateId_UK1"
db.requisition_candidate.createIndex( 
{ 
  "requisition_id" : 1,
  "candidate_id" : 1
}, 
{ 
  "name" : "requisition_candidate_RequisitionId_CandidateId_UK1", 
  "unique" : true,
  "background" : true
}
);

// indx_name="requisition_candidate_RequisitionId_IsHired_IDX1"
db.requisition_candidate.createIndex( 
{ 
  "requisition_id" : 1,
  "is_hired" : 1
}, 
{ 
  "name" : "requisition_candidate_RequisitionId_IsHired_IDX1", 
  "background" : true
}
);

// indx_name = "requisition_candidate_LoadedDate_IDX2"
db.requisition_candidate.createIndex( 
{ 
  "loaded_date" : -1
}, 
{ 
  "name" : "requisition_candidate_LoadedDate_IDX2", 
  "background" : true
}
);

// indx_name = "requisition_candidate_UpdateDate_IDX3"
db.requisition_candidate.createIndex( 
{ 
  "update_date" : -1
}, 
{ 
  "name" : "requisition_candidate_UpdateDate_IDX3", 
  "background" : true
}
);

// indx_name = "requisition_candidate_EtlProcessFlag_IDX4"
db.requisition_candidate.createIndex( 
{ 
  "etl_process_flag" : 1
}, 
{ 
  "name" : "requisition_candidate_EtlProcessFlag_IDX4", 
  "background" : true
}
);

// indx_name = "requisition_candidate_CandidateId_RequisitionSubmissionStatusId_IDX5"
db.requisition_candidate.createIndex( 
{ 
  "candidate_id" : 1,
  "requisition_submission_status_id" : 1
}, 
{ 
  "name" : "requisition_candidate_CandidateId_RequisitionSubmissionStatusId_IDX5", 
  "background" : true
}
);

// ### Collection: vendor ###
// indx_name="vendor_ReqClientId_MasterSupplierId_VendorId_ClientID_IDX1"
db.vendor.createIndex( 
{ 
  "req_client_id" : 1,
  "master_supplier_id" : 1,
  "vendor_id" : 1,
  "client_id" : 1  
}, 
{ 
  "name" : "vendor_ReqClientId_MasterSupplierId_VendorId_ClientID_IDX1", 
  "background" : true
}
);

// ### Collection: requisition_candidate_division ###
// indx_name="requisition_candidate_division_CandidateId_IDX1"
db.requisition_candidate_division.createIndex( 
{ 
  "candidate_id" : 1
}, 
{ 
  "name" : "requisition_candidate_division_CandidateId_IDX1", 
  "background" : true
}
);

// indx_name="requisition_candidate_division_CilentId_IDX2"
db.requisition_candidate_division.createIndex( 
{ 
  "client_id" : 1
}, 
{ 
  "name" : "requisition_candidate_division_CilentId_IDX2", 
  "background" : true
}
);

// indx_name="requisition_candidate_division_CilentId_Active_IDX3"
db.requisition_candidate_division.createIndex( 
{ 
  "client_id" : 1,
  "active" :1
  
}, 
{ 
  "name" : "requisition_candidate_division_CilentId_Active_IDX3", 
  "background" : true
}
);

// ### Collection: client ###
// indx_name="client_ClientId_UK1"
db.client.createIndex( 
{ 
  "client_id" : 1 
}, 
{ 
  "name" : "client_ClientId_UK1", 
  "unique" : true,
  "background" : true
}
);


// ### Collection: configuration_settings ###
// indx_name="configuration_settings_Name_UK1"
db.configuration_settings.createIndex( 
{ 
  "name" : 1 
}, 
{ 
  "name" : "configuration_settings_Name_UK1", 
  "unique" : true,
  "background" : true
}
);