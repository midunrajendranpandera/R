
/* Collections Involved - requisition_skills_from_parsed_requisition, candidate_skills_from_parsed_resumes, ideal_candidate_characteritics,category_candidate_map, searchscore_cand_zindex_scores, requisition_cand_zindex_scores,
candidate_zindex_score, candidate_zindex_score_audit, candidate_zindex_score_summary, candidate_zindex_score_summary_audit, etl_job_log
*/

// ### Collection: requisition_skills_from_parsed_requisition ###
// indx_name="requisition_skills_parsed_RequisitionId_UK1"
db.requisition_skills_from_parsed_requisition.createIndex( 
{ 
  "requisition_id" : 1 
}, 
{ 
  "name" : "requisition_skills_parsed_RequisitionId_UK1", 
  "unique" : true,
  "background" : true
}
);


// ### Collection: candidate_skills_from_parsed_resumes  ###
//  indx_name : "candidate_skills_parsed_resumes_CandidateID_ResumeId_UK1"
db.candidate_skills_from_parsed_resumes.createIndex( 
{ 
  "candidate_id" : 1, 
  "resume_id" : 1
}, 
{ 
  "name" : "candidate_skills_parsed_resumes_CandidateID_ResumeId_UK1", 
  "unique" : true, 
  "background" : true
}
);

//  indx_name : "candidate_skills_parsed_resumes_CandidateID_ResumeId_InterpreterValue_UK2"
db.candidate_skills_from_parsed_resumes.createIndex( 
{ 
  "candidate_id" : 1, 
  "resume_id" : 1, 
  "parsedWords.interpreter_value": 1 
}, 
{ 
  "name" : "candidate_skills_parsed_resumes_CandidateID_ResumeId_InterpreterValue_UK2", 
  "unique" : true, 
  "background" : true
}
);



// ### Collection: ideal_candidate_characteritics ###
// indx_name="ideal_candidate_characteritics_GlobalJobCategoryId_UK1"
db.ideal_candidate_characteritics.createIndex( 
{ 
  "global_job_category_id" : 1
}, 
{ 
  "name" : "ideal_candidate_characteritics_GlobalJobCategoryId_UK1", 
  "unique" : true,
  "background" : true
}
);

// ### Collection: category_candidate_map ###
//  indx_name : "category_candidate_map_JobCategoryId_UK1"
db.category_candidate_map.createIndex( 
{ 
  "global_job_category_id" : 1 
}, 
{ 
  "name" : "category_candidate_map_JobCategoryId_UK1", 
  "unique" : true,
  "background" : true
}
);

//  indx_name : "category_candidate_map_Candidates_IDX1"
db.category_candidate_map.createIndex( 
{ 
  "candidates" : 1 
}, 
{ 
  "name" : "category_candidate_map_Candidates_IDX1", 
  "background" : true
}
);



// ### Collection: searchscore_cand_zindex_scores ###
// indx_name="searchscore_cand_zindex_scores_RequisitionId_CandidateId_UK1"
db.searchscore_cand_zindex_scores.createIndex( 
{ 
  "requisition_id" : 1,
  "candidate_id" : 1
}, 
{ 
  "name" : "searchscore_cand_zindex_scores_RequisitionId_CandidateId_UK1", 
  "unique" : true,
  "background" : true
}
);


// ### Collection: requisition_cand_zindex_scores ###
// indx_name="requisition_cand_zindex_scores_RequisitionId_CandidateId_UK1"
db.requisition_cand_zindex_scores.createIndex( 
{ 
  "requisition_id" : 1,
  "candidate_id" : 1
}, 
{ 
  "name" : "requisition_cand_zindex_scores_RequisitionId_CandidateId_UK1", 
  "unique" : true,
  "background" : true
}
);



// ### Collection: candidate_zindex_score ###
//  indx_name : "candidate_zindex_score_RequisitionId_IDX1"
db.candidate_zindex_score.createIndex( 
{ 
  "requisition_id" : 1 
}, 
{ 
  "name" : "candidate_zindex_score_RequisitionId_IDX1", 
  "background" : true
}
);


// ### Collection: candidate_zindex_score_audit ###
//  indx_name : "candidate_zindex_score_audit_RequisitionId_IDX1"
db.candidate_zindex_score_audit.createIndex( 
{ 
  "requisition_id" : 1 
}, 
{ 
  "name" : "candidate_zindex_score_audit_RequisitionId_IDX1", 
  "background" : true
}
);

// ### Collection: candidate_zindex_score_summary ###
//  indx_name : "candidate_zindex_score_summary_RequisitionId_UK1"
db.candidate_zindex_score_summary.createIndex( 
{ 
  "requisition_id" : 1 
}, 
{ 
  "name" : "candidate_zindex_score_summary_RequisitionId_UK1", 
  "unique" : true,
  "background" : true
}
);


// ### Collection: candidate_zindex_score_summary_audit ###
//  indx_name : "candidate_zindex_score_summary_audit_RequisitionId_IDX1"
db.candidate_zindex_score_summary_audit.createIndex( 
{ 
  "requisition_id" : 1 
}, 
{ 
  "name" : "candidate_zindex_score_summary_audit_RequisitionId_IDX1", 
  "background" : true
}
);

// ### Collection: etl_job_log ###
// indx_name="etl_job_log_JobName_StartDatetime_EndDatetime_IDX1"
db.etl_job_log.createIndex( 
{ 
  "job_name" : 1,
  "start_datetime" : -1,
  "end_date_time" : -1
}, 
{ 
  "name" : "etl_job_log_JobName_StartDatetime_EndDatetime_IDX1", 
  "background" : true
}
);

