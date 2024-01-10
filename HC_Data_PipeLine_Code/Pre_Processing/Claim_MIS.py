import os
import pandas as pd
from datetime import datetime

class claims_mis_pre_processing:
    
    
    @staticmethod
    def remove_files_in_folder(folder_path):
        files = os.listdir(folder_path)
        for file in files: 
            file_path = os.path.join(folder_path, file) 
            if os.path.isfile(file_path): 
                os.remove(file_path)
                print(f"Removed file: {file_path}")
                
    @staticmethod
    def claims_mis_main():
        
        current_date=datetime.now()
    
        final_claim_data = pd.read_excel('./Pre_Processing/Inputs/Consolidated_Claims_Dump_20-12-2023.xlsx')
        
        final_claim_data.columns = final_claim_data.columns.str.strip()
        
        
        final_claim_data = final_claim_data.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        # Load the data from the Step1 and step2 mergerd CSV file
        
        S3_input_bucket=r"./Pre_Processing/Claims_Output/Investication_MIS/"+ str(current_date.strftime("%Y-%m-%d"))
    
        
        step_1_2_inv_data = pd.read_csv(S3_input_bucket + '\step2_final_output.csv',encoding='latin1')
        
        convert_dict = {'claim_number': str}
        
        step_1_2_inv_data = step_1_2_inv_data.astype(convert_dict)
        
        step_1_2_inv_data.columns = step_1_2_inv_data.columns.str.strip()
        
        step_1_2_inv_data = step_1_2_inv_data.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        claim_final_merge_output = pd.merge(step_1_2_inv_data,
                                      final_claim_data[['CLAIM_NUMBER', 'Final_Status','CLAIM_REJECTION_REMARKS_GENCON']], 
                                      left_on = ['claim_number'], right_on = 'CLAIM_NUMBER', how='left')
        
        #Final status will be in the following buckets
        paid_df = claim_final_merge_output[claim_final_merge_output['Final_Status'].str.startswith(('Paid'))]
        
        approved_not_paid_df = claim_final_merge_output[claim_final_merge_output['Final_Status'].str.startswith(('Approved not Paid'))]
        
        query_df = claim_final_merge_output[claim_final_merge_output['Final_Status'].str.startswith(('Query'))]
        
        query_investigation_df = claim_final_merge_output[claim_final_merge_output['Final_Status'].str.startswith(('Query + Investigation'))]
        
        repudiated_df = claim_final_merge_output[claim_final_merge_output['Final_Status'].str.startswith(('Repudiated'))]
        
        investigation_df = claim_final_merge_output[claim_final_merge_output['Final_Status'].str.startswith(('Investigation'))]
        
        under_process_df = claim_final_merge_output[claim_final_merge_output['Final_Status'].str.startswith(('Under Process'))]
        
        # Merge All Buckets into single dataframe
        
        step_3_concat = pd.concat([paid_df, approved_not_paid_df, query_df, query_investigation_df, repudiated_df, investigation_df, under_process_df])
        
        lo_category_df = pd.read_csv(r'./Pre_Processing/Inputs/LO_Category.csv')
            
        lo_category_df.columns = lo_category_df.columns.str.strip()
        
        final_lo_category_df = pd.merge(step_3_concat,
                                      lo_category_df[['final_claim_rejection_remarks_gencon','Lo_Catgeory']], 
                                      left_on = ['CLAIM_REJECTION_REMARKS_GENCON'], right_on = 'final_claim_rejection_remarks_gencon', how='left')
        
        
        final_lo_category_df = final_lo_category_df.loc[:, ~final_lo_category_df.columns.str.contains('CLAIM_REJECTION_REMARKS_GENCON')]
        
        
        target_definition_data=[['Non-Disclosure / Pre Existing Disease','Quasi/Potential Fraud'],
                           ['Hospitalization Abuse','Quasi/Potential Fraud'],
                           ['Fraud','Fraud'],
                           ['General Exclusion','Operational Repudiation/Policy Rejects'],
                           ['Product Communication Issue','Operational Repudiation/Policy Rejects'],
                           ['Unresolved Query','Operational Repudiation/Policy Rejects'],
                           ['Application Issue','Operational Repudiation/Policy Rejects'],
                           ['nan','Ignored'],
                           ['Other','Ignored']]
        
        
        target_definition_df = pd.DataFrame(target_definition_data, columns=['repudiation_reason', 'rejection_mapping'])
        
        target_definition_df.columns = target_definition_df.columns.str.strip()
        
        final_target_def_lo_cate_df = pd.merge(final_lo_category_df,
                                      target_definition_df[['repudiation_reason','rejection_mapping']], 
                                      left_on = ['Lo_Catgeory'], right_on = 'repudiation_reason', how='left')
        
        final_target_def_lo_cate_df = final_target_def_lo_cate_df.loc[:, ~final_target_def_lo_cate_df.columns.str.contains('repudiation_reason')]
        
        
        # s3 bucket path
        
        s3_bucket_path=r"./Pre_Processing/Claims_Output/claims_mis/" + str(current_date.strftime("%Y-%m-%d"))
            
        if not os.path.exists(s3_bucket_path):
            os.makedirs(s3_bucket_path)
        else:
           ext_remove_files_in_folder=claims_mis_pre_processing.remove_files_in_folder(s3_bucket_path)
        
        
        final_target_def_lo_cate_df.to_csv(s3_bucket_path + '/step3_final_consolidated_output.csv',index=False)
        
