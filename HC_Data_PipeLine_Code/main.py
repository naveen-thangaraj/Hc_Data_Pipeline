# -*- coding: utf-8 -*-
"""
Created on Wed Jan 10 12:37:32 2024

@author: naveen
"""

from HC_Data_PipeLine_Code.Pre_Processing.Mariadb_Claims import mariadb_pre_processing
from HC_Data_PipeLine_Code.Pre_Processing.Investication_MIS import investication_mis_pre_processing
from HC_Data_PipeLine_Code.Pre_Processing.Claim_MIS import claims_mis_pre_processing


try:

    # Step1 MariaDB Pre_Processing call
    maria_db_pre_process_call=mariadb_pre_processing.mariadb_pre_processing_main()
    
    # Step2 Investication MIS call
    investication_pre_process_call=investication_mis_pre_processing.investication_pre_process_main()
    
    # Step3 Claims MIS call
    claims_mis__pre_process_call=claims_mis_pre_processing.claims_mis_main()

except Exception as e:
    print(e)

