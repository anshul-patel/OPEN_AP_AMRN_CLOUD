create or replace PACKAGE aee_ap_invoice_extract_pkg AS
/*------------------------------------------------------------------------------------
--| Module Name:  AEE_AP_INVOICE_EXTRACT_PKG
--|
--| Description:  Pacakge to extract AP Invoices for cloud conversion
--|
--| Date:         11-Feb-2022
--|
--| Author:       Akshay Kumar
--|
--| Parameters:   p_data_level           --> AP Invoice Headers/AP Invoice Lines
---------------------------------------------------------------|
--|  Modification History
---------------------------------------------------------------|
--|  Date        Who           Description
--| ----------- -------------  --------------------------------|
--| 25-Feb-2022  Akshay Kumar   Initial Creation
--| 24-Aug-2022  Anshul Patel	Mock4 Changes
--| 20-Sep-2022  Anshul Patel	Mock5 Changes
--| 10-Oct-2022  Anshul Patel	API Changes
------------------------------------------------------------------------------------*/
    PROCEDURE main (
        p_errbuf_out         OUT   VARCHAR2,
        p_retcode_out        OUT   NUMBER,
        p_data_level         IN    VARCHAR2,
        p_batch_number       IN    VARCHAR2,        
        p_business_extract   IN    VARCHAR2
    );

    PROCEDURE ap_invoice_headers_extract (
        p_hold IN VARCHAR2
    );

    PROCEDURE ap_invoice_lines_extract (
        p_hold               IN   VARCHAR2,
        p_business_extract   IN   VARCHAR2
    );

    PROCEDURE ap_unique_code_combination_extract;
	
	PROCEDURE ap_paid_invoices_header_extract;

	PROCEDURE ap_paid_invoices_lines_extract;
	
	PROCEDURE unique_code_atp_to_ebs_db;
    
    PROCEDURE ap_holds_all_header_extract;
	
	PROCEDURE ap_holds_all_Lines_extract(p_business_extract  IN   VARCHAR2);
	
	PROCEDURE ap_holds_all_descp;
	
	PROCEDURE ap_inv_int_header_extract;
	
	PROCEDURE ap_inv_int_Lines_extract;
	
	PROCEDURE ap_inv_int_descp;
    
    PROCEDURE custom_table_insert;
	
END aee_ap_invoice_extract_pkg;
/