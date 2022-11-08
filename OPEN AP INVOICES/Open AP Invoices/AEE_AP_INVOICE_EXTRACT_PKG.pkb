create or replace PACKAGE BODY aee_ap_invoice_extract_pkg AS
/*------------------------------------------------------------------------------------
--| Module Name:  AEE_AP_INVOICE_EXTRACT_PKG
--|
--| Description:  Package to extract AP Invoices for cloud conversion
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
--| 23-Sep-2022  Anshul Patel	Mock5 PO Changes
--| 05-Oct-2022  Anshul Patel	AP_HOLDS changes
------------------------------------------------------------------------------------*/
TYPE gt_xxap_inv_type IS
      TABLE OF xxap.aee_fin_ap_inv_details_tbl%rowtype INDEX BY BINARY_INTEGER;
     

   -- Global Records
   gt_xxap_inv_tab         gt_xxap_inv_type;

    gv_retcode_out      NUMBER := 0;
    gv_warnretcode_out  NUMBER := 0;
    gv_errorretcode_out NUMBER := 0;
    gv_errbuf_out       VARCHAR2(4000) := NULL;
    gv_temp_errbuffout  VARCHAR2(4000) := NULL;
    l_ext_file_loc      VARCHAR2(100);
    l_ext_file_name     VARCHAR2(100) := NULL;
    gv_process_sts      VARCHAR2(1) := 'Y';
    gv_error_msg        VARCHAR2(2000) := NULL;
    gv_count            NUMBER := 0;
    gv_batch_no         VARCHAR2(20) := to_char(sysdate, 'RRRRMMDDHHMISS');
    gv_batch            VARCHAR2(30);

    PROCEDURE main (
        p_errbuf_out       OUT VARCHAR2,
        p_retcode_out      OUT NUMBER,
        p_data_level       IN VARCHAR2,
		p_batch_number     IN VARCHAR2,       
        p_business_extract IN VARCHAR2
    ) IS
    
  
    
    BEGIN
      IF p_batch_number is not null then 
    gv_batch := p_batch_number;
    END IF;
        p_errbuf_out := 'Concurrent Program Completed Succesfully';
        p_retcode_out := 0;
        fnd_file.put_line(fnd_file.output, '------------Printing Parameters------------');
        fnd_file.put_line(fnd_file.output, 'p_data_level --> ' || p_data_level);
        BEGIN
            SELECT
                meaning
            INTO l_ext_file_loc
            FROM
                apps.fnd_lookup_values_vl
            WHERE
                    lookup_type = 'AEE_CONVERSION_DIRECTORIES_LKP'
                AND lookup_code = 'AP-CNV-001-INV';

        EXCEPTION
            WHEN OTHERS THEN
                l_ext_file_loc := NULL;
        END;

        IF p_data_level = 'EBS TO ATP AP CODE' THEN
            ap_unique_code_combination_extract;
        ELSIF p_data_level = 'ATP TO EBS AP CODE' THEN
            unique_code_atp_to_ebs_db;
        ELSIF p_data_level = 'Unpaid Open AP Invoices' THEN
            ap_invoice_headers_extract('Y');        
            ap_invoice_lines_extract('Y', p_business_extract);
			ap_invoice_headers_extract('N');        
            ap_invoice_lines_extract('N', p_business_extract);
        ELSIF p_data_level = 'Paid Invoices' THEN
            ap_paid_invoices_header_extract;        
            ap_paid_invoices_lines_extract;
		ELSIF p_data_level = 'ON-Hold Invoices' THEN
            ap_holds_all_header_extract;		
            ap_holds_all_Lines_extract(p_business_extract);
			ap_holds_all_descp;
        ELSIF p_data_level = 'API Interface Invoices' THEN
            ap_inv_int_header_extract;			
            ap_inv_int_Lines_extract;
			ap_inv_int_descp;
			
            IF gv_retcode_out = 1 OR gv_retcode_out = 2 THEN
                IF gv_temp_errbuffout IS NOT NULL THEN
                    gv_temp_errbuffout := gv_temp_errbuffout
                                          || ', '
                                          || gv_errbuf_out;
                ELSIF gv_temp_errbuffout IS NULL THEN
                    gv_temp_errbuffout := gv_errbuf_out;
                END IF;

            END IF;

            IF gv_retcode_out = 1 THEN
                gv_warnretcode_out := gv_retcode_out;
            ELSIF gv_retcode_out = 2 THEN
                gv_errorretcode_out := gv_retcode_out;
            END IF;

            gv_errbuf_out := gv_temp_errbuffout;
            IF gv_errorretcode_out = 2 THEN
                gv_retcode_out := 2;
            ELSIF gv_warnretcode_out = 1 THEN
                gv_retcode_out := 1;
            ELSE
                gv_retcode_out := 0;
            END IF;

        END IF;

        p_retcode_out := gv_retcode_out;
        p_errbuf_out := gv_errbuf_out;
    EXCEPTION
        WHEN OTHERS THEN
            p_retcode_out := 2;
            p_errbuf_out := sqlerrm;
    END main;   

----------------------------------------------------------------------------------------------------------------------------------------------
-- Extracting Invoice Lines --
----------------------------------------------------------------------------------------------------------------------------------------------

    PROCEDURE ap_paid_invoices_lines_extract IS

        CURSOR cur_invoice_line_data IS
        SELECT
    aia.invoice_id                                                  invoice_id,
    '1'                                                             line_number,
    'ITEM'                                                          line_type_lookup_code,
    '0'                                                             amount,
    NULL                                                            quantity_invoiced,
    NULL                                                            unit_price,
    NULL                                                            unit_of_meas_lookup_code,
    NULL                                                            description,
    NULL                                                            po_number,
    NULL                                                            po_line_number,
    NULL                                                            po_shipment_num,
    NULL                                                            po_distribution_num,
    NULL                                                            item_description,
    NULL                                                            release_num,
    NULL                                                            purchasing_category,
    NULL                                                            receipt_number,
    NULL                                                            receipt_line_number,
    NULL                                                            consumption_advice_number,
    NULL                                                            consumption_advice_line_number,
    NULL                                                            packing_slip,
    NULL                                                            final_match_flag,
    decode('ADC', 'ADC.NU.184061.0000.170.00000.000.00.0000', 'AIC', 'AIC.CU.184061.0000.ZA0.00000.000.00.0000',
           'AMC', 'AMC.CU.184061.0000.100.00000.000.00.0000', 'AMS', 'AMS.CU.184061.0000.110.00000.000.00.0000', 'ATX',
           'ATX.T1.184061.0000.TX0.00000.000.00.0000', 'ITC', 'ITC.T1.184061.0000.TN0.00000.000.00.0000', 'UEC', 'UEC.CU.184061.0000.200.00000.000.00.0000',
           'MV1', 'MV1.CE.184061.0000.V10.00000.000.00.0000', NULL) dist_code_concatenated, -- to be confirmed with Piyush
    NULL                                                            distribution_set_name,
    to_char(last_day(sysdate), 'RRRR/MM/DD')                        accounting_date, --to be chnged every mock
    NULL                                                            account_segment,
    NULL                                                            balancing_segment,
    NULL                                                            cost_center_segment,
    NULL                                                            tax_classification_code,
    NULL                                                            ship_to_location_code,
    NULL                                                            ship_from_location_code,
    NULL                                                            final_discharge_location_code,
    NULL                                                            trx_business_category,
    NULL                                                            product_fisc_classification,
    NULL                                                            primary_intended_use,
    NULL                                                            user_defined_fisc_class,
    NULL                                                            product_type,
    NULL                                                            assessable_value,
    NULL                                                            product_category,
    NULL                                                            control_amount,
    NULL                                                            tax_regime_code,
    NULL                                                            tax,
    NULL                                                            tax_status_code,
    NULL                                                            tax_jurisdiction_code,
    NULL                                                            tax_rate_code,
    NULL                                                            tax_rate,
    NULL                                                            awt_group_name,
    NULL                                                            type_1099,
    NULL                                                            income_tax_region,
    NULL                                                            prorate_across_flag,
    NULL                                                            line_group_number,
    NULL                                                            cost_factor_name,
    NULL                                                            stat_amount,
    NULL                                                            assets_tracking_flag,
    NULL                                                            asset_book_type_code,
    NULL                                                            asset_category_id,
    NULL                                                            serial_number,
    NULL                                                            manufacturer,
    NULL                                                            model_number,
    NULL                                                            warranty_number,
    NULL                                                            price_correction_flag,
    NULL                                                            price_correct_inv_num,
    NULL                                                            price_correct_inv_line_num,
    NULL                                                            requester_first_name,
    NULL                                                            requester_last_name,
    NULL                                                            requester_employee_num,
    NULL                                                            attribute_category,
    NULL                                                            attribute1,
    NULL                                                            attribute2,
    NULL                                                            attribute3,
    NULL                                                            attribute4,
    NULL                                                            attribute5,
    NULL                                                            attribute6,
    NULL                                                            attribute7,
    NULL                                                            attribute8,
    NULL                                                            attribute9,
    NULL                                                            attribute10,
    NULL                                                            attribute11,
    NULL                                                            attribute12,
    NULL                                                            attribute13,
    NULL                                                            attribute14,
    NULL                                                            attribute15,
    NULL                                                            attribute_number1,
    NULL                                                            attribute_number2,
    NULL                                                            attribute_number3,
    NULL                                                            attribute_number4,
    NULL                                                            attribute_number5,
    NULL                                                            attribute_date1,
    NULL                                                            attribute_date2,
    NULL                                                            attribute_date3,
    NULL                                                            attribute_date4,
    NULL                                                            attribute_date5,
    NULL                                                            global_attribute_catgory,
    NULL                                                            global_attribute1,
    NULL                                                            global_attribute2,
    NULL                                                            global_attribute3,
    NULL                                                            global_attribute4,
    NULL                                                            global_attribute5,
    NULL                                                            global_attribute6,
    NULL                                                            global_attribute7,
    NULL                                                            global_attribute8,
    NULL                                                            global_attribute9,
    NULL                                                            global_attribute10,
    NULL                                                            global_attribute11,
    NULL                                                            global_attribute12,
    NULL                                                            global_attribute13,
    NULL                                                            global_attribute14,
    NULL                                                            global_attribute15,
    NULL                                                            global_attribute16,
    NULL                                                            global_attribute17,
    NULL                                                            global_attribute18,
    NULL                                                            global_attribute19,
    NULL                                                            global_attribute20,
    NULL                                                            global_attribute_number1,
    NULL                                                            global_attribute_number2,
    NULL                                                            global_attribute_number3,
    NULL                                                            global_attribute_number4,
    NULL                                                            global_attribute_number5,
    NULL                                                            global_attribute_date1,
    NULL                                                            global_attribute_date2,
    NULL                                                            global_attribute_date3,
    NULL                                                            global_attribute_date4,
    NULL                                                            global_attribute_date5,
    NULL                                                            pjc_project_id,
    NULL                                                            pjc_task_id,
    NULL                                                            pjc_expenditure_type_id,
    NULL                                                            pjc_expenditure_item_date,
    NULL                                                            pjc_organization_id,
    NULL                                                            pjc_project_number,
    NULL                                                            pjc_task_number,
    NULL                                                            pjc_expenditure_type_name,
    NULL                                                            pjc_organization_name,
    NULL                                                            pjc_reserved_attribute1,
    NULL                                                            pjc_reserved_attribute2,
    NULL                                                            pjc_reserved_attribute3,
    NULL                                                            pjc_reserved_attribute4,
    NULL                                                            pjc_reserved_attribute5,
    NULL                                                            pjc_reserved_attribute6,
    NULL                                                            pjc_reserved_attribute7,
    NULL                                                            pjc_reserved_attribute8,
    NULL                                                            pjc_reserved_attribute9,
    NULL                                                            pjc_reserved_attribute10,
    NULL                                                            pjc_user_def_attribute1,
    NULL                                                            pjc_user_def_attribute2,
    NULL                                                            pjc_user_def_attribute3,
    NULL                                                            pjc_user_def_attribute4,
    NULL                                                            pjc_user_def_attribute5,
    NULL                                                            pjc_user_def_attribute6,
    NULL                                                            pjc_user_def_attribute7,
    NULL                                                            pjc_user_def_attribute8,
    NULL                                                            pjc_user_def_attribute9,
    NULL                                                            pjc_user_def_attribute10,
    NULL                                                            fiscal_charge_type,
    NULL                                                            def_acctg_start_date,
    NULL                                                            def_acctg_end_date,
    NULL                                                            def_accural_code_concatenated,
    NULL                                                            pjc_project_name,
    NULL                                                            jc_task_name
  FROM
    apps.ap_invoices_all         aia,
    apps.ap_checks_all           aca,
    apps.ap_invoice_payments_all apsa,
    apps.ap_suppliers            aps,
    apps.ap_supplier_sites_all   apss
 WHERE
        1 = 1
       AND aia.vendor_id           = aps.vendor_id
       AND aia.payment_status_flag = 'Y'
       AND aia.cancelled_amount IS NULL
       AND aia.invoice_type_lookup_code <> 'PAYMENT REQUEST'
       AND aia.vendor_site_id      = apss.vendor_site_id
       AND aps.vendor_id           = apss.vendor_id
       AND aia.invoice_id          = apsa.invoice_id
       AND apsa.check_id           = aca.check_id
       AND aca.check_id            = apsa.check_id
       AND aia.invoice_date >= TO_DATE('2022/01/01', 'RRRR/MM/DD')
       AND aca.status_lookup_code != 'VOIDED'
       AND aia.invoice_amount != 0
       AND aia.invoice_type_lookup_code <> 'PAYMENT REQUEST'
       AND nvl(aps.vendor_type_lookup_code, 'XXX') NOT IN ( 'EMPLOYEE' )
       AND aia.invoice_amount      = aia.amount_paid;

        lc_header    VARCHAR2(32000) := NULL;
        lc_data_type VARCHAR2(32000) := NULL;

	   --Changes for generating CSV file.
        v_utlfile_issue EXCEPTION;
        v_dir_path_fail EXCEPTION;
        v_no_data_to_process EXCEPTION;
        lv_error_msg VARCHAR2(2000) := NULL;
        f_handle     utl_file.file_type;
        p_f_handle   utl_file.file_type;
    BEGIN

	    -- Changes for generating CSV file. --
        l_ext_file_name := 'ApInvoicesInterface_'
                           || 'PAID_LINES'
                           || gv_batch_no
                           || '.csv';
        aee_common_util_pkg.validate_ext_dir(l_ext_file_loc, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_dir_path_fail;
        END IF;

        aee_common_util_pkg.openfile(l_ext_file_name, l_ext_file_loc, gv_process_sts, lv_error_msg, p_f_handle);
        fnd_file.put_line(fnd_file.log, 'Flag gv_process_sts value - ' || gv_process_sts);
        f_handle := p_f_handle;
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        lc_header := chr(34)
                     || 'INVOICE_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LINE_TYPE_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'QUANTITY_INVOICED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'UNIT_PRICE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'UNIT_OF_MEAS_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DESCRIPTION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_SHIPMENT_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_DISTRIBUTION_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ITEM_DESCRIPTION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'RELEASE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PURCHASING_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'RECEIPT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'RECEIPT_LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONSUMPTION_ADVICE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONSUMPTION_ADVICE_LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PACKING_SLIP'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FINAL_MATCH_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DIST_CODE_CONCATENATED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DISTRIBUTION_SET_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ACCOUNTING_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ACCOUNT_SEGMENT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'BALANCING_SEGMENT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'COST_CENTER_SEGMENT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_CLASSIFICATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SHIP_TO_LOCATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SHIP_FROM_LOCATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FINAL_DISCHARGE_LOCATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TRX_BUSINESS_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRODUCT_FISC_CLASSIFICATION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRIMARY_INTENDED_USE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'USER_DEFINED_FISC_CLASS'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRODUCT_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSESSABLE_VALUE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRODUCT_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONTROL_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_REGIME_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_STATUS_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_JURISDICTION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_RATE_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_RATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AWT_GROUP_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TYPE_1099'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INCOME_TAX_REGION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRORATE_ACROSS_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LINE_GROUP_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'COST_FACTOR_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'STAT_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSETS_TRACKING_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSET_BOOK_TYPE_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSET_CATEGORY_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SERIAL_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'MANUFACTURER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'MODEL_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'WARRANTY_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRICE_CORRECTION_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRICE_CORRECT_INV_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRICE_CORRECT_INV_LINE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_FIRST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_LAST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_EMPLOYEE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_CATGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE16'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE17'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE18'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE19'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE20'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_PROJECT_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_TASK_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_EXPENDITURE_TYPE_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_EXPENDITURE_ITEM_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_ORGANIZATION_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_PROJECT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_TASK_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_EXPENDITURE_TYPE_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_ORGANIZATION_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FISCAL_CHARGE_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DEF_ACCTG_START_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DEF_ACCTG_END_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DEF_ACCURAL_CODE_CONCATENATED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_PROJECT_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'JC_TASK_NAME'
                     || chr(34);

        lc_data_type := 'VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2';

		-- Changes for generating CSV file
        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_data_type, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        FOR cur_rec IN cur_invoice_line_data LOOP
            lc_header := chr(34)
                         || cur_rec.invoice_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.line_type_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.quantity_invoiced
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.unit_price
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.unit_of_meas_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.description
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_shipment_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_distribution_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.item_description
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.release_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.purchasing_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.receipt_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.receipt_line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.consumption_advice_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.consumption_advice_line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.packing_slip
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.final_match_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.dist_code_concatenated
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.distribution_set_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.accounting_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.account_segment
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.balancing_segment
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cost_center_segment
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_classification_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.ship_to_location_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.ship_from_location_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.final_discharge_location_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.trx_business_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.product_fisc_classification
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.primary_intended_use
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.user_defined_fisc_class
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.product_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.assessable_value
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.product_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.control_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_regime_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_status_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_jurisdiction_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_rate_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_rate
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.awt_group_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.type_1099
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.income_tax_region
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prorate_across_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.line_group_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cost_factor_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.stat_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.assets_tracking_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.asset_book_type_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.asset_category_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.serial_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.manufacturer
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.model_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.warranty_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.price_correction_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.price_correct_inv_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.price_correct_inv_line_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_first_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_last_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_employee_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_catgory
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute16
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute17
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute18
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute19
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute20
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_project_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_task_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_expenditure_type_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_expenditure_item_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_organization_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_project_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_task_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_expenditure_type_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_organization_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.fiscal_charge_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.def_acctg_start_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.def_acctg_end_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.def_accural_code_concatenated
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_project_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.jc_task_name
                         || chr(34);

	 --Changes for generating CSV file

            aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
            IF gv_process_sts = 'N' THEN
                gv_error_msg := gv_error_msg || lv_error_msg;
                RAISE v_utlfile_issue;
            END IF;
--gv_count:=gv_count+1;

        END LOOP;
--dbms_output.put_line('Records in output file: '||gv_count);

        utl_file.fclose(f_handle); -- Change
        gv_retcode_out := 0;
        gv_errbuf_out := NULL;
    EXCEPTION
        WHEN v_dir_path_fail THEN
            fnd_file.put_line(fnd_file.log, 'v_dir_path_fail Exception of proc ap_paid_invoices_lines_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_dir_path_fail';
        WHEN v_utlfile_issue THEN
            fnd_file.put_line(fnd_file.log, 'v_utlfile_issue Exception of proc ap_paid_invoices_lines_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_utlfile_issue';
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while fetching the data for paid lines with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END ap_paid_invoices_lines_extract;



----------------------------------------------------------------------------------------------------------------------------------------------
-- Extracting Invoice Lines --
----------------------------------------------------------------------------------------------------------------------------------------------

    PROCEDURE ap_inv_int_lines_extract IS

        CURSOR cur_invoice_line_data IS
       select 
*
from (SELECT DISTINCT
    aila.invoice_id                                                                                          invoice_id,
    to_char(aila.line_number)                                                                                line_number,
    aila.line_type_lookup_code                                                                               line_type_lookup_code,
    aila.amount                                                                                              amount,
    aila.quantity_invoiced                                                                                   quantity_invoiced,
    aila.unit_price                                                                                          unit_price,
    NULL                                                                                                     unit_of_meas_lookup_code,
    substr(replace(replace(replace(replace(aila.description, '""', '"'), '"', '""'), CHR(10), NULL), CHR(13), NULL),1,240) description,
    aila.po_header_id,
    (
        CASE
                WHEN (
                    SELECT
                        release_num
                    FROM
                        po_releases_all pra
                    WHERE
                        pra.po_release_id = aila.po_release_id
                ) IS NULL THEN
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                ELSE
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                    || '-'
                    || (
                        SELECT
                            release_num
                        FROM
                            po_releases_all pra
                        WHERE
                            pra.po_release_id = aila.po_release_id
                    )
            END
    )                                                                                                        po_number,
    (
        SELECT
            pla.line_num
          FROM
            po_lines_all pla
         WHERE
            pla.po_line_id = aila.po_line_id
    )                                                                                                        po_line_number,
    (
        SELECT
            plla.shipment_num
          FROM
            po_line_locations_all plla
         WHERE
            plla.line_location_id = aila.po_line_location_id
    )                                                                                                        po_shipment_num,
    (
        SELECT
            pda.distribution_num
          FROM
            po_distributions_all pda
         WHERE
            pda.po_distribution_id = aila.po_distribution_id
    )                                                                                                        po_distribution_num,
      
    NULL                                                                                                     item_description,
    NULL                                                                                                     release_num,
    NULL                                                                                                     purchasing_category,
    NULL                                                                                                     receipt_number,
    NULL                                                                                                     receipt_line_number,
    NULL                                                                                                     consumption_advice_number,
    NULL                                                                                                     consumption_advice_line_number,
    NULL                                                                                                     packing_slip,
    NULL                                                                                                     final_match_flag,
   (
        SELECT
           aga.oracle_company
      || '.'
      || aga.oracle_product
      || '.'
      || aga.oracle_account
      || '.'
      || aga.oracle_cost_center
      || '.'
      || aga.oracle_location
      || '.'
      || aga.oracle_compliance_code
      || '.'
      || aga.oracle_intercompany
      || '.'
      || aga.oracle_resource_type
      || '.'
      || aga.oracle_future
          FROM
            aee_gl_aavm_batch_txn_tbl    aga,           
            gl_code_combinations         glcc
         WHERE
                1 = 1              
               AND aila.dist_code_combination_id = glcc.code_combination_id
               AND glcc.segment1                 = aga.corp
               AND glcc.segment2                 = aga.util
               AND glcc.segment3                 = aga.business_division
               AND glcc.segment4                 = aga.major_minor
               AND glcc.segment5                 = aga.fmc
               AND glcc.segment6                 = aga.rmc
               AND glcc.segment7                 = aga.tran_type
               AND glcc.segment8                 = aga.project
               AND glcc.segment9                 = aga.product
               AND glcc.segment10                = aga.activity
               AND glcc.segment11                = aga.resource_type
               AND aga.batch_no = gv_batch
    )                                                                                                                                dist_code_concatenated,
    NULL                                                                                                     distribution_set_name,
    to_char(to_date(/*'30-APR-2023'*/'30-SEP-2022'), 'YYYY/MM/DD')                                                            accounting_date,  -- to be checked with piyush
    NULL                                                                                                     account_segment,
    NULL                                                                                                     balancing_segment,
    NULL                                                                                                     cost_center_segment,
    aila.tax_classification_code                                                                             tax_classification_code,
    (
        SELECT
            hla.location_code
          FROM
            hr_locations_all hla
         WHERE
            aila.ship_to_location_id = hla.location_id
    )                                                                                                        ship_to_location_code,
    NULL                                                                                                     ship_from_location_code,
    NULL                                                                                                     final_discharge_location_code,
    NULL                                                                                                     trx_business_category,
    NULL                                                                                                     product_fisc_classification,
    NULL                                                                                                     primary_intended_use,
    NULL                                                                                                     user_defined_fisc_class,
    NULL                                                                                                     product_type,
    NULL                                                                                                     assessable_value,
    NULL                                                                                                     product_category,
    aia.control_amount                                                                                       control_amount,
    aila.tax_regime_code                                                                                     tax_regime_code,
    aila.tax                                                                                                 tax,
    aila.tax_status_code                                                                                     tax_status_code,
    aila.tax_jurisdiction_code                                                                               tax_jurisdiction_code,
    aila.tax_rate_code                                                                                       tax_rate_code,
    aila.tax_rate                                                                                            tax_rate,
    NULL                                                                                                     awt_group_name,
    Case 
			When aila.type_1099 ='1099S' 
			THEN NULL 
			ELSE aila.type_1099 
			END                                                                                                      type_1099,
    aila.income_tax_region                                                                                   income_tax_region,
    NULL                                                                                                     prorate_across_flag,
    NULL                                                                                                     line_group_number,
    NULL                                                                                                     cost_factor_name,
    NULL                                                                                                     stat_amount,
    NULL                                                                                                     assets_tracking_flag,
    NULL                                                                                                     asset_book_type_code,
    NULL                                                                                                     asset_category_id,
    NULL                                                                                                     serial_number,
    NULL                                                                                                     manufacturer,
    NULL                                                                                                     model_number,
    NULL                                                                                                     warranty_number,
    NULL                                                                                                     price_correction_flag,
    NULL                                                                                                     price_correct_inv_num,
    NULL                                                                                                     price_correct_inv_line_num,
    NULL                                                                                                     requester_first_name,
    NULL                                                                                                     requester_last_name,
    NULL                                                                                                     requester_employee_num,
    'CONVERSION'                                                                                             attribute_category,
    NULL                                                                                                     attribute1,
    NULL                                                                                                     attribute2,
    aila.dist_Code_concatenated                                                                              attribute3,
    NULL                                                                                                     attribute4,
    NULL                                                                                                     attribute5,
    NULL                                                                                                     attribute6,
    NULL                                                                                                     attribute7,
    NULL                                                                                                     attribute8,
    NULL                                                                                                     attribute9,
    NULL                                                                                                     attribute10,
    NULL                                                                                                     attribute11,
    NULL                                                                                                     attribute12,
    NULL                                                                                                     attribute13,
    NULL                                                                                                     attribute14,
    NULL                                                                                                     attribute15,
    NULL                                                                                                     attribute_number1,
    NULL                                                                                                     attribute_number2,
    NULL                                                                                                     attribute_number3,
    NULL                                                                                                     attribute_number4,
    NULL                                                                                                     attribute_number5,
    NULL                                                                                                     attribute_date1,
    NULL                                                                                                     attribute_date2,
    NULL                                                                                                     attribute_date3,
    NULL                                                                                                     attribute_date4,
    NULL                                                                                                     attribute_date5,
    NULL                                                                                                     global_attribute_catgory,
    NULL                                                                                                     global_attribute1,
    NULL                                                                                                     global_attribute2,
    NULL                                                                                                     global_attribute3,
    NULL                                                                                                     global_attribute4,
    NULL                                                                                                     global_attribute5,
    NULL                                                                                                     global_attribute6,
    NULL                                                                                                     global_attribute7,
    NULL                                                                                                     global_attribute8,
    NULL                                                                                                     global_attribute9,
    NULL                                                                                                     global_attribute10,
    NULL                                                                                                     global_attribute11,
    NULL                                                                                                     global_attribute12,
    NULL                                                                                                     global_attribute13,
    NULL                                                                                                     global_attribute14,
    NULL                                                                                                     global_attribute15,
    NULL                                                                                                     global_attribute16,
    NULL                                                                                                     global_attribute17,
    NULL                                                                                                     global_attribute18,
    NULL                                                                                                     global_attribute19,
    NULL                                                                                                     global_attribute20,
    NULL                                                                                                     global_attribute_number1,
    NULL                                                                                                     global_attribute_number2,
    NULL                                                                                                     global_attribute_number3,
    NULL                                                                                                     global_attribute_number4,
    NULL                                                                                                     global_attribute_number5,
    NULL                                                                                                     global_attribute_date1,
    NULL                                                                                                     global_attribute_date2,
    NULL                                                                                                     global_attribute_date3,
    NULL                                                                                                     global_attribute_date4,
    NULL                                                                                                     global_attribute_date5,
    NULL                                                                                                     pjc_project_id,
    NULL                                                                                                     pjc_task_id,
    NULL                                                                                                     pjc_expenditure_type_id,
    NULL                                                                                                     pjc_expenditure_item_date,
    NULL                                                                                                     pjc_organization_id,
   (
        SELECT
		CASE when (aga.project = '*****' or aga.project  is null) then null else 
             aga.proj_out end
          FROM
            aee_gl_aavm_batch_txn_tbl    aga,
            gl_code_combinations         glcc
         WHERE
                1 = 1
               AND aila.dist_code_combination_id = glcc.code_combination_id
               AND glcc.segment1                 = aga.corp
               AND glcc.segment2                 = aga.util
               AND glcc.segment3                 = aga.business_division
               AND glcc.segment4                 = aga.major_minor
               AND glcc.segment5                 = aga.fmc
               AND glcc.segment6                 = aga.rmc
               AND glcc.segment7                 = aga.tran_type
               AND glcc.segment8                 = aga.project
               AND glcc.segment9                 = aga.product
               AND glcc.segment10                = aga.activity
               AND glcc.segment11                = aga.resource_type
               AND aga.batch_no = gv_batch
    )                                                                                                        pjc_project_number,
   (
        SELECT
		CASE when (aga.project = '*****' or aga.project  is null) then null else
             aga.task_out end
          FROM
            aee_gl_aavm_batch_txn_tbl    aga,
            gl_code_combinations         glcc
         WHERE
                1 = 1
              AND aila.dist_code_combination_id = glcc.code_combination_id
               AND glcc.segment1                 = aga.corp
               AND glcc.segment2                 = aga.util
               AND glcc.segment3                 = aga.business_division
               AND glcc.segment4                 = aga.major_minor
               AND glcc.segment5                 = aga.fmc
               AND glcc.segment6                 = aga.rmc
               AND glcc.segment7                 = aga.tran_type
               AND glcc.segment8                 = aga.project
               AND glcc.segment9                 = aga.product
               AND glcc.segment10                = aga.activity
               AND glcc.segment11                = aga.resource_type
              AND aga.batch_no = gv_batch
    )                                                                                                        pjc_task_number,
    (
        SELECT
		CASE when (aga.project = '*****' or aga.project  is null) then null else
            aga.exp_type_out end
          FROM
            aee_gl_aavm_batch_txn_tbl    aga,
            gl_code_combinations         glcc
         WHERE
                1 = 1
               AND aila.dist_code_combination_id = glcc.code_combination_id
               AND glcc.segment1                 = aga.corp
               AND glcc.segment2                 = aga.util
               AND glcc.segment3                 = aga.business_division
               AND glcc.segment4                 = aga.major_minor
               AND glcc.segment5                 = aga.fmc
               AND glcc.segment6                 = aga.rmc
               AND glcc.segment7                 = aga.tran_type
               AND glcc.segment8                 = aga.project
               AND glcc.segment9                 = aga.product
               AND glcc.segment10                = aga.activity
               AND glcc.segment11                = aga.resource_type
               AND aga.batch_no = gv_batch
    )                                                                                                        pjc_expenditure_type_name,
    (
        SELECT
		CASE when (aga.project = '*****' or aga.project  is null) then null else
             aga.exp_org_out end
          FROM
            aee_gl_aavm_batch_txn_tbl    aga,
            gl_code_combinations         glcc
         WHERE
                1 = 1
               AND aila.dist_code_combination_id = glcc.code_combination_id
               AND glcc.segment1                 = aga.corp
               AND glcc.segment2                 = aga.util
               AND glcc.segment3                 = aga.business_division
               AND glcc.segment4                 = aga.major_minor
               AND glcc.segment5                 = aga.fmc
               AND glcc.segment6                 = aga.rmc
               AND glcc.segment7                 = aga.tran_type
               AND glcc.segment8                 = aga.project
               AND glcc.segment9                 = aga.product
               AND glcc.segment10                = aga.activity
               AND glcc.segment11                = aga.resource_type
               AND aga.batch_no = gv_batch
    )                                                                                                        pjc_organization_name,
    NULL                                                                                                     pjc_reserved_attribute1,
    NULL                                                                                                     pjc_reserved_attribute2,
    NULL                                                                                                     pjc_reserved_attribute3,
    NULL                                                                                                     pjc_reserved_attribute4,
    NULL                                                                                                     pjc_reserved_attribute5,
    NULL                                                                                                     pjc_reserved_attribute6,
    NULL                                                                                                     pjc_reserved_attribute7,
    NULL                                                                                                     pjc_reserved_attribute8,
    NULL                                                                                                     pjc_reserved_attribute9,
    NULL                                                                                                     pjc_reserved_attribute10,
    NULL                                                                                                     pjc_user_def_attribute1,
    NULL                                                                                                     pjc_user_def_attribute2,
    NULL                                                                                                     pjc_user_def_attribute3,
    NULL                                                                                                     pjc_user_def_attribute4,
    NULL                                                                                                     pjc_user_def_attribute5,
    NULL                                                                                                     pjc_user_def_attribute6,
    NULL                                                                                                     pjc_user_def_attribute7,
    NULL                                                                                                     pjc_user_def_attribute8,
    NULL                                                                                                     pjc_user_def_attribute9,
    NULL                                                                                                     pjc_user_def_attribute10,
    NULL                                                                                                     fiscal_charge_type,
    NULL                                                                                                     def_acctg_start_date,
    NULL                                                                                                     def_acctg_end_date,
    NULL                                                                                                     def_accural_code_concatenated,
    NULL                                                                                                     pjc_project_name,
    NULL                                                                                                     jc_task_name
  FROM
    ap_invoices_interface      aia,
    ap_invoice_lines_interface aila
 WHERE
        1 = 1
       AND aia.invoice_id = aila.invoice_id
       AND aia.invoice_amount != 0
       AND aia.status IS NULL
       AND ( aia.attribute1 IS NULL
        OR aia.attribute1 NOT IN ( 20, 21, 23, 25, 29,
                                   31, 32, 34, 38 ) )
       AND aia.vendor_id IS NOT NULL
       AND aia.vendor_site_id IS NOT NULL
       AND aia.invoice_num IS NOT NULL
       AND aia.invoice_date IS NOT NULL
       AND aia.invoice_type_lookup_code IS NOT NULL
       AND aia.voucher_num IS NOT NULL
       AND aila.line_number IS NOT NULL
       AND aila.line_type_lookup_code IS NOT NULL  );


        lc_header    VARCHAR2(32000) := NULL;
        lc_data_type VARCHAR2(32000) := NULL;

	   --Changes for generating CSV file.
        v_utlfile_issue EXCEPTION;
        v_dir_path_fail EXCEPTION;
        v_no_data_to_process EXCEPTION;
        lv_error_msg VARCHAR2(2000) := NULL;
        f_handle     utl_file.file_type;
        p_f_handle   utl_file.file_type;
    BEGIN

	    -- Changes for generating CSV file. --
        l_ext_file_name := 'ApInvoicesInterface_'
                           || 'AP_INV_INT_LINES'
                           || gv_batch_no
                           || '.csv';
        aee_common_util_pkg.validate_ext_dir(l_ext_file_loc, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_dir_path_fail;
        END IF;

        aee_common_util_pkg.openfile(l_ext_file_name, l_ext_file_loc, gv_process_sts, lv_error_msg, p_f_handle);
        fnd_file.put_line(fnd_file.log, 'Flag gv_process_sts value - ' || gv_process_sts);
        f_handle := p_f_handle;
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        lc_header := chr(34)
                     || 'INVOICE_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LINE_TYPE_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'QUANTITY_INVOICED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'UNIT_PRICE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'UNIT_OF_MEAS_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DESCRIPTION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_SHIPMENT_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_DISTRIBUTION_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ITEM_DESCRIPTION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'RELEASE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PURCHASING_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'RECEIPT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'RECEIPT_LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONSUMPTION_ADVICE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONSUMPTION_ADVICE_LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PACKING_SLIP'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FINAL_MATCH_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DIST_CODE_CONCATENATED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DISTRIBUTION_SET_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ACCOUNTING_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ACCOUNT_SEGMENT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'BALANCING_SEGMENT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'COST_CENTER_SEGMENT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_CLASSIFICATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SHIP_TO_LOCATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SHIP_FROM_LOCATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FINAL_DISCHARGE_LOCATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TRX_BUSINESS_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRODUCT_FISC_CLASSIFICATION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRIMARY_INTENDED_USE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'USER_DEFINED_FISC_CLASS'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRODUCT_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSESSABLE_VALUE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRODUCT_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONTROL_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_REGIME_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_STATUS_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_JURISDICTION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_RATE_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_RATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AWT_GROUP_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TYPE_1099'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INCOME_TAX_REGION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRORATE_ACROSS_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LINE_GROUP_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'COST_FACTOR_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'STAT_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSETS_TRACKING_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSET_BOOK_TYPE_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSET_CATEGORY_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SERIAL_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'MANUFACTURER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'MODEL_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'WARRANTY_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRICE_CORRECTION_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRICE_CORRECT_INV_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRICE_CORRECT_INV_LINE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_FIRST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_LAST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_EMPLOYEE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_CATGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE16'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE17'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE18'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE19'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE20'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_PROJECT_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_TASK_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_EXPENDITURE_TYPE_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_EXPENDITURE_ITEM_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_ORGANIZATION_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_PROJECT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_TASK_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_EXPENDITURE_TYPE_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_ORGANIZATION_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FISCAL_CHARGE_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DEF_ACCTG_START_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DEF_ACCTG_END_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DEF_ACCURAL_CODE_CONCATENATED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_PROJECT_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'JC_TASK_NAME'
                     || chr(34);

        lc_data_type := 'VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2';

		-- Changes for generating CSV file
        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_data_type, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        FOR cur_rec IN cur_invoice_line_data LOOP
            lc_header := chr(34)
                         || cur_rec.invoice_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.line_type_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.quantity_invoiced
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.unit_price
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.unit_of_meas_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.description
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_shipment_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_distribution_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.item_description
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.release_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.purchasing_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.receipt_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.receipt_line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.consumption_advice_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.consumption_advice_line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.packing_slip
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.final_match_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.dist_code_concatenated
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.distribution_set_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.accounting_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.account_segment
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.balancing_segment
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cost_center_segment
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_classification_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.ship_to_location_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.ship_from_location_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.final_discharge_location_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.trx_business_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.product_fisc_classification
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.primary_intended_use
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.user_defined_fisc_class
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.product_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.assessable_value
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.product_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.control_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_regime_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_status_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_jurisdiction_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_rate_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_rate
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.awt_group_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.type_1099
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.income_tax_region
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prorate_across_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.line_group_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cost_factor_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.stat_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.assets_tracking_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.asset_book_type_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.asset_category_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.serial_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.manufacturer
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.model_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.warranty_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.price_correction_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.price_correct_inv_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.price_correct_inv_line_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_first_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_last_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_employee_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_catgory
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute16
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute17
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute18
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute19
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute20
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_project_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_task_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_expenditure_type_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_expenditure_item_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_organization_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_project_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_task_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_expenditure_type_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_organization_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.fiscal_charge_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.def_acctg_start_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.def_acctg_end_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.def_accural_code_concatenated
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_project_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.jc_task_name
                         || chr(34);

	 --Changes for generating CSV file

            aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
            IF gv_process_sts = 'N' THEN
                gv_error_msg := gv_error_msg || lv_error_msg;
                RAISE v_utlfile_issue;
            END IF;
--gv_count:=gv_count+1;

        END LOOP;
--dbms_output.put_line('Records in output file: '||gv_count);

        utl_file.fclose(f_handle); -- Change
        gv_retcode_out := 0;
        gv_errbuf_out := NULL;
    EXCEPTION
        WHEN v_dir_path_fail THEN
            fnd_file.put_line(fnd_file.log, 'v_dir_path_fail Exception of proc ap_paid_invoices_lines_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_dir_path_fail';
        WHEN v_utlfile_issue THEN
            fnd_file.put_line(fnd_file.log, 'v_utlfile_issue Exception of proc ap_paid_invoices_lines_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_utlfile_issue';
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while fetching the data for paid lines with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END ap_inv_int_lines_extract;
	
----------------------------------------------------------------------------------------------------------------------------------------------
--  API Invoice Description --
----------------------------------------------------------------------------------------------------------------------------------------------

    PROCEDURE ap_inv_int_descp IS

        CURSOR cur_invoice_hdr_data IS
        SELECT DISTINCT
    aia.invoice_id invoice_id,
    aia.invoice_num,
    aia.invoice_type_lookup_code,
    aia.last_update_date,
    aia.creation_date,
    aia.attribute1,
    decode(aia.attribute1,'38','REJECTED-PCARD PO'
,'39','REQUIRES BANKING REVIEW'
,'40','APPVL REVIEW BY BANKING'
,'1','API-APPROVED'
,'2','GAERROR'
,'3','UNAUTHORIZED AMT'
,'5','POR'
,'6','OUTOFBALANCE'
,'7','GAVALID'
,'8','OVERPODOLLARS'
,'9','API-PARTIAL-FINAL'
,'10','PREAPPROVED'
,'11','ACCOUNTING ERROR'
,'12','WF-ESCALATED NO APPR'
,'13','UNAPPROVED BY AP'
,'20','REJECTED'
,'99',NULL  
,'14','PO PRICE MISMATCH'
,'15','QUANTITY MISMATCH'
,'16','MISC AMT ERROR'
,'17','FREIGHT AMT ERROR'
,'18','PENDING BUYER APPRV'
,'19','BUYER REVIEWED'
,'21','ERROR'
,'22','BUYER APPROVED'
,'23','BUYER REJECTED'
,'24','PAY PER PO'
,'25','BUYER SENT BACK'
,'30','POR INPROCESS'
,'27','WF-ESCALATE NO BUYER'
,'31','INVALID PO+LN+SHIP'
,'28','UOM MISMATCH'
,'29','HEADER-LINE MISMATCH'
,'32','DUPLICATE ELEC INV'
,'33','REQUIRES TAX APPROVAL'
,'34','REQUIRES LIEN WAIVER APPROVAL'
,'35','REQUIRES CAG APPROVAL'
,'36','CAG REJECTED'
,'37','PO UPDATE PENDING'
,'41','API-APPVD, PENDING VALIDATION'
,'42','PENDING CHAIRMAN APPROVAL') approval_status,

  aia.workflow_flag
  FROM
    ap_invoices_interface      aia,
    ap_invoice_lines_interface aila
 WHERE
        1 = 1     
       AND aia.invoice_id = aila.invoice_id
       AND aia.invoice_amount != 0
       AND aia.status IS NULL
       AND (aia.attribute1 IS NULL or aia.attribute1 NOT IN ( 20, 21, 23, 25, 29, 31, 32, 34, 38 )  )
   AND aia.vendor_id IS NOT NULL
   AND aia.vendor_site_id IS NOT NULL
   AND aia.invoice_num IS NOT NULL
   AND aia.invoice_date IS NOT NULL
   AND aia.invoice_type_lookup_code IS NOT NULL
   AND aia.voucher_num IS NOT NULL
   AND aila.line_number IS NOT NULL
   AND aila.line_type_lookup_code IS NOT NULL  ;
		 
		 
        lc_header    VARCHAR2(32000) := NULL;
        lc_data_type VARCHAR2(32000) := NULL;

	   --Changes for generating CSV file.
        v_utlfile_issue EXCEPTION;
        v_dir_path_fail EXCEPTION;
        v_no_data_to_process EXCEPTION;
        lv_error_msg VARCHAR2(2000) := NULL;
        f_handle     utl_file.file_type;
        p_f_handle   utl_file.file_type;
    BEGIN

	    -- Changes for generating CSV file. --
        l_ext_file_name := 'ApInvoicesInterface_description_.csv';
		
        aee_common_util_pkg.validate_ext_dir(l_ext_file_loc, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_dir_path_fail;
        END IF;

        aee_common_util_pkg.openfile(l_ext_file_name, l_ext_file_loc, gv_process_sts, lv_error_msg, p_f_handle);
        fnd_file.put_line(fnd_file.log, 'Flag gv_process_sts value - ' || gv_process_sts);
        f_handle := p_f_handle;
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        lc_header := chr(34)
                     || 'INVOICE_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'invoice_num'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'invoice_type_lookup_code'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'last_update_date'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'creation_date'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'attribute1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'approval_status'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'workflow_flag'
                     || chr(34);

        lc_data_type := 'VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2';

		-- Changes for generating CSV file
        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
		
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        FOR cur_rec IN cur_invoice_hdr_data LOOP
            lc_header := chr(34)
                         || cur_rec.invoice_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_type_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.last_update_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.creation_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.approval_status
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.workflow_flag
                         || chr(34);

	 --Changes for generating CSV file

            aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
            IF gv_process_sts = 'N' THEN
                gv_error_msg := gv_error_msg || lv_error_msg;
                RAISE v_utlfile_issue;
            END IF;


        END LOOP;


        utl_file.fclose(f_handle); -- Change
        gv_retcode_out := 0;
        gv_errbuf_out := NULL;
    EXCEPTION
        WHEN v_dir_path_fail THEN
            fnd_file.put_line(fnd_file.log, 'v_dir_path_fail Exception of proc ap_inv_int_descp, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_dir_path_fail';
        WHEN v_utlfile_issue THEN
            fnd_file.put_line(fnd_file.log, 'v_utlfile_issue Exception of proc ap_inv_int_descp, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_utlfile_issue';
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while fetching the data for Interface invoices details file with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END ap_inv_int_descp;
	
----------------------------------------------------------------------------------------------------------------------------------------------
-- Extracting Invoice Lines --
----------------------------------------------------------------------------------------------------------------------------------------------

    PROCEDURE ap_holds_all_Lines_extract (p_business_extract IN VARCHAR2)IS

        CURSOR cur_invoice_line_data IS
       SELECT
              ROW_NUMBER()
            OVER(PARTITION BY a.invoice_id, a.line_type_lookup_code
                 ORDER BY
                    a.invoice_id,  a.line_type_lookup_code ASC
            ) line_group_number,
            a.*
  FROM
    (SELECT DISTINCT
            aila.invoice_id                                                                                          invoice_id,
         CASE
                WHEN aia.invoice_id = (
                    SELECT DISTINCT
                        invoice_id
                      FROM
                        (
                            SELECT DISTINCT
                                ad.invoice_id,
                                ad.invoice_line_number,
                                COUNT(ad.invoice_id)
                              FROM
                                ap_invoice_distributions_all ad,ap_invoice_lines_all al
                             WHERE al.invoice_id= ad.invoice_id and al.line_number = ad.invoice_line_number and 
                                ad.invoice_id = aia.invoice_id
                                AND CASE
                       WHEN al.line_type_lookup_code = 'TAX' --40032
                          AND ad.amount = 0 THEN
                           0
                       ELSE
                           1
                   END = 1

                             GROUP BY
                                ad.invoice_id,
                                ad.invoice_line_number
                            HAVING
                                COUNT(ad.invoice_id) > 1
                        )
                ) THEN
                    aida.invoice_line_number || aida.distribution_line_number
                ELSE
                    to_char(aila.line_number)
            END                                                                                                      line_number,
            decode(aila.line_type_lookup_code,'RETAINAGE RELEASE','ITEM',aila.line_type_lookup_code)                                                                               line_type_lookup_code,
            aida.amount                                                                                              amount,
            aida.quantity_invoiced                                                                                   quantity_invoiced,
            aida.unit_price                                                                                          unit_price,
            NULL                                                                                                     unit_of_meas_lookup_code,
            substr(replace(replace(replace(replace(aila.description, '""', '"'), '"', '""'), CHR(10), NULL), CHR(13), NULL),1,240) description,
            /*pha.segment1*/
			CASE
                WHEN (
                    SELECT
                        release_num
                    FROM
                        po_releases_all pra
                    WHERE
                        pra.po_release_id = aila.po_release_id
                ) IS NULL THEN
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                ELSE
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                    || '-'
                    || (
                        SELECT
                            release_num
                        FROM
                            po_releases_all pra
                        WHERE
                            pra.po_release_id = aila.po_release_id
                    )
            END po_number,
            pla.line_num                                                                                             po_line_number,
            plla.shipment_num                                                                                        po_shipment_num,
            pda.distribution_num                                                                                     po_distribution_num,
            NULL                                                                                                     item_description,
            NULL                                                                                                     release_num,
            NULL                                                                                                     purchasing_category,
            NULL                                                                                                     receipt_number,
            NULL                                                                                                     receipt_line_number,
            NULL                                                                                                     consumption_advice_number,
            NULL                                                                                                     consumption_advice_line_number,
            NULL                                                                                                     packing_slip,
            NULL                                                                                                     final_match_flag,
           ( aga.oracle_company
              || '.'
              || aga.oracle_product
              || '.'
              || aga.oracle_account
              || '.'
              || aga.oracle_cost_center
              || '.'
              || aga.oracle_location
              || '.'
              || aga.oracle_compliance_code
              || '.'
              || aga.oracle_intercompany
              || '.'
              || aga.oracle_resource_type
              || '.'
              || aga.oracle_future )                                                                                 dist_code_concatenated,
            NULL                                                                                                     distribution_set_name,
            TO_CHAR(TO_DATE(/*'30-APR-2023'*/'30-SEP-2022'), 'YYYY/MM/DD')                                           accounting_date,  -- to be checked with adnan
            aga.oracle_account                                                                                                 account_segment,
            aga.oracle_company                                                                                            balancing_segment,
            aga.oracle_cost_center                                                                                                   cost_center_segment,
            aila.tax_classification_code                                                                             tax_classification_code,
            hla.location_code                                                                                        ship_to_location_code,
            NULL                                                                                                     ship_from_location_code,
            NULL                                                                                                     final_discharge_location_code,
            NULL                                                                                                     trx_business_category,
            NULL                                                                                                     product_fisc_classification,
            NULL                                                                                                     primary_intended_use,
            NULL                                                                                                     user_defined_fisc_class,
            NULL                                                                                                     product_type,
            NULL                                                                                                     assessable_value,
            NULL                                                                                                     product_category,
            aia.control_amount                                                                                       control_amount,
            aila.tax_regime_code                                                                                     tax_regime_code,
            aila.tax                                                                                                 tax,
            aila.tax_status_code                                                                                     tax_status_code,
            aila.tax_jurisdiction_code                                                                               tax_jurisdiction_code,
            aila.tax_rate_code                                                                                       tax_rate_code,
            aila.tax_rate                                                                                            tax_rate,
            NULL                                                                                                     awt_group_name,
            Case 
			When aila.type_1099 ='1099S' 
			THEN NULL 
			ELSE aila.type_1099 
			END                                                                                                      type_1099,
            aila.income_tax_region                                                                                   income_tax_region,
            'Y'                                                                                                      prorate_across_flag,
            NULL                                                                                                     cost_factor_name,
            NULL                                                                                                     stat_amount,
            NULL                                                                                                     assets_tracking_flag,
            NULL                                                                                                     asset_book_type_code,
            NULL                                                                                                     asset_category_id,
            NULL                                                                                                     serial_number,
            NULL                                                                                                     manufacturer,
            NULL                                                                                                     model_number,
            NULL                                                                                                     warranty_number,
            NULL                                                                                                     price_correction_flag,
            NULL                                                                                                     price_correct_inv_num,
            NULL                                                                                                     price_correct_inv_line_num,
            NULL                                                                                                     requester_first_name,
            NULL                                                                                                     requester_last_name,
            NULL                                                                                                     requester_employee_num,
          'CONVERSION'                                                                                             attribute_category,
            NULL attribute1,
            NULL attribute2,
            (
                glcc.segment1
              || '.'
              || glcc.segment2
              || '.'
              || glcc.segment3
              || '.'
              || glcc.segment4
              || '.'
              || glcc.segment5
              || '.'
              || glcc.segment6
              || '.'
              || glcc.segment7
              || '.'
              || glcc.segment8
              || '.'
              || glcc.segment9
              || '.'
              || glcc.segment10
              || '.'
              || glcc.segment11
              || '.'
              || glcc.segment12
              || '.'
              || glcc.segment13
                )                                                                                                     attribute3,
            NULL                                                                                                     attribute4,
            NULL                                                                                                     attribute5,
            NULL                                                                                                     attribute6,
            NULL                                                                                                     attribute7,
            NULL                                                                                                     attribute8,
            NULL                                                                                                     attribute9,
            NULL                                                                                                     attribute10,
            NULL                                                                                                     attribute11,
            NULL                                                                                                     attribute12,
            NULL                                                                                                     attribute13,
            NULL                                                                                                     attribute14,
            NULL                                                                                                     attribute15,
            NULL                                                                                                     attribute_number1,
            NULL                                                                                                     attribute_number2,
            NULL                                                                                                     attribute_number3,
            NULL                                                                                                     attribute_number4,
            NULL                                                                                                     attribute_number5,
            NULL                                                                                                     attribute_date1,
            NULL                                                                                                     attribute_date2,
            NULL                                                                                                     attribute_date3,
            NULL                                                                                                     attribute_date4,
            NULL                                                                                                     attribute_date5,
            NULL                                                                                                     global_attribute_catgory,
            decode(p_business_extract, 'Y',(
                SELECT
                    closed_code
                FROM
                    po_headers_all pha
                WHERE
                    pha.po_header_id = aila.po_header_id
            ), NULL)                                                                                                 global_attribute1,
            decode(p_business_extract, 'Y',(
                SELECT
                    closed_code
                FROM
                    po_releases_all pra
                WHERE
                    pra.po_release_id = aila.po_release_id
            ), NULL)                                                                                                 global_attribute2,
            decode(p_business_extract, 'Y',(
                SELECT
                    closed_code
                FROM
                    po_lines_all pla
                WHERE
                    pla.po_line_id = aila.po_line_id
            ), NULL)                                                                                                 global_attribute3,
            decode(p_business_extract, 'Y',(
                SELECT
                    closed_code
                FROM
                    po_line_locations_all plla
                WHERE
                    plla.line_location_id = aila.po_line_location_id
            ), NULL)                                                                                                 global_attribute4,
            NULL                                                                                                     global_attribute5,
            NULL                                                                                                     global_attribute6,
            NULL                                                                                                     global_attribute7,
            NULL                                                                                                     global_attribute8,
            NULL                                                                                                     global_attribute9,
            NULL                                                                                                     global_attribute10,
            NULL                                                                                                     global_attribute11,
            NULL                                                                                                     global_attribute12,
            NULL                                                                                                     global_attribute13,
            NULL                                                                                                     global_attribute14,
            NULL                                                                                                     global_attribute15,
            NULL                                                                                                     global_attribute16,
            NULL                                                                                                     global_attribute17,
            NULL                                                                                                     global_attribute18,
            NULL                                                                                                     global_attribute19,
            NULL                                                                                                     global_attribute20,
            NULL                                                                                                     global_attribute_number1,
            NULL                                                                                                     global_attribute_number2,
            NULL                                                                                                     global_attribute_number3,
            NULL                                                                                                     global_attribute_number4,
            NULL                                                                                                     global_attribute_number5,
            NULL                                                                                                     global_attribute_date1,
            NULL                                                                                                     global_attribute_date2,
            NULL                                                                                                     global_attribute_date3,
            NULL                                                                                                     global_attribute_date4,
            NULL                                                                                                     global_attribute_date5,
            NULL                                                                                                     pjc_project_id,
            NULL                                                                                                     pjc_task_id,
            NULL                                                                                                     pjc_expenditure_type_id,
            NULL                                                                                                     pjc_expenditure_item_date,
            NULL                                                                                                     pjc_organization_id,            
			CASE when (aga.project = '*****' or aga.project  is null) then null else aga.proj_out      end           pjc_project_number,
            CASE when (aga.project = '*****' or aga.project  is null) then null else aga.task_out      end           pjc_task_number,
            CASE when (aga.project = '*****' or aga.project  is null) then null else aga.exp_type_out  end           pjc_expenditure_type_name,
            CASE when (aga.project = '*****' or aga.project  is null) then null else aga.exp_org_out   end           pjc_organization_name,
            NULL                                                                                                     pjc_reserved_attribute1,
            NULL                                                                                                     pjc_reserved_attribute2,
            NULL                                                                                                     pjc_reserved_attribute3,
            NULL                                                                                                     pjc_reserved_attribute4,
            NULL                                                                                                     pjc_reserved_attribute5,
            NULL                                                                                                     pjc_reserved_attribute6,
            NULL                                                                                                     pjc_reserved_attribute7,
            NULL                                                                                                     pjc_reserved_attribute8,
            NULL                                                                                                     pjc_reserved_attribute9,
            NULL                                                                                                     pjc_reserved_attribute10,
            NULL                                                                                                     pjc_user_def_attribute1,
            NULL                                                                                                     pjc_user_def_attribute2,
            NULL                                                                                                     pjc_user_def_attribute3,
            NULL                                                                                                     pjc_user_def_attribute4,
            NULL                                                                                                     pjc_user_def_attribute5,
            NULL                                                                                                     pjc_user_def_attribute6,
            NULL                                                                                                     pjc_user_def_attribute7,
            NULL                                                                                                     pjc_user_def_attribute8,
            NULL                                                                                                     pjc_user_def_attribute9,
            NULL                                                                                                     pjc_user_def_attribute10,
            NULL                                                                                                     fiscal_charge_type,
            NULL                                                                                                     def_acctg_start_date,
            NULL                                                                                                     def_acctg_end_date,
            NULL                                                                                                     def_accural_code_concatenated,
            NULL                                                                                                     pjc_project_name,
            NULL                                                                                                     jc_task_name
          FROM
            ap_invoices_all              aia,
            ap_invoice_lines_all         aila,
            ap_suppliers                 aps,
            ap_supplier_sites_all        apss,
            ap_holds_all                 aha,
            ap_invoice_distributions_all aida,
            gl_code_combinations         glcc,
            po_headers_all               pha,
            po_lines_all                 pla,
            po_line_locations_all        plla,
            po_distributions_all         pda,
            po_releases_all              pra,
            hr_locations_all             hla
			,aee_gl_aavm_batch_txn_tbl aga
         WHERE
                1 = 1
               AND aha.invoice_id                                                                     = aia.invoice_id
               AND aha.release_lookup_code IS NULL
               AND aia.invoice_id                                                                     = aila.invoice_id
               AND aia.vendor_id                                                                      = aps.vendor_id
               AND aia.vendor_site_id                                                                 = apss.vendor_site_id
               AND aia.payment_status_flag                                                            = 'N'
               AND aia.cancelled_amount IS NULL
               AND aia.invoice_amount != 0
               AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')
               AND aida.invoice_id                                                                    = aia.invoice_id
               AND aida.invoice_line_number                                                           = aila.line_number
               AND aida.dist_code_combination_id                                                      = glcc.code_combination_id
               AND aila.po_header_id                                                                  = pha.po_header_id (+)
               AND aila.po_line_id                                                                    = pla.po_line_id (+)
               AND aila.po_line_location_id                                                           = plla.line_location_id (+)
               AND aila.po_distribution_id                                                            = pda.po_distribution_id (+)
               AND aila.po_release_id                                                                 = pra.po_release_id (+)
               AND aila.ship_to_location_id                                                           = hla.location_id (+)			   
			   AND glcc.segment1                                      = aga.corp
               AND glcc.segment2                                      = aga.util
               AND glcc.segment3                                      = aga.business_division
               AND glcc.segment4                                      = aga.major_minor
               AND glcc.segment5                                      = aga.fmc
               AND glcc.segment6                                      = aga.rmc
               AND glcc.segment7                                      = aga.tran_type
               AND glcc.segment8                                      = aga.project
               AND glcc.segment9                                      = aga.product
               AND glcc.segment10                                     = aga.activity
               AND glcc.segment11                                     = aga.resource_type
               AND aga.batch_no = gv_batch
               AND CASE
                       WHEN aila.line_type_lookup_code = 'TAX' 
                          AND aida.amount                                                                        = 0 THEN
                           0
                       ELSE
                           1
                   END = 1

    ) a;

        lc_header    VARCHAR2(32000) := NULL;
        lc_data_type VARCHAR2(32000) := NULL;

	   --Changes for generating CSV file.
        v_utlfile_issue EXCEPTION;
        v_dir_path_fail EXCEPTION;
        v_no_data_to_process EXCEPTION;
        lv_error_msg VARCHAR2(2000) := NULL;
        f_handle     utl_file.file_type;
        p_f_handle   utl_file.file_type;
    BEGIN

	    -- Changes for generating CSV file. --
        l_ext_file_name := 'ApInvoicesInterface_'
                           || 'AP_Holds_All_LINES'
                           || gv_batch_no
                           || '.csv';
        aee_common_util_pkg.validate_ext_dir(l_ext_file_loc, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_dir_path_fail;
        END IF;

        aee_common_util_pkg.openfile(l_ext_file_name, l_ext_file_loc, gv_process_sts, lv_error_msg, p_f_handle);
        fnd_file.put_line(fnd_file.log, 'Flag gv_process_sts value - ' || gv_process_sts);
        f_handle := p_f_handle;
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        lc_header := chr(34)
                     || 'INVOICE_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LINE_TYPE_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'QUANTITY_INVOICED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'UNIT_PRICE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'UNIT_OF_MEAS_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DESCRIPTION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_SHIPMENT_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_DISTRIBUTION_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ITEM_DESCRIPTION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'RELEASE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PURCHASING_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'RECEIPT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'RECEIPT_LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONSUMPTION_ADVICE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONSUMPTION_ADVICE_LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PACKING_SLIP'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FINAL_MATCH_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DIST_CODE_CONCATENATED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DISTRIBUTION_SET_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ACCOUNTING_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ACCOUNT_SEGMENT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'BALANCING_SEGMENT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'COST_CENTER_SEGMENT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_CLASSIFICATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SHIP_TO_LOCATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SHIP_FROM_LOCATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FINAL_DISCHARGE_LOCATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TRX_BUSINESS_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRODUCT_FISC_CLASSIFICATION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRIMARY_INTENDED_USE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'USER_DEFINED_FISC_CLASS'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRODUCT_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSESSABLE_VALUE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRODUCT_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONTROL_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_REGIME_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_STATUS_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_JURISDICTION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_RATE_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_RATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AWT_GROUP_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TYPE_1099'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INCOME_TAX_REGION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRORATE_ACROSS_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LINE_GROUP_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'COST_FACTOR_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'STAT_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSETS_TRACKING_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSET_BOOK_TYPE_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSET_CATEGORY_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SERIAL_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'MANUFACTURER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'MODEL_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'WARRANTY_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRICE_CORRECTION_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRICE_CORRECT_INV_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRICE_CORRECT_INV_LINE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_FIRST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_LAST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_EMPLOYEE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_CATGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE16'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE17'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE18'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE19'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE20'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_PROJECT_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_TASK_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_EXPENDITURE_TYPE_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_EXPENDITURE_ITEM_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_ORGANIZATION_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_PROJECT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_TASK_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_EXPENDITURE_TYPE_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_ORGANIZATION_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FISCAL_CHARGE_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DEF_ACCTG_START_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DEF_ACCTG_END_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DEF_ACCURAL_CODE_CONCATENATED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_PROJECT_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'JC_TASK_NAME'
                     || chr(34);

        lc_data_type := 'VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2';

		-- Changes for generating CSV file
        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_data_type, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        FOR cur_rec IN cur_invoice_line_data LOOP
            lc_header := chr(34)
                         || cur_rec.invoice_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.line_type_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.quantity_invoiced
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.unit_price
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.unit_of_meas_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.description
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_shipment_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_distribution_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.item_description
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.release_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.purchasing_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.receipt_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.receipt_line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.consumption_advice_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.consumption_advice_line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.packing_slip
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.final_match_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.dist_code_concatenated
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.distribution_set_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.accounting_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.account_segment
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.balancing_segment
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cost_center_segment
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_classification_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.ship_to_location_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.ship_from_location_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.final_discharge_location_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.trx_business_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.product_fisc_classification
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.primary_intended_use
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.user_defined_fisc_class
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.product_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.assessable_value
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.product_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.control_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_regime_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_status_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_jurisdiction_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_rate_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_rate
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.awt_group_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.type_1099
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.income_tax_region
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prorate_across_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.line_group_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cost_factor_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.stat_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.assets_tracking_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.asset_book_type_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.asset_category_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.serial_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.manufacturer
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.model_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.warranty_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.price_correction_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.price_correct_inv_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.price_correct_inv_line_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_first_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_last_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_employee_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_catgory
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute16
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute17
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute18
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute19
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute20
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_project_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_task_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_expenditure_type_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_expenditure_item_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_organization_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_project_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_task_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_expenditure_type_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_organization_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.fiscal_charge_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.def_acctg_start_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.def_acctg_end_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.def_accural_code_concatenated
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_project_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.jc_task_name
                         || chr(34);

	 --Changes for generating CSV file

            aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
            IF gv_process_sts = 'N' THEN
                gv_error_msg := gv_error_msg || lv_error_msg;
                RAISE v_utlfile_issue;
            END IF;
--gv_count:=gv_count+1;

        END LOOP;
--dbms_output.put_line('Records in output file: '||gv_count);

        utl_file.fclose(f_handle); -- Change
        gv_retcode_out := 0;
        gv_errbuf_out := NULL;
    EXCEPTION
        WHEN v_dir_path_fail THEN
            fnd_file.put_line(fnd_file.log, 'v_dir_path_fail Exception of proc ap_paid_invoices_lines_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_dir_path_fail';
        WHEN v_utlfile_issue THEN
            fnd_file.put_line(fnd_file.log, 'v_utlfile_issue Exception of proc ap_paid_invoices_lines_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_utlfile_issue';
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while fetching the data for paid lines with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END ap_holds_all_Lines_extract;
	

----------------------------------------------------------------------------------------------------------------------------------------------
-- AP_HOLDS description file --
----------------------------------------------------------------------------------------------------------------------------------------------

    PROCEDURE ap_holds_all_descp IS

        CURSOR cur_invoice_hdr_data IS
     SELECT aha.*
	 FROM ap_invoices_all              aia,
            ap_suppliers                 aps,
            ap_supplier_sites_all        apss,
            ap_terms                     apt,
            hr_operating_units           hou,
            ap_holds_all                 aha            
         WHERE
                1 = 1
               AND aia.invoice_id                = aha.invoice_id
               AND aha.release_lookup_code IS NULL
               AND aia.vendor_id                 = aps.vendor_id
               AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')
               AND aia.payment_status_flag       = 'N'
               AND aia.cancelled_amount IS NULL
               AND aia.vendor_site_id            = apss.vendor_site_id
               AND aps.vendor_id                 = apss.vendor_id
               AND apt.term_id                   = aia.terms_id
               AND hou.organization_id           = aia.org_id
               AND aia.invoice_amount != 0 ;

        lc_header    VARCHAR2(32000) := NULL;
        lc_data_type VARCHAR2(32000) := NULL;

	   --Changes for generating CSV file.
        v_utlfile_issue EXCEPTION;
        v_dir_path_fail EXCEPTION;
        v_no_data_to_process EXCEPTION;
        lv_error_msg VARCHAR2(2000) := NULL;
        f_handle     utl_file.file_type;
        p_f_handle   utl_file.file_type;
    BEGIN

	    -- Changes for generating CSV file. --
        l_ext_file_name := 'AP_HOLDS_ALL_DESCRIPTION.csv';
                         
        aee_common_util_pkg.validate_ext_dir(l_ext_file_loc, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_dir_path_fail;
        END IF;

        aee_common_util_pkg.openfile(l_ext_file_name, l_ext_file_loc, gv_process_sts, lv_error_msg, p_f_handle);
        fnd_file.put_line(fnd_file.log, 'Flag gv_process_sts value - ' || gv_process_sts);
        f_handle := p_f_handle;
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        lc_header := chr(34)
                     || 'INVOICE_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LINE_LOCATION_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'HOLD_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'HELD_BY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'HOLD_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'HOLD_REASON'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'RELEASE_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'RELEASE_REASON'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'STATUS_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE1'
                     || chr(34)
                     || ','                     
                     || chr(34)
                     || 'ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'HOLD_DETAILS'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'HOLD_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'WF_STATUS'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VALIDATION_REQUEST_ID'
                     || chr(34);

        lc_data_type := 'VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2';

		-- Changes for generating CSV file
        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
		
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        FOR cur_rec IN cur_invoice_hdr_data LOOP
            lc_header := chr(34)
                         || cur_rec.invoice_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.LINE_LOCATION_ID
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.HOLD_LOOKUP_CODE
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.HELD_BY
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.HOLD_DATE
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.HOLD_REASON
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.RELEASE_LOOKUP_CODE
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.RELEASE_REASON
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.STATUS_FLAG
                         || chr(34)
                         || ','                         
                         || chr(34)
                         || cur_rec.attribute_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.HOLD_DETAILS
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.LINE_NUMBER
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.HOLD_ID
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.WF_STATUS
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.VALIDATION_REQUEST_ID
                         || chr(34);

	 --Changes for generating CSV file

            aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
            IF gv_process_sts = 'N' THEN
                gv_error_msg := gv_error_msg || lv_error_msg;
                RAISE v_utlfile_issue;
            END IF;
--gv_count:=gv_count+1;

        END LOOP;
--dbms_output.put_line('Records in output file: '||gv_count);

        utl_file.fclose(f_handle); -- Change
        gv_retcode_out := 0;
        gv_errbuf_out := NULL;
    EXCEPTION
        WHEN v_dir_path_fail THEN
            fnd_file.put_line(fnd_file.log, 'v_dir_path_fail Exception of proc ap_holds_description, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_dir_path_fail';
        WHEN v_utlfile_issue THEN
            fnd_file.put_line(fnd_file.log, 'v_utlfile_issue Exception of proc ap_holds_description, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_utlfile_issue';
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while fetching the data for ap_holds_description with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END ap_holds_all_descp;
	

----------------------------------------------------------------------------------------------------------------------------------------------
-- Extracting Invoice Headers --
----------------------------------------------------------------------------------------------------------------------------------------------

    PROCEDURE ap_paid_invoices_header_extract IS

        CURSOR cur_invoice_hdr_data IS
        SELECT aia.invoice_id ,
               hou.name   operating_unit,
               'CONVERSION'  source,
               aia.invoice_num ,
               '0'  invoice_amount,
               to_char(aia.invoice_date, 'RRRR/MM/DD') invoice_date,
               upper(TRIM(TRIM(BOTH ' ' FROM aps.vendor_name))) vendor_name,
               aps.segment1 vendor_num,
               decode(instr(upper(apss.vendor_site_code), 'CCTM'), 0, upper(apss.vendor_site_code), replace(upper(apss.vendor_site_code),
               'CCTM', '')|| '-ERS') vendor_site_code,
               aia.invoice_currency_code,
               aia.payment_currency_code,
               NULL description,
               NULL group_id,
               case when aia.invoice_type_lookup_code ='RETAINAGE RELEASE' THEN ('STANDARD') ELSE aia.invoice_type_lookup_code END invoice_type_lookup_code,
               aia.pay_group_lookup_code legal_entity_name,
               NULL cust_registration_number,
               NULL cust_registration_code,
               NULL first_party_registration_num,
               NULL third_party_registration_num,  
               'IMMEDIATE' terms_name, --Ascend should handle IMMEDIATE to Immediate
              -- decode(apt.name, '2% 15 NET 30', '2% 10 NET 30', '2% 10 NET 45', '2% 10 NET 30',
                --      '1% 10 NET 30', '2% 10 NET 30', apt.name) terms_name,
               to_char(aia.terms_date, 'RRRR/MM/DD') terms_date,
               NULL goods_received_date,
               NULL invoice_received_date,
               to_char(last_day(sysdate), 'RRRR/MM/DD') gl_date,  -- to be changed at every run per mock
               'CLEARING' payment_method_code,
               NULL pay_group_lookup_code,
               NULL exclusive_payment_flag,
               NULL amount_applicable_to_discount,
               NULL prepay_num,
               NULL prepay_line_num,
               NULL prepay_apply_amount,
               NULL prepay_gl_date,
               NULL invoice_uncludes_prepay_flag,
               NULL exchange_rate_type,
               NULL exchange_date,
               NULL exchange_rate,
               NULL accts_pay_code_concatenated,
               NULL doc_category_code,
               aia.voucher_num ,
               NULL requester_first_name,
               NULL requester_last_name,
               NULL requester_employee_num,
               NULL delivery_channel_code,
               NULL bank_charge_bearer,
               NULL remit_to_supplier_name,
               NULL remit_to_supplier_num,
               NULL remit_to_address_name,
               NULL payment_priority,
               NULL settlement_priority,
               NULL unique_remittance_identifier,
               NULL uri_check_digit,
               NULL payment_reason_code,
               NULL payment_reason_comments,
               NULL remittance_message1,
               NULL remittance_message2,
               NULL remittance_message3,
               NULL awt_group_name,
               --(
                   --SELECT DISTINCT
                    --   location_code
                  -- FROM
                    --   apps.hr_locations_all     hla,
                   --    apps.ap_invoice_lines_all aila
                  -- WHERE
                   --        hla.location_id = aila.ship_to_location_id
                 --      AND aila.invoice_id = aia.invoice_id
                --       AND hla.ship_to_site_flag = 'Y'
                --       AND ROWNUM = 1
              -- )  
			  NULL ship_to_location,
               NULL taxation_country,
               NULL document_sub_type,
               NULL tax_invoice_internal_seq,
               NULL supplier_tax_invoice_number,
               NULL tax_invoice_recording_date,
               NULL supplier_tax_invoice_date,
               NULL supplier_tax_exchange_rate,
               NULL port_of_entry_code,
               NULL correction_year,
               NULL correction_period,
               NULL import_document_number,
               NULL import_document_date,
               NULL control_amount_number,
               'N' calc_tax_during_import_flag,
               'N' add_tax_to_inv_amt_flag,
               NULL attribute_category,
               NULL attribute1,
               NULL attribute2,
               NULL attribute3,
               NULL attribute4,
               NULL attribute5,
               NULL attribute6,
               NULL attribute7,
               NULL attribute8,
               NULL attribute9,
               NULL attribute10,
               NULL attribute11,
               NULL attribute12,
               NULL attribute13,
               NULL attribute14,
               NULL attribute15,
               NULL attribute_number1,
               NULL attribute_number2,
               NULL attribute_number3,
               NULL attribute_number4,
               NULL attribute_number5,
               NULL attribute_date1,
               NULL attribute_date2,
               NULL attribute_date3,
               NULL attribute_date4,
               NULL attribute_date5,
               NULL global_attribute_category,
               NULL global_attribute1,
               NULL global_attribute2,
               NULL global_attribute3,
               NULL global_attribute4,
               NULL global_attribute5,
               NULL global_attribute6,
               NULL global_attribute7,
               NULL global_attribute8,
               NULL global_attribute9,
               NULL global_attribute10,
               NULL global_attribute11,
               NULL global_attribute12,
               NULL global_attribute13,
               NULL global_attribute14,
               NULL global_attribute15,
               NULL global_attribute16,
               NULL global_attribute17,
               NULL global_attribute18,
               NULL global_attribute19,
               NULL global_attribute20,
               NULL global_attribute_number1,
               NULL global_attribute_number2,
               NULL global_attribute_number3,
               NULL global_attribute_number4,
               NULL global_attribute_number5,
               NULL global_attribute_date1,
               NULL global_attribute_date2,
               NULL global_attribute_date3,
               NULL global_attribute_date4,
               NULL global_attribute_date5,
               NULL image_document_url
          FROM apps.ap_invoices_all          aia
              ,apps.ap_suppliers             aps
              ,apps.ap_supplier_sites_all    apss
              ,apps.ap_invoice_payments_all  apsa
              ,apps.ap_checks_all            aca
              ,apps.ap_payment_schedules_all apsaa
              ,apps.ap_terms                 apt
              ,apps.hr_operating_units       hou

         WHERE 1 = 1
           AND aia.vendor_id           = aps.vendor_id
           AND aia.payment_status_flag = 'Y'
           AND aia.cancelled_amount IS NULL
		   AND aia.invoice_type_lookup_code <> 'PAYMENT REQUEST'
           AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')
           AND aia.vendor_site_id      = apss.vendor_site_id
           AND aps.vendor_id           = apss.vendor_id
           AND apsaa.invoice_id        = aia.invoice_id
           AND apsa.invoice_id         = aia.invoice_id
           AND aca.check_id            = apsa.check_id
           AND apt.term_id             = aia.terms_id
           AND hou.organization_id     = aia.org_id                  
           AND aca.status_lookup_code != 'VOIDED'
           AND aia.invoice_amount     != 0
           AND aia.invoice_amount      = aia.amount_paid
           AND aia.invoice_date         >= TO_DATE('2022/01/01', 'RRRR/MM/DD');


        lc_header    VARCHAR2(32000) := NULL;
        lc_data_type VARCHAR2(32000) := NULL;

	   --Changes for generating CSV file.
        v_utlfile_issue EXCEPTION;
        v_dir_path_fail EXCEPTION;
        v_no_data_to_process EXCEPTION;
        lv_error_msg VARCHAR2(2000) := NULL;
        f_handle     utl_file.file_type;
        p_f_handle   utl_file.file_type;
    BEGIN

	    -- Changes for generating CSV file. --
        l_ext_file_name := 'ApInvoicesInterface_'
                           || 'PAID_Header'
                           || gv_batch_no
                           || '.csv';
        aee_common_util_pkg.validate_ext_dir(l_ext_file_loc, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_dir_path_fail;
        END IF;

        aee_common_util_pkg.openfile(l_ext_file_name, l_ext_file_loc, gv_process_sts, lv_error_msg, p_f_handle);
        fnd_file.put_line(fnd_file.log, 'Flag gv_process_sts value - ' || gv_process_sts);
        f_handle := p_f_handle;
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        lc_header := chr(34)
                     || 'INVOICE_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'OPERATING_UNIT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SOURCE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VENDOR_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VENDOR_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VENDOR_SITE_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_CURRENCY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_CURRENCY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DESCRIPTION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GROUP_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_TYPE_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LEGAL_ENTITY_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CUST_REGISTRATION_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CUST_REGISTRATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FIRST_PARTY_REGISTRATION_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'THIRD_PARTY_REGISTRATION_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TERMS_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TERMS_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GOODS_RECEIVED_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_RECEIVED_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GL_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_METHOD_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAY_GROUP_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCLUSIVE_PAYMENT_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AMOUNT_APPLICABLE_TO_DISCOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_LINE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_APPLY_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_GL_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_UNCLUDES_PREPAY_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCHANGE_RATE_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCHANGE_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCHANGE_RATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ACCTS_PAY_CODE_CONCATENATED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DOC_CATEGORY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VOUCHER_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_FIRST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_LAST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_EMPLOYEE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DELIVERY_CHANNEL_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'BANK_CHARGE_BEARER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMIT_TO_SUPPLIER_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMIT_TO_SUPPLIER_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMIT_TO_ADDRESS_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_PRIORITY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SETTLEMENT_PRIORITY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'UNIQUE_REMITTANCE_IDENTIFIER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'URI_CHECK_DIGIT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_REASON_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_REASON_COMMENTS'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMITTANCE_MESSAGE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMITTANCE_MESSAGE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMITTANCE_MESSAGE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AWT_GROUP_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SHIP_TO_LOCATION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAXATION_COUNTRY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DOCUMENT_SUB_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_INVOICE_INTERNAL_SEQ'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SUPPLIER_TAX_INVOICE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_INVOICE_RECORDING_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SUPPLIER_TAX_INVOICE_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SUPPLIER_TAX_EXCHANGE_RATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PORT_OF_ENTRY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CORRECTION_YEAR'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CORRECTION_PERIOD'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'IMPORT_DOCUMENT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'IMPORT_DOCUMENT_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONTROL_AMOUNT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CALC_TAX_DURING_IMPORT_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ADD_TAX_TO_INV_AMT_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE16'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE17'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE18'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE19'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE20'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'IMAGE_DOCUMENT_URL'
                     || chr(34);

        lc_data_type := 'VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2';

		-- Changes for generating CSV file
        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_data_type, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        FOR cur_rec IN cur_invoice_hdr_data LOOP
            lc_header := chr(34)
                         || cur_rec.invoice_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.operating_unit
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.source
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.vendor_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.vendor_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.vendor_site_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_currency_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_currency_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.description
                         || chr(34)
                         || ','
                         || chr(34)
                         || gv_batch_no
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_type_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.legal_entity_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cust_registration_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cust_registration_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.first_party_registration_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.third_party_registration_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.terms_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.terms_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.goods_received_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_received_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.gl_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_method_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pay_group_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exclusive_payment_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.amount_applicable_to_discount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_line_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_apply_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_gl_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_uncludes_prepay_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exchange_rate_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exchange_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exchange_rate
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.accts_pay_code_concatenated
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.doc_category_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.voucher_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_first_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_last_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_employee_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.delivery_channel_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.bank_charge_bearer
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remit_to_supplier_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remit_to_supplier_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remit_to_address_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_priority
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.settlement_priority
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.unique_remittance_identifier
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.uri_check_digit
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_reason_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_reason_comments
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remittance_message1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remittance_message2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remittance_message3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.awt_group_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.ship_to_location
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.taxation_country
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.document_sub_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_invoice_internal_seq
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.supplier_tax_invoice_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_invoice_recording_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.supplier_tax_invoice_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.supplier_tax_exchange_rate
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.port_of_entry_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.correction_year
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.correction_period
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.import_document_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.import_document_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.control_amount_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.calc_tax_during_import_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.add_tax_to_inv_amt_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute16
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute17
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute18
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute19
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute20
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.image_document_url
                         || chr(34);

	 --Changes for generating CSV file

            aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
            IF gv_process_sts = 'N' THEN
                gv_error_msg := gv_error_msg || lv_error_msg;
                RAISE v_utlfile_issue;
            END IF;
--gv_count:=gv_count+1;

        END LOOP;
--dbms_output.put_line('Records in output file: '||gv_count);

        utl_file.fclose(f_handle); -- Change
        gv_retcode_out := 0;
        gv_errbuf_out := NULL;
    EXCEPTION
        WHEN v_dir_path_fail THEN
            fnd_file.put_line(fnd_file.log, 'v_dir_path_fail Exception of proc ap_paid_invoices_header_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_dir_path_fail';
        WHEN v_utlfile_issue THEN
            fnd_file.put_line(fnd_file.log, 'v_utlfile_issue Exception of proc ap_paid_invoices_header_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_utlfile_issue';
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while fetching the data for paid invoices with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END ap_paid_invoices_header_extract;


----------------------------------------------------------------------------------------------------------------------------------------------
-- Extracting Invoice Headers --
----------------------------------------------------------------------------------------------------------------------------------------------

    PROCEDURE ap_inv_int_header_extract IS

        CURSOR cur_invoice_hdr_data IS
         SELECT DISTINCT
    aia.invoice_id  invoice_id,
    (
        SELECT
            hou.name
          FROM
            hr_operating_units hou
         WHERE
            hou.organization_id = aia.org_id
    )                                             operating_unit,
    decode(aia.source,'INVOICE GATEWAY','INVOICE GATEWAY_API','AMAP_ISP','AMAP_ISP_API','ERS','ERS_API','VEGMGMT','VEGMGMT_API',aia.source)   source,
    aia.invoice_num                               invoice_num,
    aia.invoice_amount                            invoice_amount,
    to_char(aia.invoice_date, 'RRRR/MM/DD')       invoice_date,
    (
        SELECT
            upper(TRIM(TRIM(BOTH ' ' FROM aps.vendor_name)))
          FROM
            ap_suppliers aps
         WHERE
            aia.vendor_id = aps.vendor_id
    )    vendor_name,
    (
        SELECT
            aps.segment1
          FROM
            ap_suppliers aps
         WHERE
            aia.vendor_id = aps.vendor_id
    )      vendor_num,
    (
        SELECT
            decode(instr(upper(apss.vendor_site_code), 'CCTM'), 0, upper(apss.vendor_site_code), replace(upper(apss.vendor_site_code),
            'CCTM', '')
                                                                                                 || '-ERS')
          FROM
            ap_suppliers          aps,
            ap_supplier_sites_all apss
         WHERE
                aia.vendor_site_id = apss.vendor_site_id
               AND aia.vendor_id = aps.vendor_id
    )                                             vendor_site_code,
    aia.invoice_currency_code                     invoice_currency_code,
    aia.payment_currency_code                     payment_currency_code,
    aia.description                               description,
    to_char(sysdate, 'RRRRMMDDHHMISS')            group_id,
    CASE
        WHEN aia.invoice_type_lookup_code = 'RETAINAGE RELEASE' THEN
            'STANDARD'
        ELSE
            aia.invoice_type_lookup_code
    END                                           invoice_type_lookup_code, -- to be checked with piyush    
    CASE
        WHEN (
            SELECT DISTINCT
                1
              FROM
                ap_invoice_lines_interface ailla
             WHERE
                    aia.invoice_id = ailla.invoice_id
                   AND ailla.po_header_id IS NOT NULL
        ) = 1 THEN
            nvl((
                SELECT DISTINCT
                    scm.ship_to_org
                  FROM
                    ap_invoice_lines_interface ailla, aee_scm_po_details_tbl     scm
                 WHERE
                        aia.invoice_id = ailla.invoice_id
                       AND ailla.po_header_id IS NOT NULL
                       AND ailla.po_header_id          = scm.po_header_id
                       AND ailla.po_line_id            = scm.po_line_id
                       AND ailla.po_line_location_id   = scm.line_location_id
                       AND ailla.po_distribution_id    = scm.po_distribution_id
                       AND nvl(ailla.po_release_id, 1) = nvl(scm.po_release_id, 1)
            ), 'AMS')
        ELSE
            decode(nvl(c.legalentity, 'AMS'), '***', 'AMS', nvl(c.legalentity, 'AMS'))
    END                                           legal_entity_name,
    NULL                                          cust_registration_number,
    NULL                                          cust_registration_code,
    NULL                                          first_party_registration_num,
    NULL                                          third_party_registration_num,
    (
        SELECT
            decode(apt.name, '2% 15 NET 30', '2% 10 NET 30', '2% 10 NET 45', '2% 10 NET 30',
                   '1% 10 NET 30', '2% 10 NET 30', apt.name)
          FROM
            ap_terms apt
         WHERE
            aia.terms_id = apt.term_id
    )                                             terms_name,
    to_char(aia.terms_date, 'RRRR/MM/DD')         terms_date,
    NULL                                          goods_received_date,
    NULL                                          invoice_received_date,
    to_char(to_date('30-SEP-2022'), 'RRRR/MM/DD') gl_date,
    aia.payment_method_code   payment_method_code,
    NULL                                          pay_group_lookup_code,
    aia.exclusive_payment_flag                    exclusive_payment_flag,
    aia.amount_applicable_to_discount             amount_applicable_to_discount,
    NULL                                          prepay_num,
    NULL                                          prepay_line_num,
    NULL                                          prepay_apply_amount,
    NULL                                          prepay_gl_date,
    NULL                                          invoice_uncludes_prepay_flag,
    NULL                                          exchange_rate_type,
    NULL                                          exchange_date,
    NULL                                          exchange_rate,
    NULL                                          accts_pay_code_concatenated,
    aia.doc_category_code                         doc_category_code,
    aia.voucher_num                               voucher_num,
    (
        SELECT
            first_name
          FROM
            per_all_people_f
         WHERE
                person_id = aia.requester_id
               AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
    )                                             requester_first_name,
    (
        SELECT
            last_name
          FROM
            per_all_people_f
         WHERE
                person_id = aia.requester_id
               AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
    )                                             requester_last_name,
    (
        SELECT
            employee_number
          FROM
            per_all_people_f
         WHERE
                person_id = aia.requester_id
               AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
    )                                             requester_employee_num,
    NULL                                          delivery_channel_code,
    NULL                                          bank_charge_bearer,
    NULL                                          remit_to_supplier_name,
    NULL                                          remit_to_supplier_num,
    NULL                                          remit_to_address_name,
    NULL                                          payment_priority,
    NULL                                          settlement_priority,
    NULL                                          unique_remittance_identifier,
    NULL                                          uri_check_digit,
    NULL                                          payment_reason_code,
    NULL                                          payment_reason_comments,
    NULL                                          remittance_message1,
    NULL                                          remittance_message2,
    NULL                                          remittance_message3,
    NULL                                          awt_group_name,
    (
        SELECT DISTINCT
            location_code
          FROM
            hr_locations_all           hla,
            ap_invoice_lines_interface aila
         WHERE
                1 = 1
               AND aia.invoice_id        = aila.invoice_id
               AND hla.location_id       = aila.ship_to_location_id
               AND hla.ship_to_site_flag = 'Y'
               AND ROWNUM                = 1
    )                                             ship_to_location,
    NULL                                          taxation_country,
    NULL                                          document_sub_type,
    NULL                                          tax_invoice_internal_seq,
    NULL                                          supplier_tax_invoice_number,
    NULL                                          tax_invoice_recording_date,
    NULL                                          supplier_tax_invoice_date,
    NULL                                          supplier_tax_exchange_rate,
    NULL                                          port_of_entry_code,
    NULL                                          correction_year,
    NULL                                          correction_period,
    NULL                                          import_document_number,
    NULL                                          import_document_date,
    NULL                                          control_amount_number,
    CASE
 WHEN 1=(select 1 from ap_invoice_lines_interface ailla 
 where aia.invoice_id = ailla.invoice_id and ailla.line_type_lookup_code ='TAX') then 'N'
        WHEN 1 = (
            SELECT
                1
              FROM
                ap_invoice_lines_interface ailla
             WHERE
                    aia.invoice_id = ailla.invoice_id
                   AND ailla.po_header_id IS NOT NULL
                   and ailla.line_type_lookup_code = 'ITEM'
                   AND ROWNUM = 1
        ) THEN
            'Y'
        ELSE
            'N'
    END                                           calc_tax_during_import_flag,
    CASE
    WHEN 1=(select 1 from ap_invoice_lines_interface ailla 
 where aia.invoice_id = ailla.invoice_id and ailla.line_type_lookup_code ='TAX') then 'N'
        WHEN 1 = (
            SELECT
                1
              FROM
                ap_invoice_lines_interface ailla
             WHERE
                    aia.invoice_id = ailla.invoice_id
                   AND ailla.po_header_id IS NOT NULL
                   and ailla.line_type_lookup_code = 'ITEM'
                   AND ROWNUM = 1
        ) THEN
            'Y'
        ELSE
            'N'
    END                                           add_tax_to_inv_amt_flag,
    NULL                                          attribute_category,
    NULL                                          attribute1,
    NULL                                          attribute2,
    NULL                                          attribute3,
    NULL                                          attribute4,
    NULL                                          attribute5,
    NULL                                          attribute6,
    NULL                                          attribute7,
    NULL                                          attribute8,
    NULL                                          attribute9,
    NULL                                          attribute10,
    NULL                                          attribute11,
    NULL                                          attribute12,
    NULL                                          attribute13,
    NULL                                          attribute14,
    NULL                                          attribute15,
    NULL                                          attribute_number1,
    NULL                                          attribute_number2,
    NULL                                          attribute_number3,
    NULL                                          attribute_number4,
    NULL                                          attribute_number5,
    NULL                                          attribute_date1,
    NULL                                          attribute_date2,
    NULL                                          attribute_date3,
    NULL                                          attribute_date4,
    NULL                                          attribute_date5,
    NULL                                          global_attribute_category,
    NULL                                          global_attribute1,
    NULL                                          global_attribute2,
    NULL                                          global_attribute3,
    NULL                                          global_attribute4,
    NULL                                          global_attribute5,
    NULL                                          global_attribute6,
    NULL                                          global_attribute7,
    NULL                                          global_attribute8,
    NULL                                          global_attribute9,
    NULL                                          global_attribute10,
    NULL                                          global_attribute11,
    NULL                                          global_attribute12,
    NULL                                          global_attribute13,
    NULL                                          global_attribute14,
    NULL                                          global_attribute15,
    NULL                                          global_attribute16,
    NULL                                          global_attribute17,
    NULL                                          global_attribute18,
    NULL                                          global_attribute19,
    NULL                                          global_attribute20,
    NULL                                          global_attribute_number1,
    NULL                                          global_attribute_number2,
    NULL                                          global_attribute_number3,
    NULL                                          global_attribute_number4,
    NULL                                          global_attribute_number5,
    NULL                                          global_attribute_date1,
    NULL                                          global_attribute_date2,
    NULL                                          global_attribute_date3,
    NULL                                          global_attribute_date4,
    NULL                                          global_attribute_date5,
    NULL                                          image_document_url
  FROM
    ap_invoices_interface      aia,
    ap_invoice_lines_interface aila,
    (
        SELECT DISTINCT
            y.invoice_id,
            CASE
                WHEN cnt IS NULL THEN
                    'AMS'
                ELSE
                    y.le_name
            END legalentity
          FROM
            (
                SELECT
                    invoice_id,
                    COUNT(1) cnt
                  FROM
                    (
                        SELECT DISTINCT
                            DENSE_RANK()
                            OVER(PARTITION BY aila.invoice_id, substr(aila.dist_code_concatenated, 1, 3)
                                 ORDER BY
                                    aila.invoice_id, substr(aila.dist_code_concatenated, 1, 3) ASC
                            )                                         rnum,
                            substr(aila.dist_code_concatenated, 1, 3) le_name,
                            aila.invoice_id
                          FROM
                            ap_invoices_interface      aia,
                            ap_invoice_lines_interface aila
                         WHERE
                                aia.invoice_id = aila.invoice_id
                               AND aia.invoice_amount != 0
                               AND aia.status IS NULL
                               AND ( aia.attribute1 IS NULL
                                OR aia.attribute1 NOT IN ( 20, 21, 23, 25, 29,
                                                           31, 32, 34, 38 ) )
                               AND aia.vendor_id IS NOT NULL
                               AND aia.vendor_site_id IS NOT NULL
                               AND aia.invoice_num IS NOT NULL
                               AND aia.invoice_date IS NOT NULL
                               AND aia.invoice_type_lookup_code IS NOT NULL
                               AND aia.voucher_num IS NOT NULL
                               AND aila.line_number IS NOT NULL
                               AND aila.line_type_lookup_code IS NOT NULL
                    )
                 GROUP BY
                    invoice_id
                HAVING
                    COUNT(1) = 1
            ) x,
            (
                SELECT DISTINCT
                    DENSE_RANK()
                    OVER(PARTITION BY aila.invoice_id, substr(aila.dist_code_concatenated, 1, 3)
                         ORDER BY
                            aila.invoice_id, substr(aila.dist_code_concatenated, 1, 3) ASC
                    )                                         rnum,
                    substr(aila.dist_code_concatenated, 1, 3) le_name,
                    aila.invoice_id
                  FROM
                    ap_invoices_interface      aia,
                    ap_invoice_lines_interface aila
                 WHERE
                        aia.invoice_id = aila.invoice_id
                       AND aia.invoice_amount != 0
                       AND aia.status IS NULL
                       AND ( aia.attribute1 IS NULL
                        OR aia.attribute1 NOT IN ( 20, 21, 23, 25, 29,
                                                   31, 32, 34, 38 ) )
                       AND aia.vendor_id IS NOT NULL
                       AND aia.vendor_site_id IS NOT NULL
                       AND aia.invoice_num IS NOT NULL
                       AND aia.invoice_date IS NOT NULL
                       AND aia.invoice_type_lookup_code IS NOT NULL
                       AND aia.voucher_num IS NOT NULL
                       AND aila.line_number IS NOT NULL
                       AND aila.line_type_lookup_code IS NOT NULL
            ) y            
         WHERE
            x.invoice_id (+) = y.invoice_id
    )  c
 WHERE
        1 = 1
       AND aia.invoice_id = c.invoice_id (+)
       AND aia.invoice_id = aila.invoice_id
       AND aia.invoice_amount != 0
       AND aia.status IS NULL
       AND ( aia.attribute1 IS NULL OR aia.attribute1 NOT IN ( 20, 21, 23, 25, 29, 31, 32, 34, 38 ) )
       AND aia.vendor_id IS NOT NULL
       AND aia.vendor_site_id IS NOT NULL
       AND aia.invoice_num IS NOT NULL
       AND aia.invoice_date IS NOT NULL
       AND aia.invoice_type_lookup_code IS NOT NULL
       AND aia.voucher_num IS NOT NULL
       AND aila.line_number IS NOT NULL
       AND aila.line_type_lookup_code IS NOT NULL;--1836
		 
		 
        lc_header    VARCHAR2(32000) := NULL;
        lc_data_type VARCHAR2(32000) := NULL;

	   --Changes for generating CSV file.
        v_utlfile_issue EXCEPTION;
        v_dir_path_fail EXCEPTION;
        v_no_data_to_process EXCEPTION;
        lv_error_msg VARCHAR2(2000) := NULL;
        f_handle     utl_file.file_type;
        p_f_handle   utl_file.file_type;
    BEGIN

	    -- Changes for generating CSV file. --
        l_ext_file_name := 'ApInvoicesInterface_'
                           || 'AP_INV_INT_Header'
                           || gv_batch_no
                           || '.csv';
        aee_common_util_pkg.validate_ext_dir(l_ext_file_loc, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_dir_path_fail;
        END IF;

        aee_common_util_pkg.openfile(l_ext_file_name, l_ext_file_loc, gv_process_sts, lv_error_msg, p_f_handle);
        fnd_file.put_line(fnd_file.log, 'Flag gv_process_sts value - ' || gv_process_sts);
        f_handle := p_f_handle;
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        lc_header := chr(34)
                     || 'INVOICE_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'OPERATING_UNIT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SOURCE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VENDOR_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VENDOR_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VENDOR_SITE_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_CURRENCY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_CURRENCY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DESCRIPTION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GROUP_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_TYPE_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LEGAL_ENTITY_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CUST_REGISTRATION_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CUST_REGISTRATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FIRST_PARTY_REGISTRATION_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'THIRD_PARTY_REGISTRATION_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TERMS_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TERMS_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GOODS_RECEIVED_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_RECEIVED_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GL_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_METHOD_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAY_GROUP_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCLUSIVE_PAYMENT_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AMOUNT_APPLICABLE_TO_DISCOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_LINE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_APPLY_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_GL_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_UNCLUDES_PREPAY_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCHANGE_RATE_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCHANGE_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCHANGE_RATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ACCTS_PAY_CODE_CONCATENATED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DOC_CATEGORY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VOUCHER_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_FIRST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_LAST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_EMPLOYEE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DELIVERY_CHANNEL_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'BANK_CHARGE_BEARER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMIT_TO_SUPPLIER_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMIT_TO_SUPPLIER_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMIT_TO_ADDRESS_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_PRIORITY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SETTLEMENT_PRIORITY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'UNIQUE_REMITTANCE_IDENTIFIER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'URI_CHECK_DIGIT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_REASON_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_REASON_COMMENTS'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMITTANCE_MESSAGE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMITTANCE_MESSAGE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMITTANCE_MESSAGE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AWT_GROUP_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SHIP_TO_LOCATION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAXATION_COUNTRY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DOCUMENT_SUB_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_INVOICE_INTERNAL_SEQ'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SUPPLIER_TAX_INVOICE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_INVOICE_RECORDING_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SUPPLIER_TAX_INVOICE_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SUPPLIER_TAX_EXCHANGE_RATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PORT_OF_ENTRY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CORRECTION_YEAR'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CORRECTION_PERIOD'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'IMPORT_DOCUMENT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'IMPORT_DOCUMENT_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONTROL_AMOUNT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CALC_TAX_DURING_IMPORT_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ADD_TAX_TO_INV_AMT_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE16'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE17'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE18'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE19'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE20'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'IMAGE_DOCUMENT_URL'
                     || chr(34);

        lc_data_type := 'VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2';

		-- Changes for generating CSV file
        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_data_type, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        FOR cur_rec IN cur_invoice_hdr_data LOOP
            lc_header := chr(34)
                         || cur_rec.invoice_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.operating_unit
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.source
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.vendor_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.vendor_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.vendor_site_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_currency_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_currency_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.description
                         || chr(34)
                         || ','
                         || chr(34)
                         || gv_batch_no
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_type_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.legal_entity_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cust_registration_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cust_registration_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.first_party_registration_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.third_party_registration_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.terms_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.terms_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.goods_received_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_received_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.gl_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_method_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pay_group_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exclusive_payment_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.amount_applicable_to_discount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_line_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_apply_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_gl_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_uncludes_prepay_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exchange_rate_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exchange_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exchange_rate
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.accts_pay_code_concatenated
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.doc_category_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.voucher_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_first_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_last_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_employee_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.delivery_channel_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.bank_charge_bearer
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remit_to_supplier_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remit_to_supplier_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remit_to_address_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_priority
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.settlement_priority
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.unique_remittance_identifier
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.uri_check_digit
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_reason_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_reason_comments
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remittance_message1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remittance_message2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remittance_message3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.awt_group_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.ship_to_location
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.taxation_country
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.document_sub_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_invoice_internal_seq
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.supplier_tax_invoice_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_invoice_recording_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.supplier_tax_invoice_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.supplier_tax_exchange_rate
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.port_of_entry_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.correction_year
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.correction_period
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.import_document_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.import_document_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.control_amount_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.calc_tax_during_import_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.add_tax_to_inv_amt_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute16
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute17
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute18
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute19
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute20
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.image_document_url
                         || chr(34);

	 --Changes for generating CSV file

            aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
            IF gv_process_sts = 'N' THEN
                gv_error_msg := gv_error_msg || lv_error_msg;
                RAISE v_utlfile_issue;
            END IF;
--gv_count:=gv_count+1;

        END LOOP;
--dbms_output.put_line('Records in output file: '||gv_count);

        utl_file.fclose(f_handle); -- Change
        gv_retcode_out := 0;
        gv_errbuf_out := NULL;
    EXCEPTION
        WHEN v_dir_path_fail THEN
            fnd_file.put_line(fnd_file.log, 'v_dir_path_fail Exception of proc ap_inv_int_header_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_dir_path_fail';
        WHEN v_utlfile_issue THEN
            fnd_file.put_line(fnd_file.log, 'v_utlfile_issue Exception of proc ap_inv_int_header_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_utlfile_issue';
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while fetching the data for Interface invoices with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END ap_inv_int_header_extract;

----------------------------------------------------------------------------------------------------------------------------------------------
-- Extracting Invoice Headers --
----------------------------------------------------------------------------------------------------------------------------------------------

    PROCEDURE ap_holds_all_header_extract IS

        CURSOR cur_invoice_hdr_data IS
     SELECT
    * --1189
  FROM
    (
        SELECT DISTINCT
            aia.invoice_id                                                                                 invoice_id,
            hou.name                                                                                       operating_unit,
            aia.source                                                                                     source,
            aia.invoice_num                                                                                invoice_num,
            aia.invoice_amount                                                                             invoice_amount,
            to_char(aia.invoice_date, 'RRRR/MM/DD')                                                        invoice_date,
            upper(TRIM(TRIM(BOTH ' ' FROM aps.vendor_name)))                                               vendor_name,
            aps.segment1                                                                                   vendor_num,
            decode(instr(upper(apss.vendor_site_code), 'CCTM'), 0, upper(apss.vendor_site_code), replace(upper(apss.vendor_site_code),
            'CCTM', '')
                                                                                                 || '-ERS')                                                                                     vendor_site_code,
            aia.invoice_currency_code                                                                      invoice_currency_code,
            aia.payment_currency_code                                                                      payment_currency_code,
            substr((replace(replace(replace(replace(aia.description, '""', '"'), '"', '""'), CHR(10), NULL), CHR(13), NULL)), 1, 220)
            || (
                CASE
                    WHEN (
                        SELECT DISTINCT
                            release_num
                          FROM
                            po_releases_all      pra,
                            ap_invoice_lines_all aila
                         WHERE
                                pra.po_release_id = aila.po_release_id
                               AND aila.invoice_id = aia.invoice_id
                               AND ROWNUM          = 1
                    ) IS NULL THEN
                        (
                            SELECT DISTINCT
                                ' EBS PO#' || segment1
                              FROM
                                po_headers_all       pha,
                                ap_invoice_lines_all aila
                             WHERE
                                    pha.po_header_id = aila.po_header_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                    ELSE
                        (
                            SELECT DISTINCT
                                ' EBS PO#' || segment1
                              FROM
                                po_headers_all       pha,
                                ap_invoice_lines_all aila
                             WHERE
                                    pha.po_header_id = aila.po_header_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                        || '-'
                        || (
                            SELECT DISTINCT
                                release_num
                              FROM
                                po_releases_all      pra,
                                ap_invoice_lines_all aila
                             WHERE
                                    pra.po_release_id = aila.po_release_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                END
            )                                                                                              description,
            to_char(sysdate, 'RRRRMMDDHHMISS')                                                             group_id,
            case when aia.invoice_type_lookup_code ='RETAINAGE RELEASE' THEN ('STANDARD') ELSE aia.invoice_type_lookup_code END invoice_type_lookup_code,
            CASE
                WHEN (
                    SELECT DISTINCT
                        1
                      FROM
                        ap_invoice_lines_all aila
                     WHERE
                            aila.invoice_id = aia.invoice_id
                           AND aila.po_header_id IS NULL
                           AND aila.line_type_lookup_code = 'ITEM'
                ) = 1 THEN
                    aia.pay_group_lookup_code
                WHEN (
                    SELECT DISTINCT
                        1
                      FROM
                        po_headers_all       ph,
                        ap_invoice_lines_all aila
                     WHERE
                            aila.invoice_id = aia.invoice_id
                           AND aila.line_type_lookup_code = 'ITEM'
                           AND aila.po_header_id          = ph.po_header_id
                           AND ph.closed_code IN ( 'FINALLY CLOSED' )
                ) = 1 THEN
                    aia.pay_group_lookup_code
                ELSE
                    nvl((
                        SELECT
                            scm.ship_to_org
                          FROM
                            ap_invoice_lines_all   aila, aee_scm_po_details_tbl scm
                         WHERE
                                aia.invoice_id = aila.invoice_id
								and aila.po_header_id is not null
                               AND aila.po_header_id          = scm.po_header_id
                               AND aila.po_line_id            = scm.po_line_id
                               AND aila.po_line_location_id   = scm.line_location_id
                               AND aila.po_distribution_id    = scm.po_distribution_id
                               AND nvl(aila.po_release_id, 1) = nvl(scm.po_release_id, 1)
                               AND ROWNUM                     = 1
                    ), aia.pay_group_lookup_code)
            END                                                                                            legal_entity_name,
            NULL                                                                                           cust_registration_number,
            NULL                                                                                           cust_registration_code,
            NULL                                                                                           first_party_registration_num,
            NULL                                                                                           third_party_registration_num,
            decode(apt.name, '2% 15 NET 30', '2% 10 NET 30', '2% 10 NET 45', '2% 10 NET 30',
                   '1% 10 NET 30', '2% 10 NET 30', apt.name)                                               terms_name,
            to_char(aia.terms_date, 'RRRR/MM/DD')                                                          terms_date,
            NULL                                                                                           goods_received_date,
            NULL                                                                                           invoice_received_date,
            to_char(to_date('30-SEP-2022'), 'RRRR/MM/DD')                                                  gl_date,
            aia.payment_method_code                                                                        payment_method_code,
            NULL                                                                                           pay_group_lookup_code,
            aia.exclusive_payment_flag                                                                     exclusive_payment_flag,
            aia.amount_applicable_to_discount                                                              amount_applicable_to_discount,
            NULL                                                                                           prepay_num,
            NULL                                                                                           prepay_line_num,
            NULL                                                                                           prepay_apply_amount,
            NULL                                                                                           prepay_gl_date,
            NULL                                                                                           invoice_uncludes_prepay_flag,
            NULL                                                                                           exchange_rate_type,
            NULL                                                                                           exchange_date,
            NULL                                                                                           exchange_rate,
            NULL                                                                                           accts_pay_code_concatenated,
            aia.doc_category_code                                                                          doc_category_code,
            aia.voucher_num                                                                                voucher_num,
            (
                SELECT
                    first_name
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_first_name,
            (
                SELECT
                    last_name
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_last_name,
            (
                SELECT
                    employee_number
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_employee_num,
            NULL                                                                                           delivery_channel_code,
            NULL                                                                                           bank_charge_bearer,
            NULL                                                                                           remit_to_supplier_name,
            NULL                                                                                           remit_to_supplier_num,
            NULL                                                                                           remit_to_address_name,
            NULL                                                                                           payment_priority,
            NULL                                                                                           settlement_priority,
            NULL                                                                                           unique_remittance_identifier,
            NULL                                                                                           uri_check_digit,
            NULL                                                                                           payment_reason_code,
            NULL                                                                                           payment_reason_comments,
            NULL                                                                                           remittance_message1,
            NULL                                                                                           remittance_message2,
            NULL                                                                                           remittance_message3,
            NULL                                                                                           awt_group_name,
            (
                SELECT DISTINCT
                    location_code
                  FROM
                    hr_locations_all     hla,
                    ap_invoice_lines_all aila
                 WHERE
                        1 = 1
                       AND aia.invoice_id        = aila.invoice_id
                       AND hla.location_id       = aila.ship_to_location_id
                       AND hla.ship_to_site_flag = 'Y'
                       AND ROWNUM                = 1
            )                                                                                              ship_to_location,
            NULL                                                                                           taxation_country,
            NULL                                                                                           document_sub_type,
            NULL                                                                                           tax_invoice_internal_seq,
            NULL                                                                                           supplier_tax_invoice_number,
            NULL                                                                                           tax_invoice_recording_date,
            NULL                                                                                           supplier_tax_invoice_date,
            NULL                                                                                           supplier_tax_exchange_rate,
            NULL                                                                                           port_of_entry_code,
            NULL                                                                                           correction_year,
            NULL                                                                                           correction_period,
            NULL                                                                                           import_document_number,
            NULL                                                                                           import_document_date,
            aia.tax_amount                                                                                 control_amount_number,
            'N'                                                                                            calc_tax_during_import_flag,
            'N'                                                                                            add_tax_to_inv_amt_flag,
            aia.attribute_category                                                                         attribute_category,
            aia.attribute9                                                                                 attribute1,
            aia.attribute14                                                                                attribute2,
            NULL                                                                                           attribute3,
            NULL                                                                                           attribute4,
            NULL                                                                                           attribute5,
            NULL                                                                                           attribute6,
            NULL                                                                                           attribute7,
            NULL                                                                                           attribute8,
            NULL                                                                                           attribute9,
            NULL                                                                                           attribute10,
            NULL                                                                                           attribute11,
            NULL                                                                                           attribute12,
            NULL                                                                                           attribute13,
            NULL                                                                                           attribute14,
            NULL                                                                                           attribute15,
            NULL                                                                                           attribute_number1,
            NULL                                                                                           attribute_number2,
            NULL                                                                                           attribute_number3,
            NULL                                                                                           attribute_number4,
            NULL                                                                                           attribute_number5,
            decode(aia.attribute_category, '1099S', to_char(to_date(aia.attribute12), 'RRRR/MM/DD'), NULL) attribute_date1,
            NULL                                                                                           attribute_date2,
            NULL                                                                                           attribute_date3,
            NULL                                                                                           attribute_date4,
            NULL                                                                                           attribute_date5,
            NULL                                                                                           global_attribute_category,
            NULL                                                                                           global_attribute1,
            NULL                                                                                           global_attribute2,
            NULL                                                                                           global_attribute3,
            NULL                                                                                           global_attribute4,
            NULL                                                                                           global_attribute5,
            NULL                                                                                           global_attribute6,
            NULL                                                                                           global_attribute7,
            NULL                                                                                           global_attribute8,
            NULL                                                                                           global_attribute9,
            NULL                                                                                           global_attribute10,
            NULL                                                                                           global_attribute11,
            NULL                                                                                           global_attribute12,
            NULL                                                                                           global_attribute13,
            NULL                                                                                           global_attribute14,
            NULL                                                                                           global_attribute15,
            NULL                                                                                           global_attribute16,
            NULL                                                                                           global_attribute17,
            NULL                                                                                           global_attribute18,
            NULL                                                                                           global_attribute19,
            NULL                                                                                           global_attribute20,
            NULL                                                                                           global_attribute_number1,
            NULL                                                                                           global_attribute_number2,
            NULL                                                                                           global_attribute_number3,
            NULL                                                                                           global_attribute_number4,
            NULL                                                                                           global_attribute_number5,
            NULL                                                                                           global_attribute_date1,
            NULL                                                                                           global_attribute_date2,
            NULL                                                                                           global_attribute_date3,
            NULL                                                                                           global_attribute_date4,
            NULL                                                                                           global_attribute_date5,
            NULL                                                                                           image_document_url
         FROM
            ap_invoices_all              aia,
            ap_suppliers                 aps,
            ap_supplier_sites_all        apss,
            ap_terms                     apt,
            hr_operating_units           hou,
            ap_holds_all                 aha            
         WHERE
                1 = 1
               AND aia.invoice_id                = aha.invoice_id
               AND aha.release_lookup_code IS NULL
               AND aia.vendor_id                 = aps.vendor_id
               AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')
               AND aia.payment_status_flag       = 'N'
               AND aia.cancelled_amount IS NULL
               AND aia.vendor_site_id            = apss.vendor_site_id
               AND aps.vendor_id                 = apss.vendor_id
               AND apt.term_id                   = aia.terms_id
               AND hou.organization_id           = aia.org_id
               AND aia.invoice_amount != 0 		   
             );

        lc_header    VARCHAR2(32000) := NULL;
        lc_data_type VARCHAR2(32000) := NULL;

	   --Changes for generating CSV file.
        v_utlfile_issue EXCEPTION;
        v_dir_path_fail EXCEPTION;
        v_no_data_to_process EXCEPTION;
        lv_error_msg VARCHAR2(2000) := NULL;
        f_handle     utl_file.file_type;
        p_f_handle   utl_file.file_type;
    BEGIN

	    -- Changes for generating CSV file. --
        l_ext_file_name := 'ApInvoicesInterface_'
                           || 'AP_Holds_All_Header'
                           || gv_batch_no
                           || '.csv';
        aee_common_util_pkg.validate_ext_dir(l_ext_file_loc, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_dir_path_fail;
        END IF;

        aee_common_util_pkg.openfile(l_ext_file_name, l_ext_file_loc, gv_process_sts, lv_error_msg, p_f_handle);
        fnd_file.put_line(fnd_file.log, 'Flag gv_process_sts value - ' || gv_process_sts);
        f_handle := p_f_handle;
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        lc_header := chr(34)
                     || 'INVOICE_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'OPERATING_UNIT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SOURCE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VENDOR_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VENDOR_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VENDOR_SITE_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_CURRENCY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_CURRENCY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DESCRIPTION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GROUP_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_TYPE_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LEGAL_ENTITY_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CUST_REGISTRATION_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CUST_REGISTRATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FIRST_PARTY_REGISTRATION_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'THIRD_PARTY_REGISTRATION_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TERMS_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TERMS_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GOODS_RECEIVED_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_RECEIVED_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GL_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_METHOD_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAY_GROUP_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCLUSIVE_PAYMENT_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AMOUNT_APPLICABLE_TO_DISCOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_LINE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_APPLY_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_GL_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_UNCLUDES_PREPAY_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCHANGE_RATE_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCHANGE_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCHANGE_RATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ACCTS_PAY_CODE_CONCATENATED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DOC_CATEGORY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VOUCHER_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_FIRST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_LAST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_EMPLOYEE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DELIVERY_CHANNEL_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'BANK_CHARGE_BEARER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMIT_TO_SUPPLIER_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMIT_TO_SUPPLIER_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMIT_TO_ADDRESS_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_PRIORITY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SETTLEMENT_PRIORITY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'UNIQUE_REMITTANCE_IDENTIFIER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'URI_CHECK_DIGIT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_REASON_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_REASON_COMMENTS'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMITTANCE_MESSAGE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMITTANCE_MESSAGE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMITTANCE_MESSAGE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AWT_GROUP_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SHIP_TO_LOCATION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAXATION_COUNTRY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DOCUMENT_SUB_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_INVOICE_INTERNAL_SEQ'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SUPPLIER_TAX_INVOICE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_INVOICE_RECORDING_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SUPPLIER_TAX_INVOICE_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SUPPLIER_TAX_EXCHANGE_RATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PORT_OF_ENTRY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CORRECTION_YEAR'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CORRECTION_PERIOD'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'IMPORT_DOCUMENT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'IMPORT_DOCUMENT_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONTROL_AMOUNT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CALC_TAX_DURING_IMPORT_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ADD_TAX_TO_INV_AMT_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE16'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE17'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE18'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE19'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE20'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'IMAGE_DOCUMENT_URL'
                     || chr(34);

        lc_data_type := 'VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2';

		-- Changes for generating CSV file
        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_data_type, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        FOR cur_rec IN cur_invoice_hdr_data LOOP
            lc_header := chr(34)
                         || cur_rec.invoice_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.operating_unit
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.source
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.vendor_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.vendor_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.vendor_site_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_currency_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_currency_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.description
                         || chr(34)
                         || ','
                         || chr(34)
                         || gv_batch_no
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_type_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.legal_entity_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cust_registration_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cust_registration_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.first_party_registration_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.third_party_registration_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.terms_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.terms_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.goods_received_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_received_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.gl_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_method_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pay_group_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exclusive_payment_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.amount_applicable_to_discount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_line_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_apply_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_gl_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_uncludes_prepay_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exchange_rate_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exchange_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exchange_rate
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.accts_pay_code_concatenated
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.doc_category_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.voucher_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_first_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_last_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_employee_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.delivery_channel_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.bank_charge_bearer
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remit_to_supplier_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remit_to_supplier_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remit_to_address_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_priority
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.settlement_priority
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.unique_remittance_identifier
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.uri_check_digit
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_reason_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_reason_comments
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remittance_message1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remittance_message2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remittance_message3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.awt_group_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.ship_to_location
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.taxation_country
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.document_sub_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_invoice_internal_seq
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.supplier_tax_invoice_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_invoice_recording_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.supplier_tax_invoice_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.supplier_tax_exchange_rate
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.port_of_entry_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.correction_year
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.correction_period
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.import_document_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.import_document_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.control_amount_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.calc_tax_during_import_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.add_tax_to_inv_amt_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute16
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute17
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute18
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute19
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute20
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.image_document_url
                         || chr(34);

	 --Changes for generating CSV file

            aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
            IF gv_process_sts = 'N' THEN
                gv_error_msg := gv_error_msg || lv_error_msg;
                RAISE v_utlfile_issue;
            END IF;
--gv_count:=gv_count+1;

        END LOOP;
--dbms_output.put_line('Records in output file: '||gv_count);

        utl_file.fclose(f_handle); -- Change
        gv_retcode_out := 0;
        gv_errbuf_out := NULL;
    EXCEPTION
        WHEN v_dir_path_fail THEN
            fnd_file.put_line(fnd_file.log, 'v_dir_path_fail Exception of proc ap_paid_invoices_header_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_dir_path_fail';
        WHEN v_utlfile_issue THEN
            fnd_file.put_line(fnd_file.log, 'v_utlfile_issue Exception of proc ap_paid_invoices_header_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_utlfile_issue';
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while fetching the data for paid invoices with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END ap_holds_all_header_extract;


----------------------------------------------------------------------------------------------------------------------------------------------
-- Extracting Invoice Headers --
----------------------------------------------------------------------------------------------------------------------------------------------

    PROCEDURE ap_invoice_headers_extract (
        p_hold IN VARCHAR2
    ) IS

        CURSOR cur_invoice_hdr_data IS
         SELECT
    * --12911
  FROM
    (
        SELECT DISTINCT
            aia.invoice_id                                                                                 invoice_id,
            hou.name                                                                                       operating_unit,
            aia.source                                                                                     source,
            aia.invoice_num                                                                                invoice_num,
            aia.invoice_amount                                                                             invoice_amount,
            to_char(aia.invoice_date, 'RRRR/MM/DD')                                                        invoice_date,
            upper(TRIM(TRIM(BOTH ' ' FROM aps.vendor_name)))                                               vendor_name,
            aps.segment1                                                                                   vendor_num,
            decode(instr(upper(apss.vendor_site_code), 'CCTM'), 0, upper(apss.vendor_site_code), replace(upper(apss.vendor_site_code),
            'CCTM', '')
                                                                                                 || '-ERS')                                                                                     vendor_site_code,
            aia.invoice_currency_code                                                                      invoice_currency_code,
            aia.payment_currency_code                                                                      payment_currency_code,
            substr((replace(replace(replace(replace(aia.description, '""', '"'), '"', '""'), CHR(10), NULL), CHR(13), NULL)), 1, 220)
            || (
                CASE
                    WHEN (
                        SELECT DISTINCT
                            release_num
                          FROM
                            po_releases_all      pra,
                            ap_invoice_lines_all aila
                         WHERE
                                pra.po_release_id = aila.po_release_id
                               AND aila.invoice_id = aia.invoice_id
                               AND ROWNUM          = 1
                    ) IS NULL THEN
                        (
                            SELECT DISTINCT
                                ' EBS PO#' || segment1
                              FROM
                                po_headers_all       pha,
                                ap_invoice_lines_all aila
                             WHERE
                                    pha.po_header_id = aila.po_header_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                    ELSE
                        (
                            SELECT DISTINCT
                                ' EBS PO#' || segment1
                              FROM
                                po_headers_all       pha,
                                ap_invoice_lines_all aila
                             WHERE
                                    pha.po_header_id = aila.po_header_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                        || '-'
                        || (
                            SELECT DISTINCT
                                release_num
                              FROM
                                po_releases_all      pra,
                                ap_invoice_lines_all aila
                             WHERE
                                    pra.po_release_id = aila.po_release_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                END
            )                                                                                              description,
            to_char(sysdate, 'RRRRMMDDHHMISS')                                                             group_id,
            case when aia.invoice_type_lookup_code ='RETAINAGE RELEASE' THEN ('STANDARD') ELSE aia.invoice_type_lookup_code END invoice_type_lookup_code,
            CASE
                WHEN (
                    SELECT DISTINCT
                        1
                      FROM
                        ap_invoice_lines_all aila
                     WHERE
                            aila.invoice_id = aia.invoice_id
                           AND aila.po_header_id IS NULL
                           AND aila.line_type_lookup_code = 'ITEM'
                ) = 1 THEN
                    aia.pay_group_lookup_code
                WHEN (
                    SELECT DISTINCT
                        1
                      FROM
                        po_headers_all       ph,
                        ap_invoice_lines_all aila
                     WHERE
                            aila.invoice_id = aia.invoice_id
                           AND aila.line_type_lookup_code = 'ITEM'
                           AND aila.po_header_id          = ph.po_header_id
                           AND ph.closed_code IN ( 'FINALLY CLOSED' )
                ) = 1 THEN
                    aia.pay_group_lookup_code
                ELSE
                    nvl((
                        SELECT
                            scm.ship_to_org
                          FROM
                            ap_invoice_lines_all   aila, aee_scm_po_details_tbl scm
                         WHERE
                                aia.invoice_id = aila.invoice_id
								and aila.po_header_id is not null
                               AND aila.po_header_id          = scm.po_header_id
                               AND aila.po_line_id            = scm.po_line_id
                               AND aila.po_line_location_id   = scm.line_location_id
                               AND aila.po_distribution_id    = scm.po_distribution_id
                               AND nvl(aila.po_release_id, 1) = nvl(scm.po_release_id, 1)
                               AND ROWNUM                     = 1
                    ), aia.pay_group_lookup_code)
            END                                                                                            legal_entity_name,
            NULL                                                                                           cust_registration_number,
            NULL                                                                                           cust_registration_code,
            NULL                                                                                           first_party_registration_num,
            NULL                                                                                           third_party_registration_num,
            decode(apt.name, '2% 15 NET 30', '2% 10 NET 30', '2% 10 NET 45', '2% 10 NET 30',
                   '1% 10 NET 30', '2% 10 NET 30', apt.name)                                               terms_name,
            to_char(aia.terms_date, 'RRRR/MM/DD')                                                          terms_date,
            NULL                                                                                           goods_received_date,
            NULL                                                                                           invoice_received_date,
            to_char(to_date('30-SEP-2022'), 'RRRR/MM/DD')                                                  gl_date,
            aia.payment_method_code                                                                        payment_method_code,
            NULL                                                                                           pay_group_lookup_code,
            aia.exclusive_payment_flag                                                                     exclusive_payment_flag,
            aia.amount_applicable_to_discount                                                              amount_applicable_to_discount,
            NULL                                                                                           prepay_num,
            NULL                                                                                           prepay_line_num,
            NULL                                                                                           prepay_apply_amount,
            NULL                                                                                           prepay_gl_date,
            NULL                                                                                           invoice_uncludes_prepay_flag,
            NULL                                                                                           exchange_rate_type,
            NULL                                                                                           exchange_date,
            NULL                                                                                           exchange_rate,
            NULL                                                                                           accts_pay_code_concatenated,
            aia.doc_category_code                                                                          doc_category_code,
            aia.voucher_num                                                                                voucher_num,
            (
                SELECT
                    first_name
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_first_name,
            (
                SELECT
                    last_name
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_last_name,
            (
                SELECT
                    employee_number
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_employee_num,
            NULL                                                                                           delivery_channel_code,
            NULL                                                                                           bank_charge_bearer,
            NULL                                                                                           remit_to_supplier_name,
            NULL                                                                                           remit_to_supplier_num,
            NULL                                                                                           remit_to_address_name,
            NULL                                                                                           payment_priority,
            NULL                                                                                           settlement_priority,
            NULL                                                                                           unique_remittance_identifier,
            NULL                                                                                           uri_check_digit,
            NULL                                                                                           payment_reason_code,
            NULL                                                                                           payment_reason_comments,
            NULL                                                                                           remittance_message1,
            NULL                                                                                           remittance_message2,
            NULL                                                                                           remittance_message3,
            NULL                                                                                           awt_group_name,
            (
                SELECT DISTINCT
                    location_code
                  FROM
                    hr_locations_all     hla,
                    ap_invoice_lines_all aila
                 WHERE
                        1 = 1
                       AND aia.invoice_id        = aila.invoice_id
                       AND hla.location_id       = aila.ship_to_location_id
                       AND hla.ship_to_site_flag = 'Y'
                       AND ROWNUM                = 1
            )                                                                                              ship_to_location,
            NULL                                                                                           taxation_country,
            NULL                                                                                           document_sub_type,
            NULL                                                                                           tax_invoice_internal_seq,
            NULL                                                                                           supplier_tax_invoice_number,
            NULL                                                                                           tax_invoice_recording_date,
            NULL                                                                                           supplier_tax_invoice_date,
            NULL                                                                                           supplier_tax_exchange_rate,
            NULL                                                                                           port_of_entry_code,
            NULL                                                                                           correction_year,
            NULL                                                                                           correction_period,
            NULL                                                                                           import_document_number,
            NULL                                                                                           import_document_date,
            aia.tax_amount                                                                                 control_amount_number,
            'N'                                                                                            calc_tax_during_import_flag,
            'N'                                                                                            add_tax_to_inv_amt_flag,
            aia.attribute_category                                                                         attribute_category,
            aia.attribute9                                                                                 attribute1,
            aia.attribute14                                                                                attribute2,
            NULL                                                                                           attribute3,
            NULL                                                                                           attribute4,
            NULL                                                                                           attribute5,
            NULL                                                                                           attribute6,
            NULL                                                                                           attribute7,
            NULL                                                                                           attribute8,
            NULL                                                                                           attribute9,
            NULL                                                                                           attribute10,
            NULL                                                                                           attribute11,
            NULL                                                                                           attribute12,
            NULL                                                                                           attribute13,
            NULL                                                                                           attribute14,
            NULL                                                                                           attribute15,
            NULL                                                                                           attribute_number1,
            NULL                                                                                           attribute_number2,
            NULL                                                                                           attribute_number3,
            NULL                                                                                           attribute_number4,
            NULL                                                                                           attribute_number5,
            decode(aia.attribute_category, '1099S', to_char(to_date(aia.attribute12), 'RRRR/MM/DD'), NULL) attribute_date1,
            NULL                                                                                           attribute_date2,
            NULL                                                                                           attribute_date3,
            NULL                                                                                           attribute_date4,
            NULL                                                                                           attribute_date5,
            NULL                                                                                           global_attribute_category,
            NULL                                                                                           global_attribute1,
            NULL                                                                                           global_attribute2,
            NULL                                                                                           global_attribute3,
            NULL                                                                                           global_attribute4,
            NULL                                                                                           global_attribute5,
            NULL                                                                                           global_attribute6,
            NULL                                                                                           global_attribute7,
            NULL                                                                                           global_attribute8,
            NULL                                                                                           global_attribute9,
            NULL                                                                                           global_attribute10,
            NULL                                                                                           global_attribute11,
            NULL                                                                                           global_attribute12,
            NULL                                                                                           global_attribute13,
            NULL                                                                                           global_attribute14,
            NULL                                                                                           global_attribute15,
            NULL                                                                                           global_attribute16,
            NULL                                                                                           global_attribute17,
            NULL                                                                                           global_attribute18,
            NULL                                                                                           global_attribute19,
            NULL                                                                                           global_attribute20,
            NULL                                                                                           global_attribute_number1,
            NULL                                                                                           global_attribute_number2,
            NULL                                                                                           global_attribute_number3,
            NULL                                                                                           global_attribute_number4,
            NULL                                                                                           global_attribute_number5,
            NULL                                                                                           global_attribute_date1,
            NULL                                                                                           global_attribute_date2,
            NULL                                                                                           global_attribute_date3,
            NULL                                                                                           global_attribute_date4,
            NULL                                                                                           global_attribute_date5,
            NULL                                                                                           image_document_url
          FROM
            ap_invoices_all          aia,
            ap_suppliers             aps,
            ap_supplier_sites_all    apss,
            ap_payment_schedules_all apsa,
            ap_terms                 apt,
            hr_operating_units       hou
         WHERE
                1 = 1
               AND aia.vendor_id                                      = aps.vendor_id
               AND aia.payment_status_flag                            = 'N'
               AND aia.cancelled_amount IS NULL
               AND aia.vendor_site_id                                 = apss.vendor_site_id
               AND aps.vendor_id                                      = apss.vendor_id
               AND apsa.invoice_id                                    = aia.invoice_id
               AND apsa.hold_flag                                     = p_hold
               AND apt.term_id                                        = aia.terms_id
               AND hou.organization_id                                = aia.org_id
               AND ap_invoices_pkg.get_posting_status(aia.invoice_id) = 'Y'
               AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')
               AND aia.invoice_amount != 0		
               AND EXISTS (
                SELECT DISTINCT
                    ( 1 )
                  FROM
                    ap_invoice_distributions_all aida
                 WHERE
                        aida.match_status_flag = 'A'
                       AND aida.invoice_id = aia.invoice_id
            )
        MINUS
        SELECT DISTINCT
            aia.invoice_id                                                                                 invoice_id,
            hou.name                                                                                       operating_unit,
            aia.source                                                                                     source,
            aia.invoice_num                                                                                invoice_num,
            aia.invoice_amount                                                                             invoice_amount,
            to_char(aia.invoice_date, 'RRRR/MM/DD')                                                        invoice_date,
            upper(TRIM(TRIM(BOTH ' ' FROM aps.vendor_name)))                                               vendor_name,
            aps.segment1                                                                                   vendor_num,
            decode(instr(upper(apss.vendor_site_code), 'CCTM'), 0, upper(apss.vendor_site_code), replace(upper(apss.vendor_site_code),
            'CCTM', '')
                                                                                                 || '-ERS')                                                                                     vendor_site_code,
            aia.invoice_currency_code                                                                      invoice_currency_code,
            aia.payment_currency_code                                                                      payment_currency_code,
            substr((replace(replace(replace(replace(aia.description, '""', '"'), '"', '""'), CHR(10), NULL), CHR(13), NULL)), 1, 220)
            || (
                CASE
                    WHEN (
                        SELECT DISTINCT
                            release_num
                          FROM
                            po_releases_all      pra,
                            ap_invoice_lines_all aila
                         WHERE
                                pra.po_release_id = aila.po_release_id
                               AND aila.invoice_id = aia.invoice_id
                               AND ROWNUM          = 1
                    ) IS NULL THEN
                        (
                            SELECT DISTINCT
                                ' EBS PO#' || segment1
                              FROM
                                po_headers_all       pha,
                                ap_invoice_lines_all aila
                             WHERE
                                    pha.po_header_id = aila.po_header_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                    ELSE
                        (
                            SELECT DISTINCT
                                ' EBS PO#' || segment1
                              FROM
                                po_headers_all       pha,
                                ap_invoice_lines_all aila
                             WHERE
                                    pha.po_header_id = aila.po_header_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                        || '-'
                        || (
                            SELECT DISTINCT
                                release_num
                              FROM
                                po_releases_all      pra,
                                ap_invoice_lines_all aila
                             WHERE
                                    pra.po_release_id = aila.po_release_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                END
            )                                                                                              description,
            to_char(sysdate, 'RRRRMMDDHHMISS')                                                             group_id,
            case when aia.invoice_type_lookup_code ='RETAINAGE RELEASE' THEN ('STANDARD') ELSE aia.invoice_type_lookup_code END invoice_type_lookup_code,
            CASE
                WHEN (
                    SELECT DISTINCT
                        1
                      FROM
                        ap_invoice_lines_all aila
                     WHERE
                            aila.invoice_id = aia.invoice_id
                           AND aila.po_header_id IS NULL
                           AND aila.line_type_lookup_code = 'ITEM'
                ) = 1 THEN
                    aia.pay_group_lookup_code
                WHEN (
                    SELECT DISTINCT
                        1
                      FROM
                        po_headers_all       ph,
                        ap_invoice_lines_all aila
                     WHERE
                            aila.invoice_id = aia.invoice_id
                           AND aila.line_type_lookup_code = 'ITEM'
                           AND aila.po_header_id          = ph.po_header_id
                           AND ph.closed_code IN ( 'FINALLY CLOSED' )
                ) = 1 THEN
                    aia.pay_group_lookup_code
                ELSE
                    nvl((
                        SELECT
                            scm.ship_to_org
                          FROM
                            ap_invoice_lines_all   aila, aee_scm_po_details_tbl scm
                         WHERE
                                aia.invoice_id = aila.invoice_id
								and aila.po_header_id is not null
                               AND aila.po_header_id          = scm.po_header_id
                               AND aila.po_line_id            = scm.po_line_id
                               AND aila.po_line_location_id   = scm.line_location_id
                               AND aila.po_distribution_id    = scm.po_distribution_id
                               AND nvl(aila.po_release_id, 1) = nvl(scm.po_release_id, 1)
                               AND ROWNUM                     = 1
                    ), aia.pay_group_lookup_code)
            END                                                                                            legal_entity_name,
            NULL                                                                                           cust_registration_number,
            NULL                                                                                           cust_registration_code,
            NULL                                                                                           first_party_registration_num,
            NULL                                                                                           third_party_registration_num,
            decode(apt.name, '2% 15 NET 30', '2% 10 NET 30', '2% 10 NET 45', '2% 10 NET 30',
                   '1% 10 NET 30', '2% 10 NET 30', apt.name)                                               terms_name,
            to_char(aia.terms_date, 'RRRR/MM/DD')                                                          terms_date,
            NULL                                                                                           goods_received_date,
            NULL                                                                                           invoice_received_date,
            to_char(to_date('30-SEP-2022'), 'RRRR/MM/DD')                                                  gl_date,
            aia.payment_method_code                                                                        payment_method_code,
            NULL                                                                                           pay_group_lookup_code,
            aia.exclusive_payment_flag                                                                     exclusive_payment_flag,
            aia.amount_applicable_to_discount                                                              amount_applicable_to_discount,
            NULL                                                                                           prepay_num,
            NULL                                                                                           prepay_line_num,
            NULL                                                                                           prepay_apply_amount,
            NULL                                                                                           prepay_gl_date,
            NULL                                                                                           invoice_uncludes_prepay_flag,
            NULL                                                                                           exchange_rate_type,
            NULL                                                                                           exchange_date,
            NULL                                                                                           exchange_rate,
            NULL                                                                                           accts_pay_code_concatenated,
            aia.doc_category_code                                                                          doc_category_code,
            aia.voucher_num                                                                                voucher_num,
            (
                SELECT
                    first_name
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_first_name,
            (
                SELECT
                    last_name
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_last_name,
            (
                SELECT
                    employee_number
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_employee_num,
            NULL                                                                                           delivery_channel_code,
            NULL                                                                                           bank_charge_bearer,
            NULL                                                                                           remit_to_supplier_name,
            NULL                                                                                           remit_to_supplier_num,
            NULL                                                                                           remit_to_address_name,
            NULL                                                                                           payment_priority,
            NULL                                                                                           settlement_priority,
            NULL                                                                                           unique_remittance_identifier,
            NULL                                                                                           uri_check_digit,
            NULL                                                                                           payment_reason_code,
            NULL                                                                                           payment_reason_comments,
            NULL                                                                                           remittance_message1,
            NULL                                                                                           remittance_message2,
            NULL                                                                                           remittance_message3,
            NULL                                                                                           awt_group_name,
            (
                SELECT DISTINCT
                    location_code
                  FROM
                    hr_locations_all     hla,
                    ap_invoice_lines_all aila
                 WHERE
                        1 = 1
                       AND aia.invoice_id        = aila.invoice_id
                       AND hla.location_id       = aila.ship_to_location_id
                       AND hla.ship_to_site_flag = 'Y'
                       AND ROWNUM                = 1
            )                                                                                              ship_to_location,
            NULL                                                                                           taxation_country,
            NULL                                                                                           document_sub_type,
            NULL                                                                                           tax_invoice_internal_seq,
            NULL                                                                                           supplier_tax_invoice_number,
            NULL                                                                                           tax_invoice_recording_date,
            NULL                                                                                           supplier_tax_invoice_date,
            NULL                                                                                           supplier_tax_exchange_rate,
            NULL                                                                                           port_of_entry_code,
            NULL                                                                                           correction_year,
            NULL                                                                                           correction_period,
            NULL                                                                                           import_document_number,
            NULL                                                                                           import_document_date,
            aia.tax_amount                                                                                 control_amount_number,
            'N'                                                                                            calc_tax_during_import_flag,
            'N'                                                                                            add_tax_to_inv_amt_flag,
            aia.attribute_category                                                                         attribute_category,
            aia.attribute9                                                                                 attribute1,
            aia.attribute14                                                                                attribute2,
            NULL                                                                                           attribute3,
            NULL                                                                                           attribute4,
            NULL                                                                                           attribute5,
            NULL                                                                                           attribute6,
            NULL                                                                                           attribute7,
            NULL                                                                                           attribute8,
            NULL                                                                                           attribute9,
            NULL                                                                                           attribute10,
            NULL                                                                                           attribute11,
            NULL                                                                                           attribute12,
            NULL                                                                                           attribute13,
            NULL                                                                                           attribute14,
            NULL                                                                                           attribute15,
            NULL                                                                                           attribute_number1,
            NULL                                                                                           attribute_number2,
            NULL                                                                                           attribute_number3,
            NULL                                                                                           attribute_number4,
            NULL                                                                                           attribute_number5,
            decode(aia.attribute_category, '1099S', to_char(to_date(aia.attribute12), 'RRRR/MM/DD'), NULL) attribute_date1,
            NULL                                                                                           attribute_date2,
            NULL                                                                                           attribute_date3,
            NULL                                                                                           attribute_date4,
            NULL                                                                                           attribute_date5,
            NULL                                                                                           global_attribute_category,
            NULL                                                                                           global_attribute1,
            NULL                                                                                           global_attribute2,
            NULL                                                                                           global_attribute3,
            NULL                                                                                           global_attribute4,
            NULL                                                                                           global_attribute5,
            NULL                                                                                           global_attribute6,
            NULL                                                                                           global_attribute7,
            NULL                                                                                           global_attribute8,
            NULL                                                                                           global_attribute9,
            NULL                                                                                           global_attribute10,
            NULL                                                                                           global_attribute11,
            NULL                                                                                           global_attribute12,
            NULL                                                                                           global_attribute13,
            NULL                                                                                           global_attribute14,
            NULL                                                                                           global_attribute15,
            NULL                                                                                           global_attribute16,
            NULL                                                                                           global_attribute17,
            NULL                                                                                           global_attribute18,
            NULL                                                                                           global_attribute19,
            NULL                                                                                           global_attribute20,
            NULL                                                                                           global_attribute_number1,
            NULL                                                                                           global_attribute_number2,
            NULL                                                                                           global_attribute_number3,
            NULL                                                                                           global_attribute_number4,
            NULL                                                                                           global_attribute_number5,
            NULL                                                                                           global_attribute_date1,
            NULL                                                                                           global_attribute_date2,
            NULL                                                                                           global_attribute_date3,
            NULL                                                                                           global_attribute_date4,
            NULL                                                                                           global_attribute_date5,
            NULL                                                                                           image_document_url
          FROM
            ap_invoices_all          aia,
            ap_suppliers             aps,
            ap_supplier_sites_all    apss,
            ap_payment_schedules_all apsa,
            ap_terms                 apt,
            hr_operating_units       hou,
            ap_holds_all             aha
         WHERE
                1 = 1
               AND aia.invoice_id                                     = aha.invoice_id
               AND aha.release_lookup_code IS NULL
               AND aia.vendor_id                                      = aps.vendor_id
               AND aia.payment_status_flag                            = 'N'
               AND aia.cancelled_amount IS NULL
               AND aia.vendor_site_id                                 = apss.vendor_site_id
               AND aps.vendor_id                                      = apss.vendor_id
               AND apsa.invoice_id                                    = aia.invoice_id
               AND apsa.hold_flag                                     = p_hold
               AND apt.term_id                                        = aia.terms_id
               AND hou.organization_id                                = aia.org_id
               AND ap_invoices_pkg.get_posting_status(aia.invoice_id) = 'Y'
               AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')
               AND aia.invoice_amount != 0	
               AND EXISTS (
                SELECT DISTINCT
                    ( 1 )
                  FROM
                    ap_invoice_distributions_all aida
                 WHERE
                        aida.match_status_flag = 'A'
                       AND aida.invoice_id = aia.invoice_id
            )
    );

        lc_header    VARCHAR2(32000) := NULL;
        lc_data_type VARCHAR2(32000) := NULL;

	   --Changes for generating CSV file.
        v_utlfile_issue EXCEPTION;
        v_dir_path_fail EXCEPTION;
        v_no_data_to_process EXCEPTION;
        lv_error_msg VARCHAR2(2000) := NULL;
        f_handle     utl_file.file_type;
        p_f_handle   utl_file.file_type;
    BEGIN

	    -- Changes for generating CSV file. --
        l_ext_file_name := 'ApInvoicesInterface_'
                           ||
            CASE
                WHEN p_hold = 'Y' THEN
                    'HOLD_'
                ELSE 'NO_HOLD_'
            END
                           || gv_batch_no
                           || '.csv';

        aee_common_util_pkg.validate_ext_dir(l_ext_file_loc, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_dir_path_fail;
        END IF;

        aee_common_util_pkg.openfile(l_ext_file_name, l_ext_file_loc, gv_process_sts, lv_error_msg, p_f_handle);
        fnd_file.put_line(fnd_file.log, 'Flag gv_process_sts value - ' || gv_process_sts);
        f_handle := p_f_handle;
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        lc_header := chr(34)
                     || 'INVOICE_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'OPERATING_UNIT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SOURCE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VENDOR_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VENDOR_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VENDOR_SITE_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_CURRENCY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_CURRENCY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DESCRIPTION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GROUP_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_TYPE_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LEGAL_ENTITY_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CUST_REGISTRATION_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CUST_REGISTRATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FIRST_PARTY_REGISTRATION_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'THIRD_PARTY_REGISTRATION_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TERMS_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TERMS_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GOODS_RECEIVED_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_RECEIVED_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GL_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_METHOD_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAY_GROUP_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCLUSIVE_PAYMENT_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AMOUNT_APPLICABLE_TO_DISCOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_LINE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_APPLY_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PREPAY_GL_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INVOICE_UNCLUDES_PREPAY_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCHANGE_RATE_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCHANGE_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'EXCHANGE_RATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ACCTS_PAY_CODE_CONCATENATED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DOC_CATEGORY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'VOUCHER_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_FIRST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_LAST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_EMPLOYEE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DELIVERY_CHANNEL_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'BANK_CHARGE_BEARER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMIT_TO_SUPPLIER_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMIT_TO_SUPPLIER_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMIT_TO_ADDRESS_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_PRIORITY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SETTLEMENT_PRIORITY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'UNIQUE_REMITTANCE_IDENTIFIER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'URI_CHECK_DIGIT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_REASON_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PAYMENT_REASON_COMMENTS'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMITTANCE_MESSAGE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMITTANCE_MESSAGE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REMITTANCE_MESSAGE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AWT_GROUP_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SHIP_TO_LOCATION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAXATION_COUNTRY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DOCUMENT_SUB_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_INVOICE_INTERNAL_SEQ'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SUPPLIER_TAX_INVOICE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_INVOICE_RECORDING_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SUPPLIER_TAX_INVOICE_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SUPPLIER_TAX_EXCHANGE_RATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PORT_OF_ENTRY_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CORRECTION_YEAR'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CORRECTION_PERIOD'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'IMPORT_DOCUMENT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'IMPORT_DOCUMENT_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONTROL_AMOUNT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CALC_TAX_DURING_IMPORT_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ADD_TAX_TO_INV_AMT_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE16'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE17'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE18'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE19'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE20'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'IMAGE_DOCUMENT_URL'
                     || chr(34);

        lc_data_type := 'VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2';

		-- Changes for generating CSV file
        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_data_type, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        FOR cur_rec IN cur_invoice_hdr_data LOOP
            lc_header := chr(34)
                         || cur_rec.invoice_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.operating_unit
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.source
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.vendor_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.vendor_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.vendor_site_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_currency_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_currency_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.description
                         || chr(34)
                         || ','
                         || chr(34)
                         || gv_batch_no
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_type_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.legal_entity_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cust_registration_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cust_registration_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.first_party_registration_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.third_party_registration_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.terms_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.terms_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.goods_received_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_received_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.gl_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_method_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pay_group_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exclusive_payment_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.amount_applicable_to_discount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_line_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_apply_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prepay_gl_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.invoice_uncludes_prepay_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exchange_rate_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exchange_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.exchange_rate
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.accts_pay_code_concatenated
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.doc_category_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.voucher_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_first_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_last_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_employee_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.delivery_channel_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.bank_charge_bearer
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remit_to_supplier_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remit_to_supplier_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remit_to_address_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_priority
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.settlement_priority
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.unique_remittance_identifier
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.uri_check_digit
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_reason_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.payment_reason_comments
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remittance_message1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remittance_message2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.remittance_message3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.awt_group_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.ship_to_location
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.taxation_country
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.document_sub_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_invoice_internal_seq
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.supplier_tax_invoice_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_invoice_recording_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.supplier_tax_invoice_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.supplier_tax_exchange_rate
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.port_of_entry_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.correction_year
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.correction_period
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.import_document_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.import_document_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.control_amount_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.calc_tax_during_import_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.add_tax_to_inv_amt_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute16
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute17
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute18
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute19
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute20
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.image_document_url
                         || chr(34);

	 --Changes for generating CSV file

            aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
            IF gv_process_sts = 'N' THEN
                gv_error_msg := gv_error_msg || lv_error_msg;
                RAISE v_utlfile_issue;
            END IF;
--gv_count:=gv_count+1;

        END LOOP;
--dbms_output.put_line('Records in output file: '||gv_count);

        utl_file.fclose(f_handle); -- Change
        gv_retcode_out := 0;
        gv_errbuf_out := NULL;
    EXCEPTION
        WHEN v_dir_path_fail THEN
            fnd_file.put_line(fnd_file.log, 'v_dir_path_fail Exception of proc ap_invoice_headers_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_dir_path_fail';
        WHEN v_utlfile_issue THEN
            fnd_file.put_line(fnd_file.log, 'v_utlfile_issue Exception of proc ap_invoice_headers_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_utlfile_issue';
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while fetching the data with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END ap_invoice_headers_extract;

----------------------------------------------------------------------------------------------------------------------------------------------
-- Extracting Invoice Lines --
----------------------------------------------------------------------------------------------------------------------------------------------

    PROCEDURE ap_invoice_lines_extract (
        p_hold             IN VARCHAR2,
        p_business_extract IN VARCHAR2
    ) IS

        CURSOR cur_invoice_line_data IS
       SELECT
    *
  FROM
    (
        SELECT DISTINCT
            aila.invoice_id                                                                                          invoice_id,
         CASE
                WHEN aia.invoice_id = (
                    SELECT DISTINCT
                        invoice_id
                      FROM
                        (
                            SELECT DISTINCT
                                ad.invoice_id,
                                ad.invoice_line_number,
                                COUNT(ad.invoice_id)
                              FROM
                                ap_invoice_distributions_all ad,ap_invoice_lines_all al
                             WHERE al.invoice_id= ad.invoice_id and al.line_number = ad.invoice_line_number and 
                                ad.invoice_id = aia.invoice_id
                                AND CASE
                       WHEN al.line_type_lookup_code = 'TAX' --40032
                          AND ad.amount = 0 THEN
                           0
                       ELSE
                           1
                   END = 1

                             GROUP BY
                                ad.invoice_id,
                                ad.invoice_line_number
                            HAVING
                                COUNT(ad.invoice_id) > 1
                        )
                ) THEN
                    aida.invoice_line_number || aida.distribution_line_number
                ELSE
                    to_char(aila.line_number)
            END                                                                                                      line_number,
            decode(aila.line_type_lookup_code,'RETAINAGE RELEASE','ITEM',aila.line_type_lookup_code)                                                                               line_type_lookup_code,
            aida.amount                                                                                              amount,
            aida.quantity_invoiced                                                                                   quantity_invoiced,
            aida.unit_price                                                                                          unit_price,
            NULL                                                                                                     unit_of_meas_lookup_code,
            substr(replace(replace(replace(replace(aila.description, '""', '"'), '"', '""'), CHR(10), NULL), CHR(13), NULL),1,240) description,
            /*pha.segment1*/
			CASE
                WHEN (
                    SELECT
                        release_num
                    FROM
                        po_releases_all pra
                    WHERE
                        pra.po_release_id = aila.po_release_id
                ) IS NULL THEN
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                ELSE
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                    || '-'
                    || (
                        SELECT
                            release_num
                        FROM
                            po_releases_all pra
                        WHERE
                            pra.po_release_id = aila.po_release_id
                    )
            END po_number,
            pla.line_num                                                                                             po_line_number,
            plla.shipment_num                                                                                        po_shipment_num,
            pda.distribution_num                                                                                     po_distribution_num,
            NULL                                                                                                     item_description,
            NULL                                                                                                     release_num,
            NULL                                                                                                     purchasing_category,
            NULL                                                                                                     receipt_number,
            NULL                                                                                                     receipt_line_number,
            NULL                                                                                                     consumption_advice_number,
            NULL                                                                                                     consumption_advice_line_number,
            NULL                                                                                                     packing_slip,
            NULL                                                                                                     final_match_flag,
            decode(glcc.segment1,'ADC', 'ADC.NU.184061.0000.170.00000.000.00.0000', 'AIC', 'AIC.CU.184061.0000.ZA0.00000.000.00.0000',
           'AMC', 'AMC.CU.184061.0000.100.00000.000.00.0000', 'AMS', 'AMS.CU.184061.0000.110.00000.000.00.0000', 'ATX',
           'ATX.T1.184061.0000.TX0.00000.000.00.0000', 'ITC', 'ITC.T1.184061.0000.TN0.00000.000.00.0000', 'UEC', 'UEC.CU.184061.0000.200.00000.000.00.0000',
           'MV1', 'MV1.CE.184061.0000.V10.00000.000.00.0000', NULL)                                         dist_code_concatenated,
            NULL                                                                                                     distribution_set_name,
            TO_CHAR(TO_DATE(/*'30-APR-2023'*/'30-SEP-2022'), 'YYYY/MM/DD')                                                            accounting_date,  -- to be checked with adnan
            '184061'                                                                                                 account_segment,
            glcc.segment1                                                                                            balancing_segment,
            '0000'                                                                                                   cost_center_segment,
            aila.tax_classification_code                                                                             tax_classification_code,
            hla.location_code                                                                                        ship_to_location_code,
            NULL                                                                                                     ship_from_location_code,
            NULL                                                                                                     final_discharge_location_code,
            NULL                                                                                                     trx_business_category,
            NULL                                                                                                     product_fisc_classification,
            NULL                                                                                                     primary_intended_use,
            NULL                                                                                                     user_defined_fisc_class,
            NULL                                                                                                     product_type,
            NULL                                                                                                     assessable_value,
            NULL                                                                                                     product_category,
            aia.control_amount                                                                                       control_amount,
            aila.tax_regime_code                                                                                     tax_regime_code,
            aila.tax                                                                                                 tax,
            aila.tax_status_code                                                                                     tax_status_code,
            aila.tax_jurisdiction_code                                                                               tax_jurisdiction_code,
            aila.tax_rate_code                                                                                       tax_rate_code,
            aila.tax_rate                                                                                            tax_rate,
            NULL                                                                                                     awt_group_name,
            Case 
			When aila.type_1099 ='1099S' 
			THEN NULL 
			ELSE aila.type_1099 
			END                                                                                                      type_1099,
            aila.income_tax_region                                                                                   income_tax_region,
            'Y'                                                                                                      prorate_across_flag,
            ROW_NUMBER()
            OVER(PARTITION BY aia.invoice_id, aila.line_type_lookup_code
                 ORDER BY
                    aia.invoice_id, aila.line_number, aida.distribution_line_number ASC
            )                                                                                                        line_group_number,
            NULL                                                                                                     cost_factor_name,
            NULL                                                                                                     stat_amount,
            NULL                                                                                                     assets_tracking_flag,
            NULL                                                                                                     asset_book_type_code,
            NULL                                                                                                     asset_category_id,
            NULL                                                                                                     serial_number,
            NULL                                                                                                     manufacturer,
            NULL                                                                                                     model_number,
            NULL                                                                                                     warranty_number,
            NULL                                                                                                     price_correction_flag,
            NULL                                                                                                     price_correct_inv_num,
            NULL                                                                                                     price_correct_inv_line_num,
            NULL                                                                                                     requester_first_name,
            NULL                                                                                                     requester_last_name,
            NULL                                                                                                     requester_employee_num,
            'CONVERSION'                                                                                             attribute_category,
            ( aga.oracle_company
              || '.'
              || aga.oracle_product
              || '.'
              || aga.oracle_account
              || '.'
              || aga.oracle_cost_center
              || '.'
              || aga.oracle_location
              || '.'
              || aga.oracle_compliance_code
              || '.'
              || aga.oracle_intercompany
              || '.'
              || aga.oracle_resource_type
              || '.'
              || aga.oracle_future ) attribute1,
            case
            when ( aga.project is null or aga.project = '*****' or aga.proj_out is null or aga.task_out is null or aga.exp_type_out is null or aga.exp_org_out is null )
            then
                null
            else    
                ( aga.proj_out
              || '|'
              || aga.task_out
              || '|'
              || aga.exp_org_out
              || '|'
              || aga.exp_type_out ) end attribute2,
            (
                glcc.segment1
              || '.'
              || glcc.segment2
              || '.'
              || glcc.segment3
              || '.'
              || glcc.segment4
              || '.'
              || glcc.segment5
              || '.'
              || glcc.segment6
              || '.'
              || glcc.segment7
              || '.'
              || glcc.segment8
              || '.'
              || glcc.segment9
              || '.'
              || glcc.segment10
              || '.'
              || glcc.segment11
              || '.'
              || glcc.segment12
              || '.'
              || glcc.segment13
                )                                                                                                     attribute3,
            NULL                                                                                                     attribute4,
            NULL                                                                                                     attribute5,
            NULL                                                                                                     attribute6,
            NULL                                                                                                     attribute7,
            NULL                                                                                                     attribute8,
            NULL                                                                                                     attribute9,
            NULL                                                                                                     attribute10,
            NULL                                                                                                     attribute11,
            NULL                                                                                                     attribute12,
            NULL                                                                                                     attribute13,
            NULL                                                                                                     attribute14,
            NULL                                                                                                     attribute15,
            NULL                                                                                                     attribute_number1,
            NULL                                                                                                     attribute_number2,
            NULL                                                                                                     attribute_number3,
            NULL                                                                                                     attribute_number4,
            NULL                                                                                                     attribute_number5,
            NULL                                                                                                     attribute_date1,
            NULL                                                                                                     attribute_date2,
            NULL                                                                                                     attribute_date3,
            NULL                                                                                                     attribute_date4,
            NULL                                                                                                     attribute_date5,
            NULL                                                                                                     global_attribute_catgory,
            decode(p_business_Extract, 'Y',(
                SELECT
                    closed_code
                FROM
                    po_headers_all pha
                WHERE
                    pha.po_header_id = aila.po_header_id
            ), NULL)                                                                                                 global_attribute1,
            decode(p_business_Extract, 'Y',(
                SELECT
                    closed_code
                FROM
                    po_releases_all pra
                WHERE
                    pra.po_release_id = aila.po_release_id
            ), NULL)                                                                                                 global_attribute2,
            decode(p_business_Extract, 'Y',(
                SELECT
                    closed_code
                FROM
                    po_lines_all pla
                WHERE
                    pla.po_line_id = aila.po_line_id
            ), NULL)                                                                                                 global_attribute3,
            decode(p_business_Extract, 'Y',(
                SELECT
                    closed_code
                FROM
                    po_line_locations_all plla
                WHERE
                    plla.line_location_id = aila.po_line_location_id
            ), NULL)                                                                                                 global_attribute4,
            NULL                                                                                                     global_attribute5,
            NULL                                                                                                     global_attribute6,
            NULL                                                                                                     global_attribute7,
            NULL                                                                                                     global_attribute8,
            NULL                                                                                                     global_attribute9,
            NULL                                                                                                     global_attribute10,
            NULL                                                                                                     global_attribute11,
            NULL                                                                                                     global_attribute12,
            NULL                                                                                                     global_attribute13,
            NULL                                                                                                     global_attribute14,
            NULL                                                                                                     global_attribute15,
            NULL                                                                                                     global_attribute16,
            NULL                                                                                                     global_attribute17,
            NULL                                                                                                     global_attribute18,
            NULL                                                                                                     global_attribute19,
            NULL                                                                                                     global_attribute20,
            NULL                                                                                                     global_attribute_number1,
            NULL                                                                                                     global_attribute_number2,
            NULL                                                                                                     global_attribute_number3,
            NULL                                                                                                     global_attribute_number4,
            NULL                                                                                                     global_attribute_number5,
            NULL                                                                                                     global_attribute_date1,
            NULL                                                                                                     global_attribute_date2,
            NULL                                                                                                     global_attribute_date3,
            NULL                                                                                                     global_attribute_date4,
            NULL                                                                                                     global_attribute_date5,
            NULL                                                                                                     pjc_project_id,
            NULL                                                                                                     pjc_task_id,
            NULL                                                                                                     pjc_expenditure_type_id,
            NULL                                                                                                     pjc_expenditure_item_date,
            NULL                                                                                                     pjc_organization_id,
            NULL                                                                                                     pjc_project_number,
            NULL                                                                                                     pjc_task_number,
            NULL                                                                                                     pjc_expenditure_type_name,
            NULL                                                                                                     pjc_organization_name,
            NULL                                                                                                     pjc_reserved_attribute1,
            NULL                                                                                                     pjc_reserved_attribute2,
            NULL                                                                                                     pjc_reserved_attribute3,
            NULL                                                                                                     pjc_reserved_attribute4,
            NULL                                                                                                     pjc_reserved_attribute5,
            NULL                                                                                                     pjc_reserved_attribute6,
            NULL                                                                                                     pjc_reserved_attribute7,
            NULL                                                                                                     pjc_reserved_attribute8,
            NULL                                                                                                     pjc_reserved_attribute9,
            NULL                                                                                                     pjc_reserved_attribute10,
            NULL                                                                                                     pjc_user_def_attribute1,
            NULL                                                                                                     pjc_user_def_attribute2,
            NULL                                                                                                     pjc_user_def_attribute3,
            NULL                                                                                                     pjc_user_def_attribute4,
            NULL                                                                                                     pjc_user_def_attribute5,
            NULL                                                                                                     pjc_user_def_attribute6,
            NULL                                                                                                     pjc_user_def_attribute7,
            NULL                                                                                                     pjc_user_def_attribute8,
            NULL                                                                                                     pjc_user_def_attribute9,
            NULL                                                                                                     pjc_user_def_attribute10,
            NULL                                                                                                     fiscal_charge_type,
            NULL                                                                                                     def_acctg_start_date,
            NULL                                                                                                     def_acctg_end_date,
            NULL                                                                                                     def_accural_code_concatenated,
            NULL                                                                                                     pjc_project_name,
            NULL                                                                                                     jc_task_name
          FROM
            ap_invoices_all              aia,
            ap_invoice_lines_all         aila,
            ap_suppliers                 aps,
            ap_supplier_sites_all        apss,
            ap_payment_schedules_all     apsa,
            ap_invoice_distributions_all aida,
            gl_code_combinations         glcc,
            po_headers_all               pha,
            po_lines_all                 pla,
            po_line_locations_all        plla,
            po_distributions_all         pda,
            po_releases_all              pra,
            hr_locations_all             hla
			, aee_gl_aavm_batch_txn_tbl aga
         WHERE
                1 = 1
               AND aia.invoice_id                                                                     = aila.invoice_id
               AND aia.vendor_id                                                                      = aps.vendor_id
               AND aia.vendor_site_id                                                                 = apss.vendor_site_id
               AND apsa.invoice_id                                                                    = aia.invoice_id--17272611
               AND aia.payment_status_flag                                                            = 'N'
               AND aia.cancelled_amount IS NULL
               AND aia.invoice_amount != 0
               AND aida.invoice_id                                                                    = aia.invoice_id
               AND aida.invoice_line_number                                                           = aila.line_number
               AND aida.dist_code_combination_id                                                      = glcc.code_combination_id
               AND aila.po_header_id                                                                  = pha.po_header_id (+)
               AND aila.po_line_id                                                                    = pla.po_line_id (+)
               AND aila.po_line_location_id                                                           = plla.line_location_id (+)
               AND aila.po_distribution_id                                                            = pda.po_distribution_id (+)
               AND aila.po_release_id                                                                 = pra.po_release_id (+)
               AND aila.ship_to_location_id                                                           = hla.location_id (+) --43279
			   AND apsa.hold_flag = p_hold
			   AND ap_invoices_pkg.get_posting_status(aia.invoice_id) = 'Y'			    
               AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')			    
               AND glcc.segment1                                      = aga.corp
               AND glcc.segment2                                      = aga.util
               AND glcc.segment3                                      = aga.business_division
               AND glcc.segment4                                      = aga.major_minor
               AND glcc.segment5                                      = aga.fmc
               AND glcc.segment6                                      = aga.rmc
               AND glcc.segment7                                      = aga.tran_type
               AND glcc.segment8                                       = aga.project
               AND glcc.segment9                                      = aga.product
               AND glcc.segment10                                     = aga.activity
               AND glcc.segment11                                     = aga.resource_type
               AND aga.batch_no = gv_batch
               AND CASE
                       WHEN aila.line_type_lookup_code = 'TAX'
                          AND aida.amount                                                                        = 0 THEN
                           0
                       ELSE
                           1
                   END = 1
               AND EXISTS (
                SELECT DISTINCT
                    ( 1 )
                  FROM
                    ap_invoice_distributions_all aida
                 WHERE
                        aida.match_status_flag = 'A'      
                       AND aida.invoice_id = aia.invoice_id
            )
        MINUS
        SELECT DISTINCT
            aila.invoice_id                                                                                          invoice_id,
         CASE
                WHEN aia.invoice_id = (
                    SELECT DISTINCT
                        invoice_id
                      FROM
                        (
                            SELECT DISTINCT
                                ad.invoice_id,
                                ad.invoice_line_number,
                                COUNT(ad.invoice_id)
                              FROM
                                ap_invoice_distributions_all ad,ap_invoice_lines_all al
                             WHERE al.invoice_id= ad.invoice_id and al.line_number = ad.invoice_line_number and 
                                ad.invoice_id = aia.invoice_id
                                AND CASE
                       WHEN al.line_type_lookup_code = 'TAX' --40032
                          AND ad.amount = 0 THEN
                           0
                       ELSE
                           1
                   END = 1

                             GROUP BY
                                ad.invoice_id,
                                ad.invoice_line_number
                            HAVING
                                COUNT(ad.invoice_id) > 1
                        )
                ) THEN
                    aida.invoice_line_number || aida.distribution_line_number
                ELSE
                    to_char(aila.line_number)
            END                                                                                                      line_number,
            decode(aila.line_type_lookup_code,'RETAINAGE RELEASE','ITEM',aila.line_type_lookup_code)                                                                               line_type_lookup_code,
            aida.amount                                                                                              amount,
            aila.quantity_invoiced                                                                                   quantity_invoiced,
            aila.unit_price                                                                                          unit_price,
            NULL                                                                                                     unit_of_meas_lookup_code,
            substr(replace(replace(replace(replace(aila.description, '""', '"'), '"', '""'), CHR(10), NULL), CHR(13), NULL),1,240) description,
            /*pha.segment1*/
			CASE
                WHEN (
                    SELECT
                        release_num
                    FROM
                        po_releases_all pra
                    WHERE
                        pra.po_release_id = aila.po_release_id
                ) IS NULL THEN
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                ELSE
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                    || '-'
                    || (
                        SELECT
                            release_num
                        FROM
                            po_releases_all pra
                        WHERE
                            pra.po_release_id = aila.po_release_id
                    )
            END po_number,
            pla.line_num                                                                                             po_line_number,
            plla.shipment_num                                                                                        po_shipment_num,
            pda.distribution_num                                                                                     po_distribution_num,
            NULL                                                                                                     item_description,
            NULL                                                                                                     release_num,
            NULL                                                                                                     purchasing_category,
            NULL                                                                                                     receipt_number,
            NULL                                                                                                     receipt_line_number,
            NULL                                                                                                     consumption_advice_number,
            NULL                                                                                                     consumption_advice_line_number,
            NULL                                                                                                     packing_slip,
            NULL                                                                                                     final_match_flag,
            decode(glcc.segment1,'ADC', 'ADC.NU.184061.0000.170.00000.000.00.0000', 'AIC', 'AIC.CU.184061.0000.ZA0.00000.000.00.0000',
           'AMC', 'AMC.CU.184061.0000.100.00000.000.00.0000', 'AMS', 'AMS.CU.184061.0000.110.00000.000.00.0000', 'ATX',
           'ATX.T1.184061.0000.TX0.00000.000.00.0000', 'ITC', 'ITC.T1.184061.0000.TN0.00000.000.00.0000', 'UEC', 'UEC.CU.184061.0000.200.00000.000.00.0000',
           'MV1', 'MV1.CE.184061.0000.V10.00000.000.00.0000', NULL)                                         dist_code_concatenated,
            NULL                                                                                                     distribution_set_name,
            TO_CHAR(TO_DATE(/*'30-APR-2023'*/'30-SEP-2022'), 'YYYY/MM/DD')                                                            accounting_date,  -- to be checked with adnan
            '184061'                                                                                                 account_segment,
            glcc.segment1                                                                                            balancing_segment,
            '0000'                                                                                                   cost_center_segment,
            aila.tax_classification_code                                                                             tax_classification_code,
            hla.location_code                                                                                        ship_to_location_code,
            NULL                                                                                                     ship_from_location_code,
            NULL                                                                                                     final_discharge_location_code,
            NULL                                                                                                     trx_business_category,
            NULL                                                                                                     product_fisc_classification,
            NULL                                                                                                     primary_intended_use,
            NULL                                                                                                     user_defined_fisc_class,
            NULL                                                                                                     product_type,
            NULL                                                                                                     assessable_value,
            NULL                                                                                                     product_category,
            aia.control_amount                                                                                       control_amount,
            aila.tax_regime_code                                                                                     tax_regime_code,
            aila.tax                                                                                                 tax,
            aila.tax_status_code                                                                                     tax_status_code,
            aila.tax_jurisdiction_code                                                                               tax_jurisdiction_code,
            aila.tax_rate_code                                                                                       tax_rate_code,
            aila.tax_rate                                                                                            tax_rate,
            NULL                                                                                                     awt_group_name,
            Case 
			When aila.type_1099 ='1099S' 
			THEN NULL 
			ELSE aila.type_1099 
			END                                                                                                      type_1099,
            aila.income_tax_region                                                                                   income_tax_region,
            'Y'                                                                                                      prorate_across_flag,
            ROW_NUMBER()
            OVER(PARTITION BY aia.invoice_id, aila.line_type_lookup_code
                 ORDER BY
                    aia.invoice_id, aila.line_number, aida.distribution_line_number ASC
            )                                                                                                        line_group_number,
            NULL                                                                                                     cost_factor_name,
            NULL                                                                                                     stat_amount,
            NULL                                                                                                     assets_tracking_flag,
            NULL                                                                                                     asset_book_type_code,
            NULL                                                                                                     asset_category_id,
            NULL                                                                                                     serial_number,
            NULL                                                                                                     manufacturer,
            NULL                                                                                                     model_number,
            NULL                                                                                                     warranty_number,
            NULL                                                                                                     price_correction_flag,
            NULL                                                                                                     price_correct_inv_num,
            NULL                                                                                                     price_correct_inv_line_num,
            NULL                                                                                                     requester_first_name,
            NULL                                                                                                     requester_last_name,
            NULL                                                                                                     requester_employee_num,
            'CONVERSION'                                                                                             attribute_category,
            ( aga.oracle_company
              || '.'
              || aga.oracle_product
              || '.'
              || aga.oracle_account
              || '.'
              || aga.oracle_cost_center
              || '.'
              || aga.oracle_location
              || '.'
              || aga.oracle_compliance_code
              || '.'
              || aga.oracle_intercompany
              || '.'
              || aga.oracle_resource_type
              || '.'
              || aga.oracle_future ) attribute1,
            case
            when ( aga.project is null or aga.project = '*****' or aga.proj_out is null or aga.task_out is null or aga.exp_type_out is null or aga.exp_org_out is null )
            then
                null
            else    
                ( aga.proj_out
              || '|'
              || aga.task_out
              || '|'
              || aga.exp_org_out
              || '|'
              || aga.exp_type_out ) end attribute2,
            (
                glcc.segment1
              || '.'
              || glcc.segment2
              || '.'
              || glcc.segment3
              || '.'
              || glcc.segment4
              || '.'
              || glcc.segment5
              || '.'
              || glcc.segment6
              || '.'
              || glcc.segment7
              || '.'
              || glcc.segment8
              || '.'
              || glcc.segment9
              || '.'
              || glcc.segment10
              || '.'
              || glcc.segment11
              || '.'
              || glcc.segment12
              || '.'
              || glcc.segment13
                )                                                                                                     attribute3,
            NULL                                                                                                     attribute4,
            NULL                                                                                                     attribute5,
            NULL                                                                                                     attribute6,
            NULL                                                                                                     attribute7,
            NULL                                                                                                     attribute8,
            NULL                                                                                                     attribute9,
            NULL                                                                                                     attribute10,
            NULL                                                                                                     attribute11,
            NULL                                                                                                     attribute12,
            NULL                                                                                                     attribute13,
            NULL                                                                                                     attribute14,
            NULL                                                                                                     attribute15,
            NULL                                                                                                     attribute_number1,
            NULL                                                                                                     attribute_number2,
            NULL                                                                                                     attribute_number3,
            NULL                                                                                                     attribute_number4,
            NULL                                                                                                     attribute_number5,
            NULL                                                                                                     attribute_date1,
            NULL                                                                                                     attribute_date2,
            NULL                                                                                                     attribute_date3,
            NULL                                                                                                     attribute_date4,
            NULL                                                                                                     attribute_date5,
            NULL                                                                                                     global_attribute_catgory,
            decode(p_business_Extract, 'Y',(
                SELECT
                    closed_code
                FROM
                    po_headers_all pha
                WHERE
                    pha.po_header_id = aila.po_header_id
            ), NULL)                                                                                                 global_attribute1,
            decode(p_business_Extract, 'Y',(
                SELECT
                    closed_code
                FROM
                    po_releases_all pra
                WHERE
                    pra.po_release_id = aila.po_release_id
            ), NULL)                                                                                                 global_attribute2,
            decode(p_business_Extract, 'Y',(
                SELECT
                    closed_code
                FROM
                    po_lines_all pla
                WHERE
                    pla.po_line_id = aila.po_line_id
            ), NULL)                                                                                                 global_attribute3,
            decode(p_business_Extract, 'Y',(
                SELECT
                    closed_code
                FROM
                    po_line_locations_all plla
                WHERE
                    plla.line_location_id = aila.po_line_location_id
            ), NULL)                                                                                                 global_attribute4,
            NULL                                                                                                     global_attribute5,
            NULL                                                                                                     global_attribute6,
            NULL                                                                                                     global_attribute7,
            NULL                                                                                                     global_attribute8,
            NULL                                                                                                     global_attribute9,
            NULL                                                                                                     global_attribute10,
            NULL                                                                                                     global_attribute11,
            NULL                                                                                                     global_attribute12,
            NULL                                                                                                     global_attribute13,
            NULL                                                                                                     global_attribute14,
            NULL                                                                                                     global_attribute15,
            NULL                                                                                                     global_attribute16,
            NULL                                                                                                     global_attribute17,
            NULL                                                                                                     global_attribute18,
            NULL                                                                                                     global_attribute19,
            NULL                                                                                                     global_attribute20,
            NULL                                                                                                     global_attribute_number1,
            NULL                                                                                                     global_attribute_number2,
            NULL                                                                                                     global_attribute_number3,
            NULL                                                                                                     global_attribute_number4,
            NULL                                                                                                     global_attribute_number5,
            NULL                                                                                                     global_attribute_date1,
            NULL                                                                                                     global_attribute_date2,
            NULL                                                                                                     global_attribute_date3,
            NULL                                                                                                     global_attribute_date4,
            NULL                                                                                                     global_attribute_date5,
            NULL                                                                                                     pjc_project_id,
            NULL                                                                                                     pjc_task_id,
            NULL                                                                                                     pjc_expenditure_type_id,
            NULL                                                                                                     pjc_expenditure_item_date,
            NULL                                                                                                     pjc_organization_id,
            NULL                                                                                                     pjc_project_number,
            NULL                                                                                                     pjc_task_number,
            NULL                                                                                                     pjc_expenditure_type_name,
            NULL                                                                                                     pjc_organization_name,
            NULL                                                                                                     pjc_reserved_attribute1,
            NULL                                                                                                     pjc_reserved_attribute2,
            NULL                                                                                                     pjc_reserved_attribute3,
            NULL                                                                                                     pjc_reserved_attribute4,
            NULL                                                                                                     pjc_reserved_attribute5,
            NULL                                                                                                     pjc_reserved_attribute6,
            NULL                                                                                                     pjc_reserved_attribute7,
            NULL                                                                                                     pjc_reserved_attribute8,
            NULL                                                                                                     pjc_reserved_attribute9,
            NULL                                                                                                     pjc_reserved_attribute10,
            NULL                                                                                                     pjc_user_def_attribute1,
            NULL                                                                                                     pjc_user_def_attribute2,
            NULL                                                                                                     pjc_user_def_attribute3,
            NULL                                                                                                     pjc_user_def_attribute4,
            NULL                                                                                                     pjc_user_def_attribute5,
            NULL                                                                                                     pjc_user_def_attribute6,
            NULL                                                                                                     pjc_user_def_attribute7,
            NULL                                                                                                     pjc_user_def_attribute8,
            NULL                                                                                                     pjc_user_def_attribute9,
            NULL                                                                                                     pjc_user_def_attribute10,
            NULL                                                                                                     fiscal_charge_type,
            NULL                                                                                                     def_acctg_start_date,
            NULL                                                                                                     def_acctg_end_date,
            NULL                                                                                                     def_accural_code_concatenated,
            NULL                                                                                                     pjc_project_name,
            NULL                                                                                                     jc_task_name
          FROM
            ap_invoices_all              aia,
            ap_invoice_lines_all         aila,
            ap_suppliers                 aps,
            ap_supplier_sites_all        apss,
            ap_payment_schedules_all     apsa,
            ap_holds_all                 aha,
            ap_invoice_distributions_all aida,
            gl_code_combinations         glcc,
            po_headers_all               pha,
            po_lines_all                 pla,
            po_line_locations_all        plla,
            po_distributions_all         pda,
            po_releases_all              pra,
            hr_locations_all             hla
			,aee_gl_aavm_batch_txn_tbl aga
         WHERE
                1 = 1
               AND aha.invoice_id                                                                     = aia.invoice_id
               AND aha.release_lookup_code IS NULL
               AND aia.invoice_id                                                                     = aila.invoice_id
               AND aia.vendor_id                                                                      = aps.vendor_id
               AND aia.vendor_site_id                                                                 = apss.vendor_site_id
               AND apsa.invoice_id                                                                    = aia.invoice_id--17272611
               AND aia.payment_status_flag                                                            = 'N'
               AND aia.cancelled_amount IS NULL
               AND aia.invoice_amount != 0
               AND aida.invoice_id                                                                    = aia.invoice_id
               AND aida.invoice_line_number                                                           = aila.line_number
               AND aida.dist_code_combination_id                                                      = glcc.code_combination_id
               AND aila.po_header_id                                                                  = pha.po_header_id (+)
               AND aila.po_line_id                                                                    = pla.po_line_id (+)
               AND aila.po_line_location_id                                                           = plla.line_location_id (+)
               AND aila.po_distribution_id                                                            = pda.po_distribution_id (+)
               AND aila.po_release_id                                                                 = pra.po_release_id (+)
               AND aila.ship_to_location_id                                                           = hla.location_id (+)
			   AND apsa.hold_flag = p_hold
			   AND ap_invoices_pkg.get_posting_status(aia.invoice_id) = 'Y'
			   AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')			   
               AND glcc.segment1                                      = aga.corp
               AND glcc.segment2                                      = aga.util
               AND glcc.segment3                                      = aga.business_division
               AND glcc.segment4                                      = aga.major_minor
               AND glcc.segment5                                      = aga.fmc
               AND glcc.segment6                                      = aga.rmc
               AND glcc.segment7                                      = aga.tran_type
               AND glcc.segment8                                      = aga.project
               AND glcc.segment9                                      = aga.product
               AND glcc.segment10                                     = aga.activity
               AND glcc.segment11                                     = aga.resource_type
               AND aga.batch_no = gv_batch
               AND CASE
                       WHEN aila.line_type_lookup_code = 'TAX' 
                          AND aida.amount                                                                        = 0 THEN
                           0
                       ELSE
                           1
                   END = 1
               AND EXISTS (
                SELECT DISTINCT
                    ( 1 )
                  FROM
                    ap_invoice_distributions_all aida
                 WHERE
                        aida.match_status_flag = 'A'      
                       AND aida.invoice_id = aia.invoice_id
            )
    );

        lc_header    VARCHAR2(32000) := NULL;
        lc_data_type VARCHAR2(32000) := NULL;

	   --Changes for generating CSV file.
        v_utlfile_issue EXCEPTION;
        v_dir_path_fail EXCEPTION;
        v_no_data_to_process EXCEPTION;
        lv_error_msg VARCHAR2(2000) := NULL;
        f_handle     utl_file.file_type;
        p_f_handle   utl_file.file_type;
    BEGIN

	    -- Changes for generating CSV file. --
        l_ext_file_name := 'ApInvoiceLinesInterface_'
                           ||
            CASE
                WHEN p_hold = 'Y' THEN
                    'HOLD_'
                ELSE 'NO_HOLD_'
            END
                           || gv_batch_no
                           || '.csv';

        aee_common_util_pkg.validate_ext_dir(l_ext_file_loc, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_dir_path_fail;
        END IF;

        aee_common_util_pkg.openfile(l_ext_file_name, l_ext_file_loc, gv_process_sts, lv_error_msg, p_f_handle);
        fnd_file.put_line(fnd_file.log, 'Flag gv_process_sts value - ' || gv_process_sts);
        f_handle := p_f_handle;
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        lc_header := chr(34)
                     || 'INVOICE_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LINE_TYPE_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'QUANTITY_INVOICED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'UNIT_PRICE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'UNIT_OF_MEAS_LOOKUP_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DESCRIPTION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_SHIPMENT_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PO_DISTRIBUTION_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ITEM_DESCRIPTION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'RELEASE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PURCHASING_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'RECEIPT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'RECEIPT_LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONSUMPTION_ADVICE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONSUMPTION_ADVICE_LINE_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PACKING_SLIP'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FINAL_MATCH_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DIST_CODE_CONCATENATED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DISTRIBUTION_SET_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ACCOUNTING_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ACCOUNT_SEGMENT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'BALANCING_SEGMENT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'COST_CENTER_SEGMENT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_CLASSIFICATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SHIP_TO_LOCATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SHIP_FROM_LOCATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FINAL_DISCHARGE_LOCATION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TRX_BUSINESS_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRODUCT_FISC_CLASSIFICATION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRIMARY_INTENDED_USE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'USER_DEFINED_FISC_CLASS'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRODUCT_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSESSABLE_VALUE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRODUCT_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'CONTROL_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_REGIME_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_STATUS_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_JURISDICTION_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_RATE_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TAX_RATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'AWT_GROUP_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'TYPE_1099'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'INCOME_TAX_REGION'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRORATE_ACROSS_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'LINE_GROUP_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'COST_FACTOR_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'STAT_AMOUNT'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSETS_TRACKING_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSET_BOOK_TYPE_CODE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ASSET_CATEGORY_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'SERIAL_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'MANUFACTURER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'MODEL_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'WARRANTY_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRICE_CORRECTION_FLAG'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRICE_CORRECT_INV_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PRICE_CORRECT_INV_LINE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_FIRST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_LAST_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'REQUESTER_EMPLOYEE_NUM'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_CATEGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_CATGORY'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE11'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE12'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE13'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE14'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE15'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE16'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE17'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE18'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE19'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE20'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_NUMBER5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'GLOBAL_ATTRIBUTE_DATE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_PROJECT_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_TASK_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_EXPENDITURE_TYPE_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_EXPENDITURE_ITEM_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_ORGANIZATION_ID'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_PROJECT_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_TASK_NUMBER'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_EXPENDITURE_TYPE_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_ORGANIZATION_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_RESERVED_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE1'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE2'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE3'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE4'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE5'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE6'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE7'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE8'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE9'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_USER_DEF_ATTRIBUTE10'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'FISCAL_CHARGE_TYPE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DEF_ACCTG_START_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DEF_ACCTG_END_DATE'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'DEF_ACCURAL_CODE_CONCATENATED'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'PJC_PROJECT_NAME'
                     || chr(34)
                     || ','
                     || chr(34)
                     || 'JC_TASK_NAME'
                     || chr(34);

        lc_data_type := 'VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2|VARCHAR2';

		-- Changes for generating CSV file
        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
--        aee_common_util_pkg.writeline(f_handle, lc_data_type, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        FOR cur_rec IN cur_invoice_line_data LOOP
            lc_header := chr(34)
                         || cur_rec.invoice_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.line_type_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.quantity_invoiced
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.unit_price
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.unit_of_meas_lookup_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.description
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_shipment_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.po_distribution_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.item_description
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.release_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.purchasing_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.receipt_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.receipt_line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.consumption_advice_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.consumption_advice_line_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.packing_slip
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.final_match_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.dist_code_concatenated
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.distribution_set_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.accounting_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.account_segment
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.balancing_segment
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cost_center_segment
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_classification_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.ship_to_location_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.ship_from_location_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.final_discharge_location_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.trx_business_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.product_fisc_classification
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.primary_intended_use
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.user_defined_fisc_class
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.product_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.assessable_value
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.product_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.control_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_regime_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_status_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_jurisdiction_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_rate_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.tax_rate
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.awt_group_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.type_1099
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.income_tax_region
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.prorate_across_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.line_group_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.cost_factor_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.stat_amount
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.assets_tracking_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.asset_book_type_code
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.asset_category_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.serial_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.manufacturer
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.model_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.warranty_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.price_correction_flag
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.price_correct_inv_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.price_correct_inv_line_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_first_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_last_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.requester_employee_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_category
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_catgory
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute11
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute12
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute13
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute14
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute15
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute16
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute17
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute18
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute19
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute20
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_number5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.global_attribute_date5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_project_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_task_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_expenditure_type_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_expenditure_item_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_organization_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_project_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_task_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_expenditure_type_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_organization_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_reserved_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute1
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute2
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute3
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute4
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute5
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute6
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute7
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute8
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute9
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_user_def_attribute10
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.fiscal_charge_type
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.def_acctg_start_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.def_acctg_end_date
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.def_accural_code_concatenated
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.pjc_project_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || cur_rec.jc_task_name
                         || chr(34);

	 --Changes for generating CSV file

            aee_common_util_pkg.writeline(f_handle, lc_header, gv_process_sts, lv_error_msg);
            IF gv_process_sts = 'N' THEN
                gv_error_msg := gv_error_msg || lv_error_msg;
                RAISE v_utlfile_issue;
            END IF;
--gv_count:=gv_count+1;

        END LOOP;
--dbms_output.put_line('Records in output file: '||gv_count);

        utl_file.fclose(f_handle); -- Change
        gv_retcode_out := 0;
        gv_errbuf_out := NULL;
    EXCEPTION
        WHEN v_dir_path_fail THEN
            fnd_file.put_line(fnd_file.log, 'v_dir_path_fail Exception of proc ap_invoice_lines_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_dir_path_fail';
        WHEN v_utlfile_issue THEN
            fnd_file.put_line(fnd_file.log, 'v_utlfile_issue Exception of proc ap_invoice_lines_extract, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_utlfile_issue';
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while fetching the data with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END ap_invoice_lines_extract;

----------------------------------------------------------------------------------------------------------------------------------------------
-- Extracting Unique EBS AP Code Combination Data --
----------------------------------------------------------------------------------------------------------------------------------------------

    PROCEDURE ap_unique_code_combination_extract IS

        CURSOR cur_ap_cc_data IS
          SELECT 
            corp,
            util,
            business_division,
            major_minor,
            fmc,
            rmc,
            tran_type,
            project,
            product,
            activity,
            resource_type--,
            --max(input_date) input_date
          FROM
            (
              (  SELECT DISTINCT
                    glcc.segment1          corp,
                    glcc.segment2          util,
                    glcc.segment3          business_division,
                    glcc.segment4          major_minor,
                    glcc.segment5          fmc,
                    glcc.segment6          rmc,
                    glcc.segment7          tran_type,
                    glcc.segment8          project,
                    glcc.segment9          product,
                    glcc.segment10         activity,
                    glcc.segment11         resource_type--,
                   -- aia.creation_date input_date
                 FROM
            ap_invoices_all              aia,
            ap_invoice_lines_all         aila,
            ap_suppliers                 aps,
            ap_supplier_sites_all        apss,
            ap_payment_schedules_all     apsa,
            ap_invoice_distributions_all aida,
            gl_code_combinations         glcc,
            po_headers_all               pha,
            po_lines_all                 pla,
            po_line_locations_all        plla,
            po_distributions_all         pda,
            po_releases_all              pra,
            hr_locations_all             hla
         WHERE
                1 = 1
               AND aia.invoice_id                                                                     = aila.invoice_id
               AND aia.vendor_id                                                                      = aps.vendor_id
               AND aia.vendor_site_id                                                                 = apss.vendor_site_id
               AND apsa.invoice_id                                                                    = aia.invoice_id
               AND aia.payment_status_flag                                                            = 'N'
               AND aia.cancelled_amount IS NULL
               AND aia.invoice_amount != 0
               AND aida.invoice_id                                                                    = aia.invoice_id
               AND aida.invoice_line_number                                                           = aila.line_number
               AND aida.dist_code_combination_id                                                      = glcc.code_combination_id
               AND aila.po_header_id                                                                  = pha.po_header_id (+)
               AND aila.po_line_id                                                                    = pla.po_line_id (+)
               AND aila.po_line_location_id                                                           = plla.line_location_id (+)
               AND aila.po_distribution_id                                                            = pda.po_distribution_id (+)
               AND aila.po_release_id                                                                 = pra.po_release_id (+)
               AND aila.ship_to_location_id                                                           = hla.location_id (+) 		  
			   AND ap_invoices_pkg.get_posting_status(aia.invoice_id) = 'Y'
               AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')
               AND CASE
                       WHEN aila.line_type_lookup_code = 'TAX'
                          AND aida.amount                                                                        = 0 THEN
                           0
                       ELSE
                           1
                   END = 1
               AND EXISTS (
                SELECT DISTINCT
                    ( 1 )
                  FROM
                    ap_invoice_distributions_all aida
                 WHERE
                        aida.match_status_flag = 'A'      
                       AND aida.invoice_id = aia.invoice_id
            )
            
            MINUS 
            SELECT DISTINCT
                    glcc.segment1          corp,
                    glcc.segment2          util,
                    glcc.segment3          business_division,
                    glcc.segment4          major_minor,
                    glcc.segment5          fmc,
                    glcc.segment6          rmc,
                    glcc.segment7          tran_type,
                    glcc.segment8          project,
                    glcc.segment9          product,
                    glcc.segment10         activity,
                    glcc.segment11         resource_type--,
                    --aia.creation_date input_date
                     FROM
            ap_invoices_all              aia,
            ap_invoice_lines_all         aila,
            ap_suppliers                 aps,
            ap_supplier_sites_all        apss,
            ap_payment_schedules_all     apsa,
            ap_holds_all                 aha,
            ap_invoice_distributions_all aida,
            gl_code_combinations         glcc,
            po_headers_all               pha,
            po_lines_all                 pla,
            po_line_locations_all        plla,
            po_distributions_all         pda,
            po_releases_all              pra,
            hr_locations_all             hla
			,aee_gl_aavm_batch_txn_tbl aga
         WHERE
                1 = 1
               AND aha.invoice_id                                                                     = aia.invoice_id
               AND aha.release_lookup_code IS NULL
               AND aia.invoice_id                                                                     = aila.invoice_id
               AND aia.vendor_id                                                                      = aps.vendor_id
               AND aia.vendor_site_id                                                                 = apss.vendor_site_id
               AND apsa.invoice_id                                                                    = aia.invoice_id
               AND aia.payment_status_flag                                                            = 'N'
               AND aia.cancelled_amount IS NULL
               AND aia.invoice_amount != 0
               AND aida.invoice_id                                                                    = aia.invoice_id
               AND aida.invoice_line_number                                                           = aila.line_number
               AND aida.dist_code_combination_id                                                      = glcc.code_combination_id
               AND aila.po_header_id                                                                  = pha.po_header_id (+)
               AND aila.po_line_id                                                                    = pla.po_line_id (+)
               AND aila.po_line_location_id                                                           = plla.line_location_id (+)
               AND aila.po_distribution_id                                                            = pda.po_distribution_id (+)
               AND aila.po_release_id                                                                 = pra.po_release_id (+)
               AND aila.ship_to_location_id                                                           = hla.location_id (+)			   
			   AND ap_invoices_pkg.get_posting_status(aia.invoice_id) = 'Y'
			   AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')              
               AND CASE
                       WHEN aila.line_type_lookup_code = 'TAX' 
                          AND aida.amount                                                                        = 0 THEN
                           0
                       ELSE
                           1
                   END = 1
               AND EXISTS (
                SELECT DISTINCT
                    ( 1 )
                  FROM
                    ap_invoice_distributions_all aida
                 WHERE
                        aida.match_status_flag = 'A'      
                       AND aida.invoice_id = aia.invoice_id
            )
    )
    UNION
    SELECT DISTINCT
                    glcc.segment1          corp,
                    glcc.segment2          util,
                    glcc.segment3          business_division,
                    glcc.segment4          major_minor,
                    glcc.segment5          fmc,
                    glcc.segment6          rmc,
                    glcc.segment7          tran_type,
                    glcc.segment8          project,
                    glcc.segment9          product,
                    glcc.segment10         activity,
                    glcc.segment11         resource_type--,
                   -- aia.creation_date input_date
                FROM
            ap_invoices_all              aia,
            ap_invoice_lines_all         aila,
            ap_suppliers                 aps,
            ap_supplier_sites_all        apss,
            ap_holds_all                 aha,
            ap_invoice_distributions_all aida,
            gl_code_combinations         glcc,
            po_headers_all               pha,
            po_lines_all                 pla,
            po_line_locations_all        plla,
            po_distributions_all         pda,
            po_releases_all              pra,
            hr_locations_all             hla			
         WHERE
                1 = 1
               AND aha.invoice_id                                                                     = aia.invoice_id
               AND aha.release_lookup_code IS NULL
               AND aia.invoice_id                                                                     = aila.invoice_id
               AND aia.vendor_id                                                                      = aps.vendor_id
               AND aia.vendor_site_id                                                                 = apss.vendor_site_id
               AND aia.payment_status_flag                                                            = 'N'
               AND aia.cancelled_amount IS NULL
               AND aia.invoice_amount != 0
               AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')
               AND aida.invoice_id                                                                    = aia.invoice_id
               AND aida.invoice_line_number                                                           = aila.line_number
               AND aida.dist_code_combination_id                                                      = glcc.code_combination_id
               AND aila.po_header_id                                                                  = pha.po_header_id (+)
               AND aila.po_line_id                                                                    = pla.po_line_id (+)
               AND aila.po_line_location_id                                                           = plla.line_location_id (+)
               AND aila.po_distribution_id                                                            = pda.po_distribution_id (+)
               AND aila.po_release_id                                                                 = pra.po_release_id (+)
               AND aila.ship_to_location_id                                                           = hla.location_id (+)			  
               AND CASE
                       WHEN aila.line_type_lookup_code = 'TAX' 
                          AND aida.amount                                                                        = 0 THEN
                           0
                       ELSE
                           1
                   END = 1

                      
                UNION
                
                SELECT DISTINCT
                    glcc.segment1          corp,
                    glcc.segment2          util,
                    glcc.segment3          business_division,
                    glcc.segment4          major_minor,
                    glcc.segment5          fmc,
                    glcc.segment6          rmc,
                    glcc.segment7          tran_type,
                    glcc.segment8          project,
                    glcc.segment9          product,
                    glcc.segment10         activity,
                    glcc.segment11         resource_type--,
                    --aia.creation_date input_date
                  FROM
    ap_invoices_interface      aia,
    ap_invoice_lines_interface aila,
    gl_code_combinations         glcc
 WHERE
        1 = 1         
       AND aia.invoice_id = aila.invoice_id
       AND aia.invoice_amount != 0
       AND aia.status IS NULL
       AND ( aia.attribute1 IS NULL
        OR aia.attribute1 NOT IN ( 20, 21, 23, 25, 29,31, 32, 34, 38 ) )
       AND aia.vendor_id IS NOT NULL
       AND aia.vendor_site_id IS NOT NULL
       AND aia.invoice_num IS NOT NULL
       AND aia.invoice_date IS NOT NULL
       AND aia.invoice_type_lookup_code IS NOT NULL
       AND aia.voucher_num IS NOT NULL
       AND aila.line_number IS NOT NULL
       AND aila.line_type_lookup_code IS NOT NULL 
       AND aila.dist_code_combination_id is not null
      AND aila.dist_code_combination_id = glcc.code_combination_id   
      
            );
--            group by
--            corp,
--            util,
--            business_division,
--            major_minor,
--            fmc,
--            rmc,
--            tran_type,
--            project,
--            product,
--            activity,
--            resource_type;


    BEGIN 


        DELETE apps.aee_gl_aavm_batch_txn_tbl
        WHERE
            batch_no LIKE 'AP_%';

        COMMIT;
     /*   DELETE aee_gl_aavm_batch_txn_tbl@AEE_OTTC_ATP_DBCONN.US.ORACLE.COM 
        WHERE
            batch_no LIKE 'AP_%';

        COMMIT;*/

        FOR cur_rec IN cur_ap_cc_data LOOP
            INSERT INTO apps.aee_gl_aavm_batch_txn_tbl (
                corp,
                util,
                business_division,
                major_minor,
                fmc,
                rmc,
                tran_type,
                project,
                product,
                activity,
                resource_type,
               -- input_date,
                creation_date,
                batch_no,
                row_seq_no,
                batch_action,
                system
            ) VALUES (
                cur_rec.corp,
                cur_rec.util,
                cur_rec.business_division,
                cur_rec.major_minor,
                cur_rec.fmc,
                cur_rec.rmc,
                cur_rec.tran_type,
                cur_rec.project,
                cur_rec.product,
                cur_rec.activity,
                cur_rec.resource_type,
                --cur_rec.input_date,
                sysdate,
                'AP_' || gv_batch_no,
                NULL/*gv_count*/,
                'Y',
                'APCNV'
            );


        END LOOP;


        COMMIT;
        INSERT INTO aee_gl_aavm_batch_txn_tbl@AEE_OTTC_ATP_DBCONN.US.ORACLE.COM  (
            corp,
            util,
            business_division,
            major_minor,
            fmc,
            rmc,
            tran_type,
            project,
            product,
            activity,
            resource_type,
           -- input_date,
            creation_date,
            batch_no,
            batch_action,
            system
        )
            ( SELECT
                corp,
                util,
                business_division,
                major_minor,
                fmc,
                rmc,
                tran_type,
                project,
                product,
                activity,
                resource_type,
                --input_date,
                creation_date,
                batch_no,
                batch_action,
                'APCNV'
            FROM
                aee_gl_aavm_batch_txn_tbl
            WHERE
                batch_no like 'AP_%'
            );

        COMMIT;
        gv_retcode_out := 0;
        gv_errbuf_out := NULL;
    EXCEPTION
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while fetching the data with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END ap_unique_code_combination_extract;

----------------------------------------------------------------------------------------------------------------------------------------------
-- Pushing ATP Cloud Segments Data into EBS DB--
----------------------------------------------------------------------------------------------------------------------------------------------

    PROCEDURE unique_code_atp_to_ebs_db IS


    BEGIN 

        DELETE apps.aee_gl_aavm_batch_txn_tbl
        WHERE
            batch_no  = gv_batch;

        COMMIT;


       insert into apps.AEE_GL_AAVM_BATCH_TXN_TBL(system,corp,util,business_division,major_minor,fmc,rmc,tran_type,project,product,activity,
                                                   resource_type,return_code,return_msg,
                                                   --input_date,
                                                   batch_no,
                                                   oracle_company,oracle_product,oracle_account,oracle_cost_center,oracle_location,
                                                   oracle_compliance_code,oracle_intercompany,oracle_resource_type,oracle_future,         
                                                   oracle_project,proj_out,task_out,exp_org_out,exp_type_out,process_flag,          
                                                   creation_date,last_update_date,created_by,last_updated_by)
         (select system,corp,util,business_division,major_minor,fmc,rmc,tran_type,nvl(project,'*****'),product,activity,resource_type,return_code,
                 return_msg,
                 --input_date,
                 batch_no,oracle_company,oracle_product,oracle_account,oracle_cost_center,oracle_location,
                 oracle_compliance_code,oracle_intercompany,oracle_resource_type,oracle_future,oracle_project,proj_out,task_out,
                 exp_org_out,exp_type_out,process_flag,creation_date,last_update_date,created_by,last_updated_by 
            from AEE_GL_AAVM_BATCH_TXN_TBL@AEE_OTTC_ATP_DBCONN.US.ORACLE.COM 
           where batch_no = gv_batch);
		   
        COMMIT;
		
		custom_table_insert();
		
        gv_retcode_out := 0;
        gv_errbuf_out := NULL;
    EXCEPTION
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while fetching the data with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END unique_code_atp_to_ebs_db;
	

----------------------------------------------------------------------------------------------------------------------------------------------
-- Inserting records into custom table--
----------------------------------------------------------------------------------------------------------------------------------------------

    PROCEDURE custom_table_insert IS
 
 ln_line_count    BINARY_INTEGER := 0;
 ln_error_count   NUMBER := 0;
      ex_dml_errors EXCEPTION;
      PRAGMA exception_init ( ex_dml_errors, -24381 );

         
	CURSOR cur_invoice is 
	(
	SELECT DISTINCT
            aila.invoice_id                                                                                          invoice_id,
            aia.invoice_num,         
            aila.line_number invoice_line_number,
            aida.distribution_line_number,            
            case 
            when aia.invoice_type_lookup_code = 'RETAINAGE RELEASE' 
            THEN 
                'STANDARD' 
            ELSE 
                aia.invoice_type_lookup_code 
            END invoice_type_lookup_code,
            decode(aila.line_type_lookup_code,'RETAINAGE RELEASE','ITEM',aila.line_type_lookup_code)   line_type_lookup_code,
            aia.invoice_date,
            aia.source,
            aia.payment_status_flag,
            aia.invoice_amount,
            aia.cancelled_amount,
            aida.amount                                                                                              amount,
            aida.quantity_invoiced                                                                                   quantity_invoiced,
            aida.unit_price                                                                                          unit_price,
            substr((replace(replace(replace(replace(aia.description, '""', '"'), '"', '""'), CHR(10), NULL), CHR(13), NULL)), 1, 220)
            || (
                CASE
                    WHEN (
                        SELECT DISTINCT
                            release_num
                          FROM
                            po_releases_all      pra,
                            ap_invoice_lines_all aila
                         WHERE
                                pra.po_release_id = aila.po_release_id
                               AND aila.invoice_id = aia.invoice_id
                               AND ROWNUM          = 1
                    ) IS NULL 
                THEN
                        (
                            SELECT DISTINCT
                                ' EBS PO#' || segment1
                              FROM
                                po_headers_all       pha,
                                ap_invoice_lines_all aila
                             WHERE
                                    pha.po_header_id = aila.po_header_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                    ELSE
                        (
                            SELECT DISTINCT
                                ' EBS PO#' || segment1
                              FROM
                                po_headers_all       pha,
                                ap_invoice_lines_all aila
                             WHERE
                                    pha.po_header_id = aila.po_header_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                        || '-'
                        || (
                            SELECT DISTINCT
                                release_num
                              FROM
                                po_releases_all      pra,
                                ap_invoice_lines_all aila
                             WHERE
                                    pra.po_release_id = aila.po_release_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                END
            )    header_description,          
            substr(replace(replace(replace(replace(aila.description, '""', '"'), '"', '""'), CHR(10), NULL), CHR(13), NULL),1,240) line_description,
            (
                SELECT DISTINCT
                    location_code
                  FROM
                    hr_locations_all     hla,
                    ap_invoice_lines_all aila
                 WHERE
                        1 = 1
                       AND aia.invoice_id        = aila.invoice_id
                       AND hla.location_id       = aila.ship_to_location_id
                       AND hla.ship_to_site_flag = 'Y'
                       AND ROWNUM                = 1
            )   ship_to_location_code,
            aps.segment1   vendor_num,
            upper(TRIM(TRIM(BOTH ' ' FROM aps.vendor_name)))  vendor_name,            
            decode(instr(upper(apss.vendor_site_code), 'CCTM'), 0, upper(apss.vendor_site_code), replace(upper(apss.vendor_site_code), 'CCTM', '')  || '-ERS')   vendor_site_code,
            aia.pay_group_lookup_code,
           CASE
                WHEN (
                    SELECT DISTINCT
                        1
                      FROM
                        ap_invoice_lines_all aila
                     WHERE
                            aila.invoice_id = aia.invoice_id
                           AND aila.po_header_id IS NULL
                           AND aila.line_type_lookup_code = 'ITEM'
                ) = 1 
                THEN
                    aia.pay_group_lookup_code
                WHEN (
                    SELECT DISTINCT
                        1
                      FROM
                        po_headers_all       ph,
                        ap_invoice_lines_all aila
                     WHERE
                            aila.invoice_id = aia.invoice_id
                           AND aila.line_type_lookup_code = 'ITEM'
                           AND aila.po_header_id          = ph.po_header_id
                           AND ph.closed_code IN ( 'FINALLY CLOSED' )
                ) = 1 
                THEN
                    aia.pay_group_lookup_code
                ELSE
                    nvl((
                        SELECT
                            scm.ship_to_org
                          FROM
                            ap_invoice_lines_all   aila, aee_scm_po_details_tbl scm
                         WHERE
                                aia.invoice_id = aila.invoice_id
								and aila.po_header_id is not null
                               AND aila.po_header_id          = scm.po_header_id
                               AND aila.po_line_id            = scm.po_line_id
                               AND aila.po_line_location_id   = scm.line_location_id
                               AND aila.po_distribution_id    = scm.po_distribution_id
                               AND nvl(aila.po_release_id, 1) = nvl(scm.po_release_id, 1)
                               AND ROWNUM                     = 1
                    ), aia.pay_group_lookup_code)
            END   legal_entity_name,  
            (select decode(apt.name, '2% 15 NET 30', '2% 10 NET 30', '2% 10 NET 45', '2% 10 NET 30','1% 10 NET 30', '2% 10 NET 30', apt.name) 
               from apps.ap_terms apt
              where apt.term_id = aia.terms_id ) terms_name,
            aia.payment_method_code,
            aia.voucher_num,
            (
                SELECT
                    first_name
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_first_name,
            (
                SELECT
                    last_name
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_last_name,
            (
                SELECT
                    employee_number
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_employee_num,
			CASE
                WHEN (
                    SELECT
                        release_num
                    FROM
                        po_releases_all pra
                    WHERE
                        pra.po_release_id = aila.po_release_id
                ) IS NULL THEN
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                ELSE
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                    || '-'
                    || (
                        SELECT
                            release_num
                        FROM
                            po_releases_all pra
                        WHERE
                            pra.po_release_id = aila.po_release_id
                    )
            END po_number,
            pla.line_num                                                                                             po_line_number,
            plla.shipment_num                                                                                        po_shipment_num,
            pda.distribution_num                                                                                     po_distribution_num,
            aida.match_Status_flag,
            aga.project legacy_project,
             (
                glcc.segment1
              || '.'
              || glcc.segment2
              || '.'
              || glcc.segment3
              || '.'
              || glcc.segment4
              || '.'
              || glcc.segment5
              || '.'
              || glcc.segment6
              || '.'
              || glcc.segment7
              || '.'
              || glcc.segment8
              || '.'
              || glcc.segment9
              || '.'
              || glcc.segment10
              || '.'
              || glcc.segment11
              || '.'
              || glcc.segment12
              || '.'
              || glcc.segment13
                ) legacy_13_code_Segments,
                ( aga.oracle_company
              || '.'
              || aga.oracle_product
              || '.'
              || aga.oracle_account
              || '.'
              || aga.oracle_cost_center
              || '.'
              || aga.oracle_location
              || '.'
              || aga.oracle_compliance_code
              || '.'
              || aga.oracle_intercompany
              || '.'
              || aga.oracle_resource_type
              || '.'
              || aga.oracle_future ) aavm_9_code_Segments,
            ( aga.proj_out
              || '|'
              || aga.task_out
              || '|'
              || aga.exp_org_out
              || '|'
              || aga.exp_type_out ) POET,
            'AP_INV_W_WO_PAYMENT_HOLD' invoice_type
          FROM
            ap_invoices_all              aia,
            ap_invoice_lines_all         aila,
            ap_suppliers                 aps,
            ap_supplier_sites_all        apss,
            ap_payment_schedules_all     apsa,
            ap_invoice_distributions_all aida,
            gl_code_combinations         glcc,
            po_headers_all               pha,
            po_lines_all                 pla,
            po_line_locations_all        plla,
            po_distributions_all         pda,
            po_releases_all              pra,
            hr_locations_all             hla,
			aee_gl_aavm_batch_txn_tbl    aga
         WHERE
                1 = 1
               AND aia.invoice_id                                                                     = aila.invoice_id
               AND aia.vendor_id                                                                      = aps.vendor_id
               AND aia.vendor_site_id                                                                 = apss.vendor_site_id
               AND apsa.invoice_id                                                                    = aia.invoice_id
               AND aia.payment_status_flag                                                            = 'N'
               AND aia.cancelled_amount IS NULL
               AND aia.invoice_amount != 0
               AND aida.invoice_id                                                                    = aia.invoice_id
               AND aida.invoice_line_number                                                           = aila.line_number
               AND aida.dist_code_combination_id                                                      = glcc.code_combination_id
               AND aila.po_header_id                                                                  = pha.po_header_id (+)
               AND aila.po_line_id                                                                    = pla.po_line_id (+)
               AND aila.po_line_location_id                                                           = plla.line_location_id (+)
               AND aila.po_distribution_id                                                            = pda.po_distribution_id (+)
               AND aila.po_release_id                                                                 = pra.po_release_id (+)
               AND aila.ship_to_location_id                                                           = hla.location_id (+)			   
			   AND ap_invoices_pkg.get_posting_status(aia.invoice_id) = 'Y'
               AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')			    
               AND glcc.segment1                                      = aga.corp
               AND glcc.segment2                                      = aga.util
               AND glcc.segment3                                      = aga.business_division
               AND glcc.segment4                                      = aga.major_minor
               AND glcc.segment5                                      = aga.fmc
               AND glcc.segment6                                      = aga.rmc
               AND glcc.segment7                                      = aga.tran_type
               AND glcc.segment8                                      = aga.project
               AND glcc.segment9                                      = aga.product
               AND glcc.segment10                                     = aga.activity
               AND glcc.segment11                                     = aga.resource_type
               AND aga.batch_no = gv_batch
               AND CASE
                       WHEN aila.line_type_lookup_code = 'TAX'
                          AND aida.amount  = 0 THEN
                           0
                       ELSE
                           1
                   END = 1
               AND EXISTS (
                SELECT DISTINCT
                    ( 1 )
                  FROM
                    ap_invoice_distributions_all aida
                 WHERE
                        aida.match_status_flag = 'A'      
                       AND aida.invoice_id = aia.invoice_id
            )
			
        MINUS
		
       SELECT DISTINCT
            aila.invoice_id                                                                                          invoice_id,
            aia.invoice_num,         
            aila.line_number invoice_line_number,
            aida.distribution_line_number,            
            case 
            when aia.invoice_type_lookup_code = 'RETAINAGE RELEASE' 
            THEN 
                'STANDARD' 
            ELSE 
                aia.invoice_type_lookup_code 
            END invoice_type_lookup_code,
            decode(aila.line_type_lookup_code,'RETAINAGE RELEASE','ITEM',aila.line_type_lookup_code)   line_type_lookup_code,
            aia.invoice_date,
            aia.source,
            aia.payment_status_flag,
            aia.invoice_amount,
            aia.cancelled_amount,
            aida.amount                                                                                              amount,
            aida.quantity_invoiced                                                                                   quantity_invoiced,
            aida.unit_price                                                                                          unit_price,
            substr((replace(replace(replace(replace(aia.description, '""', '"'), '"', '""'), CHR(10), NULL), CHR(13), NULL)), 1, 220)
            || (
                CASE
                    WHEN (
                        SELECT DISTINCT
                            release_num
                          FROM
                            po_releases_all      pra,
                            ap_invoice_lines_all aila
                         WHERE
                                pra.po_release_id = aila.po_release_id
                               AND aila.invoice_id = aia.invoice_id
                               AND ROWNUM          = 1
                    ) IS NULL THEN
                        (
                            SELECT DISTINCT
                                ' EBS PO#' || segment1
                              FROM
                                po_headers_all       pha,
                                ap_invoice_lines_all aila
                             WHERE
                                    pha.po_header_id = aila.po_header_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                    ELSE
                        (
                            SELECT DISTINCT
                                ' EBS PO#' || segment1
                              FROM
                                po_headers_all       pha,
                                ap_invoice_lines_all aila
                             WHERE
                                    pha.po_header_id = aila.po_header_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                        || '-'
                        || (
                            SELECT DISTINCT
                                release_num
                              FROM
                                po_releases_all      pra,
                                ap_invoice_lines_all aila
                             WHERE
                                    pra.po_release_id = aila.po_release_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                END
            )    header_description,          
            substr(replace(replace(replace(replace(aila.description, '""', '"'), '"', '""'), CHR(10), NULL), CHR(13), NULL),1,240) line_description,
            (
                SELECT DISTINCT
                    location_code
                  FROM
                    hr_locations_all     hla,
                    ap_invoice_lines_all aila
                 WHERE
                        1 = 1
                       AND aia.invoice_id        = aila.invoice_id
                       AND hla.location_id       = aila.ship_to_location_id
                       AND hla.ship_to_site_flag = 'Y'
                       AND ROWNUM                = 1
            )   ship_to_location_code,
            aps.segment1   vendor_num,
            upper(TRIM(TRIM(BOTH ' ' FROM aps.vendor_name)))  vendor_name,            
            decode(instr(upper(apss.vendor_site_code), 'CCTM'), 0, upper(apss.vendor_site_code), replace(upper(apss.vendor_site_code), 'CCTM', '')  || '-ERS')   vendor_site_code,
            aia.pay_group_lookup_code,
           CASE
                WHEN (
                    SELECT DISTINCT
                        1
                      FROM
                        ap_invoice_lines_all aila
                     WHERE
                            aila.invoice_id = aia.invoice_id
                           AND aila.po_header_id IS NULL
                           AND aila.line_type_lookup_code = 'ITEM'
                ) = 1 
                THEN
                    aia.pay_group_lookup_code
                WHEN (
                    SELECT DISTINCT
                        1
                      FROM
                        po_headers_all       ph,
                        ap_invoice_lines_all aila
                     WHERE
                            aila.invoice_id = aia.invoice_id
                           AND aila.line_type_lookup_code = 'ITEM'
                           AND aila.po_header_id          = ph.po_header_id
                           AND ph.closed_code IN ( 'FINALLY CLOSED' )
                ) = 1 
                THEN
                    aia.pay_group_lookup_code
                ELSE
                    nvl((
                        SELECT
                            scm.ship_to_org
                          FROM
                            ap_invoice_lines_all   aila, aee_scm_po_details_tbl scm
                         WHERE
                                aia.invoice_id = aila.invoice_id
								and aila.po_header_id is not null
                               AND aila.po_header_id          = scm.po_header_id
                               AND aila.po_line_id            = scm.po_line_id
                               AND aila.po_line_location_id   = scm.line_location_id
                               AND aila.po_distribution_id    = scm.po_distribution_id
                               AND nvl(aila.po_release_id, 1) = nvl(scm.po_release_id, 1)
                               AND ROWNUM                     = 1
                    ), aia.pay_group_lookup_code)
            END   legal_entity_name,  
            (select decode(apt.name, '2% 15 NET 30', '2% 10 NET 30', '2% 10 NET 45', '2% 10 NET 30','1% 10 NET 30', '2% 10 NET 30', apt.name) 
               from apps.ap_terms apt
              where apt.term_id = aia.terms_id ) terms_name,
            aia.payment_method_code,
            aia.voucher_num,
            (
                SELECT
                    first_name
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_first_name,
            (
                SELECT
                    last_name
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_last_name,
            (
                SELECT
                    employee_number
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_employee_num,
			CASE
                WHEN (
                    SELECT
                        release_num
                    FROM
                        po_releases_all pra
                    WHERE
                        pra.po_release_id = aila.po_release_id
                ) IS NULL THEN
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                ELSE
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                    || '-'
                    || (
                        SELECT
                            release_num
                        FROM
                            po_releases_all pra
                        WHERE
                            pra.po_release_id = aila.po_release_id
                    )
            END po_number,
            pla.line_num                                                                                             po_line_number,
            plla.shipment_num                                                                                        po_shipment_num,
            pda.distribution_num                                                                                     po_distribution_num,
            aida.match_Status_flag,
            aga.project legacy_project,
             (
                glcc.segment1
              || '.'
              || glcc.segment2
              || '.'
              || glcc.segment3
              || '.'
              || glcc.segment4
              || '.'
              || glcc.segment5
              || '.'
              || glcc.segment6
              || '.'
              || glcc.segment7
              || '.'
              || glcc.segment8
              || '.'
              || glcc.segment9
              || '.'
              || glcc.segment10
              || '.'
              || glcc.segment11
              || '.'
              || glcc.segment12
              || '.'
              || glcc.segment13
                ) legacy_13_code_Segments,
                ( aga.oracle_company
              || '.'
              || aga.oracle_product
              || '.'
              || aga.oracle_account
              || '.'
              || aga.oracle_cost_center
              || '.'
              || aga.oracle_location
              || '.'
              || aga.oracle_compliance_code
              || '.'
              || aga.oracle_intercompany
              || '.'
              || aga.oracle_resource_type
              || '.'
              || aga.oracle_future ) aavm_9_code_Segments,
            ( aga.proj_out
              || '|'
              || aga.task_out
              || '|'
              || aga.exp_org_out
              || '|'
              || aga.exp_type_out ) POET,
            'AP_INV_W_WO_PAYMENT_HOLD' invoice_type
          FROM
            ap_invoices_all              aia,
            ap_invoice_lines_all         aila,
            ap_suppliers                 aps,
            ap_supplier_sites_all        apss,
            ap_payment_schedules_all     apsa,
            ap_holds_all                 aha,
            ap_invoice_distributions_all aida,
            gl_code_combinations         glcc,
            po_headers_all               pha,
            po_lines_all                 pla,
            po_line_locations_all        plla,
            po_distributions_all         pda,
            po_releases_all              pra,
            hr_locations_all             hla
			,aee_gl_aavm_batch_txn_tbl   aga
         WHERE
                1 = 1
               AND aha.invoice_id                                                                     = aia.invoice_id
               AND aha.release_lookup_code IS NULL
               AND aia.invoice_id                                                                     = aila.invoice_id
               AND aia.vendor_id                                                                      = aps.vendor_id
               AND aia.vendor_site_id                                                                 = apss.vendor_site_id
               AND apsa.invoice_id                                                                    = aia.invoice_id
               AND aia.payment_status_flag                                                            = 'N'
               AND aia.cancelled_amount IS NULL
               AND aia.invoice_amount != 0
               AND aida.invoice_id                                                                    = aia.invoice_id
               AND aida.invoice_line_number                                                           = aila.line_number
               AND aida.dist_code_combination_id                                                      = glcc.code_combination_id
               AND aila.po_header_id                                                                  = pha.po_header_id (+)
               AND aila.po_line_id                                                                    = pla.po_line_id (+)
               AND aila.po_line_location_id                                                           = plla.line_location_id (+)
               AND aila.po_distribution_id                                                            = pda.po_distribution_id (+)
               AND aila.po_release_id                                                                 = pra.po_release_id (+)
               AND aila.ship_to_location_id                                                           = hla.location_id (+)			  
			   AND ap_invoices_pkg.get_posting_status(aia.invoice_id) = 'Y'
			   AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')
               AND glcc.segment1                                      = aga.corp
               AND glcc.segment2                                      = aga.util
               AND glcc.segment3                                      = aga.business_division
               AND glcc.segment4                                      = aga.major_minor
               AND glcc.segment5                                      = aga.fmc
               AND glcc.segment6                                      = aga.rmc
               AND glcc.segment7                                      = aga.tran_type
               AND glcc.segment8                                      = aga.project
               AND glcc.segment9                                      = aga.product
               AND glcc.segment10                                     = aga.activity
               AND glcc.segment11                                     = aga.resource_type
               AND aga.batch_no = gv_batch
               AND CASE
                       WHEN aila.line_type_lookup_code = 'TAX' 
                          AND aida.amount  = 0 THEN
                           0
                       ELSE
                           1
                   END = 1
               AND EXISTS (
                SELECT DISTINCT
                    ( 1 )
                  FROM
                    ap_invoice_distributions_all aida
                 WHERE
                        aida.match_status_flag = 'A'      
                       AND aida.invoice_id = aia.invoice_id
            ) 
			)
            
			UNION
			
            SELECT DISTINCT
            aila.invoice_id                                                                                          invoice_id,
            aia.invoice_num,         
            aila.line_number invoice_line_number,
            aida.distribution_line_number,            
            case 
            when aia.invoice_type_lookup_code = 'RETAINAGE RELEASE' 
            THEN 
                'STANDARD' 
            ELSE 
                aia.invoice_type_lookup_code 
            END invoice_type_lookup_code,
            decode(aila.line_type_lookup_code,'RETAINAGE RELEASE','ITEM',aila.line_type_lookup_code)   line_type_lookup_code,
            aia.invoice_date,
            aia.source,
            aia.payment_status_flag,
            aia.invoice_amount,
            aia.cancelled_amount,
            aida.amount                                                                                              amount,
            aida.quantity_invoiced                                                                                   quantity_invoiced,
            aida.unit_price                                                                                          unit_price,
            substr((replace(replace(replace(replace(aia.description, '""', '"'), '"', '""'), CHR(10), NULL), CHR(13), NULL)), 1, 220)
            || (
                CASE
                    WHEN (
                        SELECT DISTINCT
                            release_num
                          FROM
                            po_releases_all      pra,
                            ap_invoice_lines_all aila
                         WHERE
                                pra.po_release_id = aila.po_release_id
                               AND aila.invoice_id = aia.invoice_id
                               AND ROWNUM          = 1
                    ) IS NULL THEN
                        (
                            SELECT DISTINCT
                                ' EBS PO#' || segment1
                              FROM
                                po_headers_all       pha,
                                ap_invoice_lines_all aila
                             WHERE
                                    pha.po_header_id = aila.po_header_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                    ELSE
                        (
                            SELECT DISTINCT
                                ' EBS PO#' || segment1
                              FROM
                                po_headers_all       pha,
                                ap_invoice_lines_all aila
                             WHERE
                                    pha.po_header_id = aila.po_header_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                        || '-'
                        || (
                            SELECT DISTINCT
                                release_num
                              FROM
                                po_releases_all      pra,
                                ap_invoice_lines_all aila
                             WHERE
                                    pra.po_release_id = aila.po_release_id
                                   AND aila.invoice_id = aia.invoice_id
                                   AND ROWNUM          = 1
                        )
                END
            )    header_description,          
            substr(replace(replace(replace(replace(aila.description, '""', '"'), '"', '""'), CHR(10), NULL), CHR(13), NULL),1,240) line_description,
            (
                SELECT DISTINCT
                    location_code
                  FROM
                    hr_locations_all     hla,
                    ap_invoice_lines_all aila
                 WHERE
                        1 = 1
                       AND aia.invoice_id        = aila.invoice_id
                       AND hla.location_id       = aila.ship_to_location_id
                       AND hla.ship_to_site_flag = 'Y'
                       AND ROWNUM                = 1
            )   ship_to_location_code,
            aps.segment1   vendor_num,
            upper(TRIM(TRIM(BOTH ' ' FROM aps.vendor_name)))  vendor_name,            
            decode(instr(upper(apss.vendor_site_code), 'CCTM'), 0, upper(apss.vendor_site_code), replace(upper(apss.vendor_site_code), 'CCTM', '')  || '-ERS')   vendor_site_code,
            aia.pay_group_lookup_code,
           CASE
                WHEN (
                    SELECT DISTINCT
                        1
                      FROM
                        ap_invoice_lines_all aila
                     WHERE
                            aila.invoice_id = aia.invoice_id
                           AND aila.po_header_id IS NULL
                           AND aila.line_type_lookup_code = 'ITEM'
                ) = 1 
                THEN
                    aia.pay_group_lookup_code
                WHEN (
                    SELECT DISTINCT
                        1
                      FROM
                        po_headers_all       ph,
                        ap_invoice_lines_all aila
                     WHERE
                            aila.invoice_id = aia.invoice_id
                           AND aila.line_type_lookup_code = 'ITEM'
                           AND aila.po_header_id          = ph.po_header_id
                           AND ph.closed_code IN ( 'FINALLY CLOSED' )
                ) = 1 
                THEN
                    aia.pay_group_lookup_code
                ELSE
                    nvl((
                        SELECT
                            scm.ship_to_org
                          FROM
                            ap_invoice_lines_all   aila, aee_scm_po_details_tbl scm
                         WHERE
                                aia.invoice_id = aila.invoice_id
								and aila.po_header_id is not null
                               AND aila.po_header_id          = scm.po_header_id
                               AND aila.po_line_id            = scm.po_line_id
                               AND aila.po_line_location_id   = scm.line_location_id
                               AND aila.po_distribution_id    = scm.po_distribution_id
                               AND nvl(aila.po_release_id, 1) = nvl(scm.po_release_id, 1)
                               AND ROWNUM                     = 1
                    ), aia.pay_group_lookup_code)
            END   legal_entity_name,  
            (select decode(apt.name, '2% 15 NET 30', '2% 10 NET 30', '2% 10 NET 45', '2% 10 NET 30','1% 10 NET 30', '2% 10 NET 30', apt.name) 
               from apps.ap_terms apt
              where apt.term_id = aia.terms_id ) terms_name,
            aia.payment_method_code,
            aia.voucher_num,
            (
                SELECT
                    first_name
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_first_name,
            (
                SELECT
                    last_name
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_last_name,
            (
                SELECT
                    employee_number
                  FROM
                    per_all_people_f
                 WHERE
                        person_id = aia.requester_id
                       AND trunc(sysdate) BETWEEN trunc(effective_start_date) AND trunc(effective_end_date)
            )                                                                                              requester_employee_num,
			CASE
                WHEN (
                    SELECT
                        release_num
                    FROM
                        po_releases_all pra
                    WHERE
                        pra.po_release_id = aila.po_release_id
                ) IS NULL THEN
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                ELSE
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                    || '-'
                    || (
                        SELECT
                            release_num
                        FROM
                            po_releases_all pra
                        WHERE
                            pra.po_release_id = aila.po_release_id
                    )
            END po_number,
            pla.line_num                                                                                             po_line_number,
            plla.shipment_num                                                                                        po_shipment_num,
            pda.distribution_num                                                                                     po_distribution_num,
            aida.match_Status_flag,
            aga.project legacy_project,
             (
                glcc.segment1
              || '.'
              || glcc.segment2
              || '.'
              || glcc.segment3
              || '.'
              || glcc.segment4
              || '.'
              || glcc.segment5
              || '.'
              || glcc.segment6
              || '.'
              || glcc.segment7
              || '.'
              || glcc.segment8
              || '.'
              || glcc.segment9
              || '.'
              || glcc.segment10
              || '.'
              || glcc.segment11
              || '.'
              || glcc.segment12
              || '.'
              || glcc.segment13
                ) legacy_13_code_Segments,
                ( aga.oracle_company
              || '.'
              || aga.oracle_product
              || '.'
              || aga.oracle_account
              || '.'
              || aga.oracle_cost_center
              || '.'
              || aga.oracle_location
              || '.'
              || aga.oracle_compliance_code
              || '.'
              || aga.oracle_intercompany
              || '.'
              || aga.oracle_resource_type
              || '.'
              || aga.oracle_future ) aavm_9_code_Segments,
            ( aga.proj_out
              || '|'
              || aga.task_out
              || '|'
              || aga.exp_org_out
              || '|'
              || aga.exp_type_out ) POET,
            'AP_INV_ON_HOLD' invoice_type             
          FROM
            ap_invoices_all              aia,
            ap_invoice_lines_all         aila,
            ap_suppliers                 aps,
            ap_supplier_sites_all        apss,
            ap_holds_all                 aha,
            ap_invoice_distributions_all aida,
            gl_code_combinations         glcc,
            po_headers_all               pha,
            po_lines_all                 pla,
            po_line_locations_all        plla,
            po_distributions_all         pda,
            po_releases_all              pra,
            hr_locations_all             hla,
			aee_gl_aavm_batch_txn_tbl    aga
         WHERE
                1 = 1
               AND aha.invoice_id                                                                     = aia.invoice_id
               AND aha.release_lookup_code IS NULL
               AND aia.invoice_id                                                                     = aila.invoice_id
               AND aia.vendor_id                                                                      = aps.vendor_id
               AND aia.vendor_site_id                                                                 = apss.vendor_site_id
               AND aia.payment_status_flag                                                            = 'N'
               AND aia.cancelled_amount IS NULL
               AND aia.invoice_amount != 0
               AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')
               AND aida.invoice_id                                                                    = aia.invoice_id
               AND aida.invoice_line_number                                                           = aila.line_number
               AND aida.dist_code_combination_id                                                      = glcc.code_combination_id
               AND aila.po_header_id                                                                  = pha.po_header_id (+)
               AND aila.po_line_id                                                                    = pla.po_line_id (+)
               AND aila.po_line_location_id                                                           = plla.line_location_id (+)
               AND aila.po_distribution_id                                                            = pda.po_distribution_id (+)
               AND aila.po_release_id                                                                 = pra.po_release_id (+)
               AND aila.ship_to_location_id                                                           = hla.location_id (+)
			   AND glcc.segment1                                      = aga.corp
               AND glcc.segment2                                      = aga.util
               AND glcc.segment3                                      = aga.business_division
               AND glcc.segment4                                      = aga.major_minor
               AND glcc.segment5                                      = aga.fmc
               AND glcc.segment6                                      = aga.rmc
               AND glcc.segment7                                      = aga.tran_type
               AND glcc.segment8                                      = aga.project
               AND glcc.segment9                                      = aga.product
               AND glcc.segment10                                     = aga.activity
               AND glcc.segment11                                     = aga.resource_type
               AND aga.batch_no = gv_batch
               AND CASE
                       WHEN aila.line_type_lookup_code = 'TAX' 
                          AND aida.amount                                                                        = 0 THEN
                           0
                       ELSE
                           1
                   END = 1
       
	   UNION
		
        SELECT DISTINCT
    aila.invoice_id,
    aia.invoice_num, 
    aila.line_number invoice_line_number,
    Null distribution_line_number,
    case 
    when aia.invoice_type_lookup_code = 'RETAINAGE RELEASE' 
    THEN 
        'STANDARD' 
    ELSE 
        aia.invoice_type_lookup_code 
    END invoice_type_lookup_code,
    aila.line_type_lookup_code,
    aia.invoice_Date,
    aia.source,
    NULL payment_status_flag,
    aia.invoice_amount,
    NULL cancelled_amount,
    aila.amount,
    aila.quantity_invoiced,
    aila.unit_price,
    aia.description header_description,          
    substr(replace(replace(replace(replace(aila.description, '""', '"'), '"', '""'), CHR(10), NULL), CHR(13), NULL),1,240) line_description,
    (
        SELECT
            hla.location_code
          FROM
            hr_locations_all hla
         WHERE
            aila.ship_to_location_id = hla.location_id
    )   ship_to_location_code,
     (
        SELECT
            aps.segment1
          FROM
            ap_suppliers aps
         WHERE
            aia.vendor_id = aps.vendor_id
    )      vendor_num,
    (
        SELECT
            upper(TRIM(TRIM(BOTH ' ' FROM aps.vendor_name)))
          FROM
            ap_suppliers aps
         WHERE
            aia.vendor_id = aps.vendor_id
    )    vendor_name,
    (
        SELECT
            decode(instr(upper(apss.vendor_site_code), 'CCTM'), 0, upper(apss.vendor_site_code), replace(upper(apss.vendor_site_code),'CCTM', '') || '-ERS')
          FROM
            ap_suppliers          aps,
            ap_supplier_sites_all apss
         WHERE
                aia.vendor_site_id = apss.vendor_site_id
               AND aia.vendor_id = aps.vendor_id
    )   vendor_site_code,
    aia.pay_group_lookup_code,
     CASE
        WHEN (
            SELECT DISTINCT
                1
              FROM
                ap_invoice_lines_interface ailla
             WHERE
                    aia.invoice_id = ailla.invoice_id
                   AND ailla.po_header_id IS NOT NULL
        ) = 1 THEN
            nvl((
                SELECT DISTINCT
                    scm.ship_to_org
                  FROM
                    ap_invoice_lines_interface ailla, aee_scm_po_details_tbl     scm
                 WHERE
                        aia.invoice_id = ailla.invoice_id
                       AND ailla.po_header_id IS NOT NULL
                       AND ailla.po_header_id          = scm.po_header_id
                       AND ailla.po_line_id            = scm.po_line_id
                       AND ailla.po_line_location_id   = scm.line_location_id
                       AND ailla.po_distribution_id    = scm.po_distribution_id
                       AND nvl(ailla.po_release_id, 1) = nvl(scm.po_release_id, 1)
            ), 'AMS')
        ELSE
            decode(nvl(c.legalentity, 'AMS'), '***', 'AMS', nvl(c.legalentity, 'AMS'))
    END   legal_entity_name,
    (
        SELECT
            decode(apt.name, '2% 15 NET 30', '2% 10 NET 30', '2% 10 NET 45', '2% 10 NET 30',
                   '1% 10 NET 30', '2% 10 NET 30', apt.name)
          FROM
            ap_terms apt
         WHERE
            aia.terms_id = apt.term_id
    )  terms_name,
    aia.payment_method_code,
    aia.voucher_num,
    NULL requester_first_name,
    NULL requester_last_name,
    NULL requester_employee_num,    
    (
        CASE
                WHEN (
                    SELECT
                        release_num
                    FROM
                        po_releases_all pra
                    WHERE
                        pra.po_release_id = aila.po_release_id
                ) IS NULL THEN
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                ELSE
                    (
                        SELECT
                            segment1
                        FROM
                            po_headers_all pha
                        WHERE
                            pha.po_header_id = aila.po_header_id
                    )
                    || '-'
                    || (
                        SELECT
                            release_num
                        FROM
                            po_releases_all pra
                        WHERE
                            pra.po_release_id = aila.po_release_id
                    )
            END
    )  po_number,
    (
        SELECT
            pla.line_num
          FROM
            po_lines_all pla
         WHERE
            pla.po_line_id = aila.po_line_id
    )  po_line_number,
    (
        SELECT
            plla.shipment_num
          FROM
            po_line_locations_all plla
         WHERE
            plla.line_location_id = aila.po_line_location_id
    ) po_shipment_num,
    (
        SELECT
            pda.distribution_num
          FROM
            po_distributions_all pda
         WHERE
            pda.po_distribution_id = aila.po_distribution_id
    )  po_distribution_num,
    NULL match_Status_flag,
  (select  aga.project 
    FROM aee_gl_aavm_batch_txn_tbl    aga,
         gl_code_combinations         glcc
         WHERE
                1 = 1
               AND aila.dist_code_combination_id = glcc.code_combination_id
               AND glcc.segment1                 = aga.corp
               AND glcc.segment2                 = aga.util
               AND glcc.segment3                 = aga.business_division
               AND glcc.segment4                 = aga.major_minor
               AND glcc.segment5                 = aga.fmc
               AND glcc.segment6                 = aga.rmc
               AND glcc.segment7                 = aga.tran_type
               AND glcc.segment8                 = aga.project
               AND glcc.segment9                 = aga.product
               AND glcc.segment10                = aga.activity
               AND glcc.segment11                = aga.resource_type
               AND aga.batch_no= gv_batch)                                 legacy_project,
    aila.dist_code_concatenated                                            legacy_13_code_Segments,
                (
        SELECT
           aga.oracle_company
      || '.'
      || aga.oracle_product
      || '.'
      || aga.oracle_account
      || '.'
      || aga.oracle_cost_center
      || '.'
      || aga.oracle_location
      || '.'
      || aga.oracle_compliance_code
      || '.'
      || aga.oracle_intercompany
      || '.'
      || aga.oracle_resource_type
      || '.'
      || aga.oracle_future
          FROM
            aee_gl_aavm_batch_txn_tbl    aga,           
            gl_code_combinations         glcc
         WHERE
                1 = 1              
               AND aila.dist_code_combination_id = glcc.code_combination_id
               AND glcc.segment1                 = aga.corp
               AND glcc.segment2                 = aga.util
               AND glcc.segment3                 = aga.business_division
               AND glcc.segment4                 = aga.major_minor
               AND glcc.segment5                 = aga.fmc
               AND glcc.segment6                 = aga.rmc
               AND glcc.segment7                 = aga.tran_type
               AND glcc.segment8                 = aga.project
               AND glcc.segment9                 = aga.product
               AND glcc.segment10                = aga.activity
               AND glcc.segment11                = aga.resource_type
               AND aga.batch_no = gv_batch
    )                                                                             aavm_9_code_Segments,
    (
        SELECT
           aga.proj_out
              || '|'
              || aga.task_out
              || '|'
              || aga.exp_org_out
              || '|'
              || aga.exp_type_out
          FROM
            aee_gl_aavm_batch_txn_tbl    aga,
            gl_code_combinations         glcc
         WHERE
                1 = 1
               AND aila.dist_code_combination_id = glcc.code_combination_id
               AND glcc.segment1                 = aga.corp
               AND glcc.segment2                 = aga.util
               AND glcc.segment3                 = aga.business_division
               AND glcc.segment4                 = aga.major_minor
               AND glcc.segment5                 = aga.fmc
               AND glcc.segment6                 = aga.rmc
               AND glcc.segment7                 = aga.tran_type
               AND glcc.segment8                 = aga.project
               AND glcc.segment9                 = aga.product
               AND glcc.segment10                = aga.activity
               AND glcc.segment11                = aga.resource_type
               AND aga.batch_no = gv_batch
    )                                                                                            POET,
   'API_INV_INTERFACE' invoice_type
  FROM
    ap_invoices_interface      aia,
    ap_invoice_lines_interface aila,
    (
        SELECT DISTINCT
            y.invoice_id,
            CASE
                WHEN cnt IS NULL THEN
                    'AMS'
                ELSE
                    y.le_name
            END legalentity
          FROM
            (
                SELECT
                    invoice_id,
                    COUNT(1) cnt
                  FROM
                    (
                        SELECT DISTINCT
                            DENSE_RANK()
                            OVER(PARTITION BY aila.invoice_id, substr(aila.dist_code_concatenated, 1, 3)
                                 ORDER BY
                                    aila.invoice_id, substr(aila.dist_code_concatenated, 1, 3) ASC
                            )                                         rnum,
                            substr(aila.dist_code_concatenated, 1, 3) le_name,
                            aila.invoice_id
                          FROM
                            ap_invoices_interface      aia,
                            ap_invoice_lines_interface aila
                         WHERE
                                aia.invoice_id = aila.invoice_id
                               AND aia.invoice_amount != 0
                               AND aia.status IS NULL
                               AND ( aia.attribute1 IS NULL
                                OR aia.attribute1 NOT IN ( 20, 21, 23, 25, 29,
                                                           31, 32, 34, 38 ) )
                               AND aia.vendor_id IS NOT NULL
                               AND aia.vendor_site_id IS NOT NULL
                               AND aia.invoice_num IS NOT NULL
                               AND aia.invoice_date IS NOT NULL
                               AND aia.invoice_type_lookup_code IS NOT NULL
                               AND aia.voucher_num IS NOT NULL
                               AND aila.line_number IS NOT NULL
                               AND aila.line_type_lookup_code IS NOT NULL
                    )
                 GROUP BY
                    invoice_id
                HAVING
                    COUNT(1) = 1
            ) x,
            (
                SELECT DISTINCT
                    DENSE_RANK()
                    OVER(PARTITION BY aila.invoice_id, substr(aila.dist_code_concatenated, 1, 3)
                         ORDER BY
                            aila.invoice_id, substr(aila.dist_code_concatenated, 1, 3) ASC
                    )                                         rnum,
                    substr(aila.dist_code_concatenated, 1, 3) le_name,
                    aila.invoice_id
                  FROM
                    ap_invoices_interface      aia,
                    ap_invoice_lines_interface aila
                 WHERE
                        aia.invoice_id = aila.invoice_id
                       AND aia.invoice_amount != 0
                       AND aia.status IS NULL
                       AND ( aia.attribute1 IS NULL
                        OR aia.attribute1 NOT IN ( 20, 21, 23, 25, 29,
                                                   31, 32, 34, 38 ) )
                       AND aia.vendor_id IS NOT NULL
                       AND aia.vendor_site_id IS NOT NULL
                       AND aia.invoice_num IS NOT NULL
                       AND aia.invoice_date IS NOT NULL
                       AND aia.invoice_type_lookup_code IS NOT NULL
                       AND aia.voucher_num IS NOT NULL
                       AND aila.line_number IS NOT NULL
                       AND aila.line_type_lookup_code IS NOT NULL
            ) y            
         WHERE
            x.invoice_id (+) = y.invoice_id
    )  c
 WHERE
        1 = 1
       AND aia.invoice_id = c.invoice_id (+)
       AND aia.invoice_id = aila.invoice_id
       AND aia.invoice_amount != 0
       AND aia.status IS NULL
       AND ( aia.attribute1 IS NULL
        OR aia.attribute1 NOT IN ( 20, 21, 23, 25, 29,31, 32, 34, 38 ) )
       AND aia.vendor_id IS NOT NULL
       AND aia.vendor_site_id IS NOT NULL
       AND aia.invoice_num IS NOT NULL
       AND aia.invoice_date IS NOT NULL
       AND aia.invoice_type_lookup_code IS NOT NULL
       AND aia.voucher_num IS NOT NULL
       AND aila.line_number IS NOT NULL
       AND aila.line_type_lookup_code IS NOT NULL ;

BEGIN
        FOR i IN cur_invoice LOOP
		ln_line_count := ln_line_count+1;
		 gt_xxap_inv_tab(ln_line_count).invoice_id                := i.invoice_id;
		 gt_xxap_inv_tab(ln_line_count).invoice_num               := i.invoice_num;              
		 gt_xxap_inv_tab(ln_line_count).invoice_line_number       := i.invoice_line_number;
		 gt_xxap_inv_tab(ln_line_count).distribution_line_number  := i.distribution_line_number; 
		 gt_xxap_inv_tab(ln_line_count).invoice_type_lookup_code  := i.invoice_type_lookup_code; 
		 gt_xxap_inv_tab(ln_line_count).line_type_lookup_code     := i.line_type_lookup_code;    
		 gt_xxap_inv_tab(ln_line_count).invoice_date              := i.invoice_date;             
		 gt_xxap_inv_tab(ln_line_count).source                    := i.source;                   
		 gt_xxap_inv_tab(ln_line_count).payment_status_flag       := i.payment_status_flag;      
		 gt_xxap_inv_tab(ln_line_count).invoice_amount            := i.invoice_amount;           
		 gt_xxap_inv_tab(ln_line_count).cancelled_amount          := i.cancelled_amount;         
		 gt_xxap_inv_tab(ln_line_count).amount                    := i.amount;                   
		 gt_xxap_inv_tab(ln_line_count).quantity_invoiced         := i.quantity_invoiced;        
		 gt_xxap_inv_tab(ln_line_count).unit_price                := i.unit_price;               
		 gt_xxap_inv_tab(ln_line_count).header_description        := i.header_description;       
		 gt_xxap_inv_tab(ln_line_count).line_description          := i.line_description;         
		 gt_xxap_inv_tab(ln_line_count).ship_to_location_code     := i.ship_to_location_code;    
		 gt_xxap_inv_tab(ln_line_count).vendor_num                := i.vendor_num;               
		 gt_xxap_inv_tab(ln_line_count).vendor_name               := i.vendor_name;              
		 gt_xxap_inv_tab(ln_line_count).vendor_site_code          := i.vendor_site_code;         
		 gt_xxap_inv_tab(ln_line_count).pay_group_lookup_code     := i.pay_group_lookup_code;    
		 gt_xxap_inv_tab(ln_line_count).legal_entity_name         := i.legal_entity_name;        
		 gt_xxap_inv_tab(ln_line_count).terms_name                := i.terms_name;               
		 gt_xxap_inv_tab(ln_line_count).payment_method_code       := i.payment_method_code;      
		 gt_xxap_inv_tab(ln_line_count).voucher_num               := i.voucher_num;              
		 gt_xxap_inv_tab(ln_line_count).requester_first_name      := i.requester_first_name;     
		 gt_xxap_inv_tab(ln_line_count).requester_last_name       := i.requester_last_name;      
		 gt_xxap_inv_tab(ln_line_count).requester_employee_num    := i.requester_employee_num;   
		 gt_xxap_inv_tab(ln_line_count).po_number                 := i.po_number;                
		 gt_xxap_inv_tab(ln_line_count).po_line_number            := i.po_line_number;          
		 gt_xxap_inv_tab(ln_line_count).po_shipment_num           := i.po_shipment_num;          
		 gt_xxap_inv_tab(ln_line_count).po_distribution_num       := i.po_distribution_num;      
		 gt_xxap_inv_tab(ln_line_count).match_status_flag         := i.match_status_flag;        
		 gt_xxap_inv_tab(ln_line_count).invoice_type              := i.invoice_type;             
				 
		 END LOOP;
            -- Populate aee_fin_ap_inv_details_tbl table  

         BEGIN
            FORALL i IN gt_xxap_inv_tab.first..gt_xxap_inv_tab.last SAVE EXCEPTIONS
               INSERT INTO xxap.aee_fin_ap_inv_details_tbl VALUES gt_xxap_inv_tab ( i );

           fnd_file.put_line(fnd_file.log, 'LOAD_DATA: aee_fin_ap_inv_details_tbl: Records loaded sucessfully: ' || SQL%rowcount);
            COMMIT;
         EXCEPTION
            WHEN ex_dml_errors THEN
               gv_retcode_out        := 1;
               ln_error_count   := SQL%bulk_exceptions.count;               
               fnd_file.put_line(fnd_file.log, 'LOAD_DATA: aee_fin_ap_inv_details_tbl: Number of failures: ' || ln_error_count);
               FOR i IN 1..ln_error_count LOOP fnd_file.put_line(fnd_file.log, 
                  'LOAD_DATA: aee_fin_ap_inv_details_tbl: Error: '
                  || i
                  || 'Array Index: '
                  || SQL%bulk_exceptions(i).error_index
                  || 'Message: '
                  || sqlerrm(-SQL%bulk_exceptions(i).error_code)
                  
               );
               END LOOP;

            WHEN OTHERS THEN
               gv_retcode_out   := 1;
               gv_errbuf_out   := 'LOAD_DATA: Unexpected error while populating data in aee_fin_ap_inv_details_tbl.'
                            || to_char(sqlcode)
                            || '-'
                            || sqlerrm;
         END;
		   
        COMMIT;
	
        gv_retcode_out := 0;
        gv_errbuf_out := NULL;
    EXCEPTION
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while inserting data in to the custom table with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END custom_table_insert;

END aee_ap_invoice_extract_pkg;
