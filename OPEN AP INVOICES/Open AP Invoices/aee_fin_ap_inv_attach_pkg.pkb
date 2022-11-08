create or replace PACKAGE BODY apps.aee_fin_ap_inv_attach_pkg AS
/*------------------------------------------------------------------------------------
--| Module Name:  aee_fin_ap_inv_attach_pkg
--|
--| Description:  Package to extract the Ap Invoice Attachments for cloud conversion
--| Date:         24-Mar-2022
--|
--| Author:       Akshay Kumar
--|
--| Parameters:   p_level - APInvoiceAttachments/APInvoiceAttachmentsDownload
--|               p_batch_from - starting attachment document number for eg.  from 1
--|               p_batch_to - ending attachment document number for eg. to 1000
---------------------------------------------------------------|
--|  Modification History
---------------------------------------------------------------|
--|  Date        Who           Description
--| ----------- ------------- -------------------------------
--| 24-Mar-2022  Akshay Kumar  Initial Creation
--| 04-Sep-2022  Anshul Patel  Doc Category code added
--| 25-Sep-2022  Adnan Patel   Automated the program to run in batches
------------------------------------------------------------------------------------*/

------------------------------------------------------------------------------------*/

    gv_retcode_out        NUMBER := 0;
    gv_warnretcode_out    NUMBER := 0;
    gv_errorretcode_out   NUMBER := 0;
    gv_errbuf_out         VARCHAR2(4000) := NULL;
    gv_temp_errbuffout    VARCHAR2(4000) := NULL;
    l_ext_ap_inv_loc      VARCHAR2(100); --:= 'OTTC_FIN_MK5_OPEN_APINV';--OTTC_FIN_AP_INVOICE';
    l_ext_file_name       VARCHAR2(100) := NULL;
    gv_process_sts        VARCHAR2(1) := 'Y';
    gv_error_msg          VARCHAR2(2000) := NULL;

    PROCEDURE apinvoice_attachments IS

        CURSOR cur_apinvoice_attachments_data IS
        SELECT 
            NULL batch_id,
            'CREATE' import_action,
            upper(aps.vendor_name) supplier_name,
            aps.segment1 supplier_number, --
            aia.invoice_num,
            aia.invoice_id,            
            'Ameren Services BU' procurement_bu,
             fdc.name category,
            'FILE' type,
            ( ( fad.entity_name
                || '_'
                || fad.document_id
                || '_' )
              || ( regexp_replace((substr(replace(fl.file_name, ' ', '_'), 1, instr(replace(fl.file_name, ' ', '_'), '.', - 1, 1) -
              1)), '[^0-9A-Za-z_]', '') )
              || ( substr(replace(fl.file_name, ' ', '_'), instr(replace(fl.file_name, ' ', '_'), '.', - 1, 1)) ) ) file_name,
            'InvoiceAtt_'
            || TO_CHAR(SYSDATE, 'MMDDYYYY')
            || '.zip' file_attachments_zip,
            fdt.title,
            fdt.description
        FROM
            apps.fnd_attached_documents   fad,
            apps.fnd_documents_tl         fdt,
            apps.fnd_documents            fd,
            apps.fnd_lobs                 fl,
            apps.fnd_document_datatypes   fdd,
            apps.ap_suppliers             aps,
            apps.ap_invoices_all          aia,
			apps.fnd_document_categories  fdc
        WHERE
            1 = 1
            AND fad.document_id = fd.document_id
            AND fad.document_id = fdt.document_id
            AND fd.media_id = fl.file_id
            AND fd.datatype_id = fdd.datatype_id
			AND fad.category_id     = fdc.category_id (+)
            AND fdd.user_name = 'File'
            AND fad.entity_name = 'AP_INVOICES'
            AND fad.pk1_value = TO_CHAR(aia.invoice_id)
            AND aia.vendor_id = aps.vendor_id
            AND aia.payment_status_flag = 'N'
            AND aia.cancelled_amount IS NULL
            AND aia.invoice_amount != 0
			AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE'); 

        lc_header      VARCHAR2(32000) := NULL;

	   --Changes for generating CSV file.
        v_utlfile_issue EXCEPTION;
        v_dir_path_fail EXCEPTION;
        v_no_data_to_process EXCEPTION;
        lv_error_msg   VARCHAR2(2000) := NULL;
        f_handle       utl_file.file_type;
        p_f_handle     utl_file.file_type;
        
    BEGIN	  
	       --Changes for generating CSV file.
        l_ext_file_name := 'APInvoiceAttachmentsInt.csv';
        aee_common_util_pkg.validate_ext_dir(l_ext_ap_inv_loc, gv_process_sts, lv_error_msg);
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_dir_path_fail;
        END IF;

        aee_common_util_pkg.openfile(l_ext_file_name, l_ext_ap_inv_loc, gv_process_sts, lv_error_msg, p_f_handle);
        fnd_file.put_line(fnd_file.log, 'Flag gv_process_sts value - ' || gv_process_sts);
        
        f_handle := p_f_handle;
        
        IF gv_process_sts = 'N' THEN
            gv_error_msg := gv_error_msg || lv_error_msg;
            RAISE v_utlfile_issue;
        END IF;

        FOR rec_data IN cur_apinvoice_attachments_data LOOP
            lc_header := chr(34)
                         || rec_data.invoice_id
                         || chr(34)
                         || ','
                         || chr(34)
                         || rec_data.invoice_num
                         || chr(34)
                         || ','
                         || chr(34)
                         || rec_data.supplier_name
                         || chr(34)
                         || ','
                         || chr(34)
                         || rec_data.supplier_number
                         || chr(34)
                         || ','
                         || chr(34)
                         || rec_data.category
                         || chr(34)
                         || ','
                         || chr(34)
                         || rec_data.file_name
                         || chr(34);

            --Changes for generating CSV filepp''

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
            fnd_file.put_line(fnd_file.log, 'v_dir_path_fail Exception of proc apinvoice_attachments, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_dir_path_fail';
        WHEN v_utlfile_issue THEN
            fnd_file.put_line(fnd_file.log, 'v_utlfile_issue Exception of proc apinvoice_attachments, Error: '
                                            || sqlerrm
                                            || gv_error_msg);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception v_utlfile_issue';
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while fetching the data with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END apinvoice_attachments;

    PROCEDURE apinvoice_attachments_download(p_batch_from IN NUMBER, p_batch_to IN NUMBER) IS

        CURSOR cur_ap_invoice_files(p_batch_from NUMBER, p_batch_to NUMBER) IS
        select * from (SELECT 
            row_number() over (order by fad.document_id) rnum,
            fad.entity_name,
            fad.document_id,
            fad.pk1_value,
            fd.datatype_id,
            ( ( fad.entity_name
                || '_'
                || fad.document_id
                || '_' )
              || ( regexp_replace((substr(replace(fl.file_name, ' ', '_'), 1, instr(replace(fl.file_name, ' ', '_'), '.', - 1, 1)
              - 1)), '[^0-9A-Za-z_]', '') )
              || ( substr(replace(fl.file_name, ' ', '_'), instr(replace(fl.file_name, ' ', '_'), '.', - 1, 1)) ) ) file_name,
            fl.file_data
        FROM
            apps.fnd_attached_documents   fad,
            apps.fnd_documents_tl         fdt,
            apps.fnd_documents            fd,
            apps.fnd_lobs                 fl,
            apps.fnd_document_datatypes   fdd,
            apps.ap_invoices_all          aia,
			apps.ap_suppliers             aps
        WHERE
            1 = 1
            AND fad.document_id = fd.document_id
            AND fad.document_id = fdt.document_id
            AND fd.media_id = fl.file_id
            AND fd.datatype_id = fdd.datatype_id
            AND fdd.user_name = 'File'
            AND fad.entity_name = 'AP_INVOICES'
            AND fad.pk1_value = TO_CHAR(aia.invoice_id)
            AND aia.payment_status_flag = 'N'
            AND aia.cancelled_amount IS NULL
            AND aia.invoice_amount != 0
			AND aia.vendor_id = aps.vendor_id
			AND nvl(aps.vendor_type_lookup_code,'XXX') not in ('EMPLOYEE')
			)
        where rnum >= p_batch_from and rnum <= p_batch_to; 

        v_file       utl_file.file_type;
        v_line       VARCHAR2(1000);
        v_blob_len   NUMBER;
        v_pos        NUMBER;
        v_buffer     RAW(32764);
        v_amt        BINARY_INTEGER := 32764;
        
    BEGIN 
    
        FOR c_file IN cur_ap_invoice_files(p_batch_from, p_batch_to) LOOP
            v_file := utl_file.fopen(l_ext_ap_inv_loc, c_file.file_name, 'wb', 32764);
            v_blob_len := dbms_lob.getlength(c_file.file_data);
            v_pos := 1;
            WHILE v_pos < v_blob_len LOOP
                dbms_lob.read(c_file.file_data, v_amt, v_pos, v_buffer);
                utl_file.put_raw(v_file, v_buffer, true);
                v_pos := v_pos + v_amt;
            END LOOP;

            utl_file.fclose(v_file);
        END LOOP;

        gv_retcode_out := 0;
        gv_errbuf_out := NULL;
        
    EXCEPTION
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Exception occured while fetching the data with error message: ' || sqlerrm);
            gv_retcode_out := 2;
            gv_errbuf_out := 'In Exception others - ' || sqlerrm;
    END apinvoice_attachments_download;

    PROCEDURE main (
        p_errbuf_out        OUT   VARCHAR2,
        p_retcode_out       OUT   NUMBER,
        p_level             IN    VARCHAR2,
        p_batch_from        IN    NUMBER,
        p_batch_to          IN    NUMBER
    ) IS
    
    BEGIN
    
        p_errbuf_out := 'Concurrent Program Completed Succesfully';
        p_retcode_out := 0;
        fnd_file.put_line(fnd_file.output, '------------Printing Parameters------------');
        fnd_file.put_line(fnd_file.output, 'p_level --> ' || p_level);
       
		BEGIN
        
            SELECT meaning
              INTO l_ext_ap_inv_loc
              FROM apps.fnd_lookup_values_vl
             WHERE lookup_type = 'AEE_CONVERSION_DIRECTORIES_LKP'
               AND lookup_code = 'AP-CNV-001-INV-1';
               
             fnd_file.put_line(fnd_file.log, 'l_ext_ap_inv_loc - ' || l_ext_ap_inv_loc);  

        EXCEPTION
            WHEN OTHERS THEN
                l_ext_ap_inv_loc := NULL;
        END;
        
        IF p_level = 'APInvoiceAttachments' THEN
            apinvoice_attachments;
        ELSIF p_level = 'APInvoiceAttachmentsDownload' THEN
            apinvoice_attachments_download(p_batch_from, p_batch_to);
        END IF;
        
        p_retcode_out := gv_retcode_out;
        p_errbuf_out := gv_errbuf_out;
        
    EXCEPTION
        WHEN OTHERS THEN
            p_retcode_out := 2;
            p_errbuf_out := sqlerrm;
    END main;

END aee_fin_ap_inv_attach_pkg;