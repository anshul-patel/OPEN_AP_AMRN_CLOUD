create or replace PACKAGE apps.aee_fin_ap_inv_attach_pkg AS
/*------------------------------------------------------------------------------------
--| Module Name:  aee_fin_ap_inv_attach_pkg
--|
--| Description:  Package to extract the Ap Invoice Attachments for cloud conversion
--| Date:         24-Mar-2021
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

    PROCEDURE main (
        p_errbuf_out        OUT   VARCHAR2,
        p_retcode_out       OUT   NUMBER,
        p_level             IN    VARCHAR2,
        p_batch_from        IN    NUMBER,
        p_batch_to          IN    NUMBER
    );

    PROCEDURE apinvoice_attachments;

    PROCEDURE apinvoice_attachments_download(p_batch_from IN NUMBER, p_batch_to IN NUMBER);

END aee_fin_ap_inv_attach_pkg;