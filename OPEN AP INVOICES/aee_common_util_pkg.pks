SET VERIFY OFF
WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
WHENEVER OSERROR  EXIT FAILURE ROLLBACK;
    
CREATE OR REPLACE PACKAGE apps.aee_common_util_pkg AS
/*------------------------------------------------------------------------------------
--| Name:  aee_common_util_pkg
--|
--| Description:  Common Utility Package for cloud conversion
--| Date:         3-NOV-2021
--|
--| Author:       Adnan Patel
---------------------------------------------------------------|
--|  Modification History
---------------------------------------------------------------|
--|  Date        Who          Description
--| ----------- ------------ -------------------------------
--| 3-NOV-2021  Adnan Patel  Initial Creation
------------------------------------------------------------------------------------*/
    PRAGMA SERIALLY_REUSABLE; 
     -----------------------------------------------------------------------------------
      -- Procedure Name : writeline
      --
      -- Description    : Write to output file
      -----------------------------------------------------------------------------------

    PROCEDURE writeline (
        f_handle        IN    utl_file.file_type,
        p_file_name     IN    VARCHAR2,
        g_process_sts   OUT   VARCHAR2,
        gv_error_msg    OUT   VARCHAR2
    );
      -----------------------------------------------------------------------------------
      -- Procedure Name : openfile
      --
      -- Description    : Open file for write
      -----------------------------------------------------------------------------------

    PROCEDURE openfile (
        file_in         IN    VARCHAR2,
        p_extract_dir   IN    VARCHAR2,
        g_process_sts   OUT   VARCHAR2,
        gv_error_msg    OUT   VARCHAR2,
        f_handle        OUT   utl_file.file_type
    );						   
      -----------------------------------------------------------------------------------
      -- Procedure Name : Check directory
      --
      -- Description    : Check network directory for writing File
      -----------------------------------------------------------------------------------   

    PROCEDURE validate_ext_dir (
        p_extract_dir   IN    VARCHAR2,
        g_process_sts   OUT   VARCHAR2,
        gv_error_msg    OUT   VARCHAR2
    );

END aee_common_util_pkg;
/
SHOW ERRORS;
EXIT;