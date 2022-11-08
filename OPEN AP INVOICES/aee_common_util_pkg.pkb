SET VERIFY OFF
WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
WHENEVER OSERROR  EXIT FAILURE ROLLBACK;
    
CREATE OR REPLACE PACKAGE BODY apps.aee_common_util_pkg AS
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
   f_handle                  UTL_FILE.file_type;
   g_process_sts             VARCHAR2 (1) := 'Y';
   gv_error_msg              VARCHAR2(2000);
   l_extract_dir             VARCHAR2 ( 200 );

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
    ) IS
    BEGIN
        utl_file.put_line(f_handle, p_file_name);
        fnd_file.put_line(fnd_file.output, p_file_name);
    EXCEPTION
        WHEN utl_file.invalid_filehandle THEN
            fnd_file.put_line(fnd_file.log, 'UTL FILE Error:- writeline: File Handle invalid - ' || sqlerrm);
            gv_error_msg := 'UTL FILE Error:- writeline: File Handle invalid - ' || sqlerrm;
            g_process_sts := 'N';
        WHEN utl_file.invalid_operation THEN
            fnd_file.put_line(fnd_file.log, 'UTL FILE Error:- writeline: File not open for write/append - ' || sqlerrm);
            gv_error_msg := 'UTL FILE Error:- writeline: File not open for write/append - ' || sqlerrm;
            g_process_sts := 'N';
        WHEN utl_file.write_error THEN
            fnd_file.put_line(fnd_file.log, 'UTL FILE Error:- writeline: OS Error while writing to file - ' || sqlerrm);
            gv_error_msg := 'UTL FILE Error:- writeline: File not open for write/append - ' || sqlerrm;
            g_process_sts := 'N';
    END writeline;

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
    ) IS
    BEGIN
        fnd_file.put_line(fnd_file.log, 'Inside openfile, P_EXTRACT_DIR:-'
                                        || p_extract_dir
                                        || ' ~ '
                                        || 'FILE_IN:-'
                                        || file_in);

        f_handle := utl_file.fopen(p_extract_dir, file_in, 'W', 32000);
        fnd_file.put_line(fnd_file.log, 'UTL File Opened ');
    EXCEPTION
        WHEN utl_file.invalid_path THEN
            fnd_file.put_line(fnd_file.log, 'UTL FILE Error:- openfile: invalid path - ' || sqlerrm);
            gv_error_msg := 'UTL FILE Error :- openfile: invalid path - ' || sqlerrm;
            g_process_sts := 'N';
        WHEN utl_file.invalid_mode THEN
            fnd_file.put_line(fnd_file.log, 'UTL FILE Error:- openfile: invalid mode - ' || sqlerrm);
            gv_error_msg := 'UTL FILE Error:- openfile: invalid mode - ' || sqlerrm;
            g_process_sts := 'N';
        WHEN utl_file.invalid_filehandle THEN
            fnd_file.put_line(fnd_file.log, 'UTL FILE Error:- openfile: invalid filehandle - ' || sqlerrm);
            gv_error_msg := 'UTL FILE Error:- openfile: invalid filehandle - ' || sqlerrm;
            g_process_sts := 'N';
        WHEN utl_file.invalid_operation THEN
            fnd_file.put_line(fnd_file.log, 'UTL FILE Error:- openfile: invalid operation - ' || sqlerrm);
            gv_error_msg := 'UTL FILE Error:- openfile: invalid operation - ' || sqlerrm;
            g_process_sts := 'N';
    END openfile;

    PROCEDURE validate_ext_dir (
        p_extract_dir   IN    VARCHAR2,
        g_process_sts   OUT   VARCHAR2,
        gv_error_msg    OUT   VARCHAR2
    ) IS
    BEGIN
        fnd_file.put_line(fnd_file.log, 'Validating Directory');
        SELECT
            directory_path
        INTO l_extract_dir
        FROM
            dba_directories
        WHERE
            directory_name = p_extract_dir;

        fnd_file.put_line(fnd_file.log, 'Extract Directory => '
                                        || p_extract_dir
                                        || '('
                                        || l_extract_dir
                                        || ')');

    EXCEPTION
        WHEN OTHERS THEN
            fnd_file.put_line(fnd_file.log, 'Error while fetching Extract Directory Path for directory name:' || sqlerrm);
            gv_error_msg := 'Error while fetching Extract Directory Path for directory name: '
                            || p_extract_dir
                            || ' - '
                            || sqlerrm;
            g_process_sts := 'N';
    END validate_ext_dir;

END aee_common_util_pkg;
/
SHOW ERRORS;
EXIT;